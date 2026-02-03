import json
import discord
from discord.ext import commands
from pathlib import Path
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from discord import ui
import asyncio
import re
import secrets
import random
import logging
import shutil

# Discord logging handler: buffers logs until bot is ready, then posts to configured log channel
class DiscordLogHandler(logging.Handler):
    def __init__(self, level=logging.INFO):
        super().__init__(level)
        self.buffer = []
        self.bot = None
        self.channel_id = None
        self.ready = False

    def set_target(self, bot_instance, channel_id):
        try:
            self.bot = bot_instance
            self.channel_id = int(channel_id) if channel_id else None
            if not self.channel_id:
                return
            self.ready = True
            loop = None
            try:
                loop = self.bot.loop
            except Exception:
                loop = None
            if loop and loop.is_running():
                for msg in list(self.buffer):
                    asyncio.run_coroutine_threadsafe(self._send(msg), loop)
                self.buffer.clear()
        except Exception:
            pass

    def emit(self, record):
        try:
            msg = self.format(record)
            if self.ready and self.bot:
                try:
                    loop = self.bot.loop
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(self._send(msg), loop)
                        return
                except Exception:
                    pass
            # buffer until ready
            self.buffer.append(msg)
        except Exception:
            pass

    async def _send(self, msg: str):
        if not self.bot or not self.channel_id:
            return
        try:
            ch = self.bot.get_channel(self.channel_id) or await self.bot.fetch_channel(self.channel_id)
            if not ch:
                return
            max_len = 1900
            for i in range(0, len(msg), max_len):
                part = msg[i:i+max_len]
                try:
                    await ch.send(f"```\n{part}\n```")
                except Exception:
                    # ignore send failures
                    pass
        except Exception:
            pass


# create and attach handler early so it captures startup logs
DISCORD_LOG_HANDLER = DiscordLogHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
DISCORD_LOG_HANDLER.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(DISCORD_LOG_HANDLER)
try:
    import pylxd
    LXD_PYLXD_AVAILABLE = True
except Exception:
    pylxd = None
    LXD_PYLXD_AVAILABLE = False

# detect docker CLI availability
DOCKER_AVAILABLE = shutil.which("docker") is not None

# Docker will be invoked via CLI (`docker`); no SDK required here.
# LXD client (initialized on demand)
LXD_CLIENT = None


CONFIG_PATH = Path("config.json")
DATABASE_DIR = Path("database")
VPS_PATH = DATABASE_DIR / "vps_data.json"
USER_DB_PATH = DATABASE_DIR / "user_database.json"
GIVEAWAYS_PATH = DATABASE_DIR / "giveaways.json"
PURGE_PROTECTED_PATH = DATABASE_DIR / "purge_protected.json"

# per-user message award cooldowns (seconds)
MESSAGE_COOLDOWNS = {}

# predefined free plans for deployment and selection
FREE_PLANS = [
    {"id": "2x-boost", "name": "2x Boost Reward", "cpu": 2, "ram": 16, "storage": 50},
    {"id": "4x-boost", "name": "4x Boost Reward", "cpu": 6, "ram": 48, "storage": 150},
    {"id": "6x-boost", "name": "6x Boost Reward", "cpu": 8, "ram": 64, "storage": 200},
    {"id": "6-invite", "name": "6 Invite Reward", "cpu": 1, "ram": 6, "storage": 40},
    {"id": "10-invite", "name": "10 Invite Reward", "cpu": 2, "ram": 12, "storage": 80},
    {"id": "15-invite", "name": "15 Invite Reward", "cpu": 3, "ram": 18, "storage": 120},
    {"id": "20-invite", "name": "20 Invite Reward", "cpu": 4, "ram": 25, "storage": 150},
    {"id": "30-invite", "name": "30 Invite Reward", "cpu": 6, "ram": 50, "storage": 250},
    {"id": "40-invite", "name": "40 Invite Reward", "cpu": 8, "ram": 64, "storage": 300},
    {"id": "50-invite", "name": "50 Invite Reward", "cpu": 12, "ram": 96, "storage": 500},
    {"id": "60-invite", "name": "60 Invite Reward", "cpu": 12, "ram": 96, "storage": 400},
    {"id": "starter-trial", "name": "Starter Trial", "cpu": 1, "ram": 2, "storage": 20},
    {"id": "100-invite", "name": "100 Invite Reward", "cpu": 24, "ram": 192, "storage": 1000},
]


def ensure_files():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps({
            "token": "YOUR_BOT_TOKEN",
            "owner_id": "YOUR_OWNER_ID",
            "version": "0.1",
            "thumbnail": "https://example.com/thumbnail.png",
            "log_channel_id": None,
            "welcome_channel_id": None,
            "member_role_id": None,
            "vps_user_role_id": None,
            "embed_colors": {
                "primary": "#95A5A6",
                "orange": "#E67E22",
                "green": "#2ecc71",
                "purple": "#9b59b6"
            },
            "paid_plans": {
                "UL-1": {"cpu": 4, "ram": 50, "disk": 150, "monthly": 1.99, "lifetime": 5},
                "UL-2": {"cpu": 6, "ram": 100, "disk": 250, "monthly": 4.99, "lifetime": 15},
                "UL-3": {"cpu": 10, "ram": 250, "disk": 500, "monthly": 9.99, "lifetime": 20}
            },
            "trial_credits": 10
        }, indent=2))
    DATABASE_DIR.mkdir(exist_ok=True)
    if not VPS_PATH.exists():
        VPS_PATH.write_text(json.dumps({
            "vps": {},
            "purge": {"active": False, "protected_vps": [], "protected_users": []},
            "maintenance": False
        }, indent=2))
    if not USER_DB_PATH.exists():
        USER_DB_PATH.write_text(json.dumps({"users": {}}, indent=2))
    if not GIVEAWAYS_PATH.exists():
        GIVEAWAYS_PATH.write_text(json.dumps({"giveaways": {}}, indent=2))
    if not PURGE_PROTECTED_PATH.exists():
        PURGE_PROTECTED_PATH.write_text(json.dumps({"protected": []}, indent=2))
    

def load_config(path=CONFIG_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_vps_data(path=VPS_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"vps": {}, "purge": {"active": False, "protected_vps": [], "protected_users": []}, "maintenance": False}


def load_user_db(path=USER_DB_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": {}}


def load_giveaways(path=GIVEAWAYS_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"giveaways": {}}


def load_purge_protected(path=PURGE_PROTECTED_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"protected": []}


def save_purge_protected(data, path=PURGE_PROTECTED_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def save_giveaways(path=GIVEAWAYS_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(GIVEAWAYS, f, indent=2)
    except Exception:
        pass


def save_vps_data(path=VPS_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(VPS_DATA, f, indent=2)
    except Exception:
        pass


def save_user_db(path=USER_DB_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(USER_DB, f, indent=2)
    except Exception:
        pass


def save_config(path=CONFIG_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except Exception:
        pass
    


def parse_duration(s: str) -> int:
    """Parse a short duration string like '5m', '2h', '1d' into seconds."""
    m = re.match(r"^(\d+)(s|m|h|d|w|mo)$", s)
    if not m:
        return 0
    val = int(m.group(1))
    unit = m.group(2)
    if unit == "s":
        return val
    if unit == "m":
        return val * 60
    if unit == "h":
        return val * 3600
    if unit == "d":
        return val * 86400
    if unit == "w":
        return val * 7 * 86400
    if unit == "mo":
        return val * 30 * 86400
    return 0


async def finalize_giveaway(message: discord.Message, channel: discord.TextChannel, end_ts: int):
    # wait until end
    now = int(datetime.now(timezone.utc).timestamp())
    wait = max(0, end_ts - now)
    await asyncio.sleep(wait)
    # fetch message to ensure up-to-date reactions
    try:
        msg = await channel.fetch_message(message.id)
    except Exception:
        return
    # count users who reacted with 🎉
    count = 0
    for react in msg.reactions:
        if str(react.emoji) == "🎉":
            users = await react.users().flatten()
            count = len([u for u in users if not u.bot])
            break
    # edit embed to show final participants
    if msg.embeds:
        e = msg.embeds[0]
        # create new embed preserving other fields
        new = discord.Embed(title=e.title, description=e.description, color=e.color)
        for f in e.fields:
            if f.name == "Participants":
                new.add_field(name=f.name, value=str(count), inline=f.inline)
            else:
                new.add_field(name=f.name, value=f.value, inline=f.inline)
        new.set_footer(text=e.footer.text if e.footer else "")
        try:
            await msg.edit(embed=new)
        except Exception:
            pass
    await log_action("Giveaway Ended", f"Giveaway ended in {channel} with {count} participants")


async def finalize_giveaway_by_id(gid: str):
    gw = GIVEAWAYS.get("giveaways", {}).get(gid)
    if not gw:
        return
    channel_id = gw.get("channel_id")
    message_id = int(gw.get("message_id"))
    amount = int(gw.get("amount", 0))
    try:
        ch = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        msg = await ch.fetch_message(message_id)
    except Exception:
        # could not fetch; remove giveaway
        GIVEAWAYS.get("giveaways", {}).pop(gid, None)
        save_giveaways()
        return

    # count reactors for 🎉
    participants = []
    for react in msg.reactions:
        if str(react.emoji) == "🎉":
            users = await react.users().flatten()
            participants = [u for u in users if not u.bot]
            break

    count = len(participants)

    winner = None
    if count > 0:
        winner = random.choice(participants)
        # award credits
        uid = str(winner.id)
        ensure_user_record(uid)
        USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + amount
        save_user_db()

    # edit embed to show final participants and winner
    if msg.embeds:
        e = msg.embeds[0]
        new = discord.Embed(title=e.title, description=e.description, color=e.color)
        for f in e.fields:
            if f.name == "Participants":
                new.add_field(name=f.name, value=str(count), inline=f.inline)
            else:
                new.add_field(name=f.name, value=f.value, inline=f.inline)
        if winner:
            new.add_field(name="Winner", value=winner.mention, inline=False)
        new.set_footer(text=e.footer.text if e.footer else "")
        try:
            await msg.edit(embed=new)
        except Exception:
            pass

    await log_action("Giveaway Finalized", f"Giveaway {gid} finalized; winner: {winner}")
    # remove giveaway
    GIVEAWAYS.get("giveaways", {}).pop(gid, None)
    save_giveaways()


async def finalize_trial_vps(vid: str):
    # remove or expire a trial VPS when time elapses
    v = VPS_DATA.get("vps", {}).get(vid)
    if not v:
        return
    owner = v.get("owner")
    # remove VPS record
    VPS_DATA.get("vps", {}).pop(vid, None)
    save_vps_data()
    # mark owner as having had a trial (trial_claimed should already be True)
    try:
        if owner:
            ensure_user_record(str(owner))
            USER_DB["users"][str(owner)]["trial_active"] = False
            save_user_db()
            # update roles for user
            try:
                asyncio.create_task(update_vps_role_for_user(str(owner)))
            except Exception:
                pass
    except Exception:
        pass
    # notify owner via DM if possible
    try:
        if owner:
            u = await bot.fetch_user(int(owner))
            try:
                await u.send(f"Your 3-day trial VPS #{vid} has expired and was removed.")
            except Exception:
                pass
    except Exception:
        pass
    await log_action("Trial Expired", f"Trial VPS #{vid} expired and removed")


async def schedule_trial_expiry(vid: str, end_ts: int):
    now = int(datetime.now(timezone.utc).timestamp())
    wait = max(0, end_ts - now)
    await asyncio.sleep(wait)
    await finalize_trial_vps(vid)


def vps_state_emoji(status: str) -> str:
    if status == "active":
        return "🟢"
    if status == "suspended":
        return "🟡"
    if status == "stopped":
        return "🔴"
    return "⚪"


async def send_vps_management(ctx_or_interaction, v: dict, requester: discord.User):
    # ctx_or_interaction can be a Context or Interaction
    vid = v.get("id")
    container_name = f"vps-{vid}"
    status = v.get("status", "stopped")
    state_emoji = vps_state_emoji(status)
    created = v.get("created_at")
    purge = VPS_DATA.get("purge", {})
    protected_vps = purge.get("protected_vps", [])
    protected_users = purge.get("protected_users", [])
    purge_protected = (str(vid) in protected_vps) or (v.get("owner") in protected_users)

    embed = discord.Embed(title=f"VPS Management - {container_name}", color=get_embed_color("green"))
    embed.add_field(name="Container", value=container_name, inline=False)
    embed.add_field(name="Status", value=(f"• State: {state_emoji} { 'Online' if status=='active' else status.title()}\n"
                                            f"• Created: {created}\n"
                                            f"• Purge Protected: {'✅ Yes' if purge_protected else '❌ No'}"), inline=False)
    embed.add_field(name="Resources", value=(f"• ⚡ CPU: {v.get('cpu')}\n"
                                              f"• 💾 RAM: {v.get('ram')}GB\n"
                                              f"• 📦 Storage: {v.get('storage')}GB"), inline=False)
    embed.add_field(name="Controls", value="Use the buttons below to manage this server", inline=False)

    thumb = config.get("thumbnail")
    if thumb:
        embed.set_image(url=thumb)
    version = config.get("version", "1.0.0")
    try:
        if ZoneInfo:
            now = datetime.now(ZoneInfo("America/New_York"))
        else:
            now = datetime.now(timezone.utc)
        ts = now.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        ts = datetime.now(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC")
    embed.set_footer(text=f"Vortex Nodes v{version} | Today At {ts}")

    # buttons: start, stop, ssh, reinstall
    class ManageButtons(ui.View):
        def __init__(self, vps_id: str, owner_id: str):
            super().__init__(timeout=None)
            self.vps_id = vps_id
            self.owner_id = owner_id

        def allowed(self, user: discord.User):
            vrec = VPS_DATA.get("vps", {}).get(self.vps_id)
            if not vrec:
                return False
            if str(user.id) == str(vrec.get("owner")):
                return True
            if str(user.id) in vrec.get("shared_with", []):
                return True
            if user.id == OWNER_ID:
                return True
            return False

        @ui.button(label="Start", emoji="▶️", style=discord.ButtonStyle.success)
        async def start(self, interaction: discord.Interaction, button: ui.Button):
            if not self.allowed(interaction.user):
                return await interaction.response.send_message("You cannot use these controls.", ephemeral=True)
            if VPS_DATA.get("maintenance"):
                return await interaction.response.send_message("Cannot start VPS while maintenance is enabled.", ephemeral=True)
            vrec = VPS_DATA.get("vps", {}).get(self.vps_id)
            vrec["status"] = "active"
            save_vps_data()
            await interaction.response.send_message(f"VPS #{self.vps_id} started.", ephemeral=True)
            await log_action("VPS Started", f"VPS #{self.vps_id} started by {interaction.user}")

        @ui.button(label="Stop", emoji="⏸️", style=discord.ButtonStyle.secondary)
        async def stop(self, interaction: discord.Interaction, button: ui.Button):
            if not self.allowed(interaction.user):
                return await interaction.response.send_message("You cannot use these controls.", ephemeral=True)
            vrec = VPS_DATA.get("vps", {}).get(self.vps_id)
            vrec["status"] = "stopped"
            save_vps_data()
            await interaction.response.send_message(f"VPS #{self.vps_id} stopped.", ephemeral=True)
            await log_action("VPS Stopped", f"VPS #{self.vps_id} stopped by {interaction.user}")

        @ui.button(label="SSH Access", emoji="🔑", style=discord.ButtonStyle.primary)
        async def ssh(self, interaction: discord.Interaction, button: ui.Button):
            if not self.allowed(interaction.user):
                return await interaction.response.send_message("You cannot use these controls.", ephemeral=True)
            vrec = VPS_DATA.get("vps", {}).get(self.vps_id)
            # check/install simulated
            if not vrec.get("ssh_installed"):
                await interaction.response.send_message("Please wait — installing OpenSSH and tmate on your VPS...", ephemeral=True)
                # simulate install
                await asyncio.sleep(3)
                vrec["ssh_installed"] = True
                save_vps_data()
            # create fake session name (expose only the session identifier, not a real tmate URL)
            session = secrets.token_hex(6)
            user = interaction.user
            try:
                dm = await user.create_dm()
                emb = discord.Embed(title="✅ SSH Access Credentials", color=get_embed_color("green"))
                emb.add_field(name="Server", value=container_name, inline=False)
                emb.add_field(name="Session Name", value=session, inline=False)
                emb.add_field(name="Instructions", value="• Use the session name to request access from staff or automated systems\n• Session expires in 24 hours\n• Do not share this value with others", inline=False)
                emb.set_footer(text=f"Vortex Nodes | v{config.get('version','1.0.0')} Today At {ts}")
                await dm.send(embed=emb)
                await log_action("SSH Issued", f"SSH session name for VPS #{self.vps_id} issued to {user}")
                return
            except Exception:
                return await interaction.response.send_message("Unable to DM you the SSH credentials.", ephemeral=True)

        @ui.button(label="Reinstall OS ubuntu 22.04", emoji="🔴", style=discord.ButtonStyle.danger)
        async def reinstall(self, interaction: discord.Interaction, button: ui.Button):
            if not self.allowed(interaction.user):
                return await interaction.response.send_message("You cannot use these controls.", ephemeral=True)
            # confirm reinstall
            await interaction.response.send_message(f"Confirm reinstall of VPS #{self.vps_id}", view=ConfirmView(self.vps_id, interaction.user), ephemeral=True)

    view = ManageButtons(vps_id=vid, owner_id=v.get("owner"))

    # send either via interaction or ctx
    if isinstance(ctx_or_interaction, discord.Interaction):
        await ctx_or_interaction.response.send_message(embed=embed, view=view, ephemeral=False)
    else:
        await ctx_or_interaction.send(embed=embed, view=view)
    await log_action("VPS Management Displayed", f"VPS #{vid} management displayed to {requester}")


def build_freeplans_embed(bullet="-"):
    embed = discord.Embed(
        title="🎁 Free VPS Plans",
        description="Earn free VPS through server boosts and invites",
        color=0x2ecc71,
    )

    b = bullet
    boost_value = (
        f"2x Boost Reward\n"
        f"{b} **Requires:** 2x Server Boosts\n"
        f"{b} ⚡ 2 CPU Cores\n"
        f"{b} 💾 16GB RAM\n"
        f"{b} 📦 50GB Disk\n\n"
        f"**4x Boost Reward**\n"
        f"{b} **Requires:** 4x Server Boosts\n"
        f"{b} ⚡ 6 CPU Cores\n"
        f"{b} 💾 48GB RAM\n"
        f"{b} 📦 150GB Disk\n\n"
        f"**6x Boost Reward**\n"
        f"{b} **Requires:** 6x Server Boosts\n"
        f"{b} ⚡ 8 CPU Cores\n"
        f"{b} 💾 64GB RAM\n"
        f"{b} 📦 200GB Disk"
    )

    invite_value = (
        f"**6 Invite Reward**\n"
        f"{b} **Requires:** 6 Invites\n"
        f"{b} ⚡ 1 CPU Core\n"
        f"{b} 💾 6GB RAM\n"
        f"{b} 📦 40GB Disk\n\n"
        f"**10 Invite Reward**\n"
        f"{b} **Requires:** 10 Invites\n"
        f"{b} ⚡ 2 CPU Cores\n"
        f"{b} 💾 12GB RAM\n"
        f"{b} 📦 80GB Disk\n\n"
        f"**15 Invite Reward**\n"
        f"{b} **Requires:** 15 Invites\n"
        f"{b} ⚡ 3 CPU Cores\n"
        f"{b} 💾 18GB RAM\n"
        f"{b} 📦 120GB Disk\n\n"
        f"**20 Invite Reward**\n"
        f"{b} **Requires:** 20 Invites\n"
        f"{b} ⚡ 4 CPU Cores\n"
        f"{b} 💾 25GB RAM\n"
        f"{b} 📦 150GB Disk\n\n"
        f"**30 Invite Reward**\n"
        f"{b} **Requires:** 30 Invites\n"
        f"{b} ⚡ 6 CPU Cores\n"
        f"{b} 💾 50GB RAM\n"
        f"{b} 📦 250GB Disk"
    )

    embed.add_field(name="🚀 Boost Rewards", value=boost_value, inline=False)
    embed.add_field(name="👥 Invite Rewards", value=invite_value, inline=False)
    return embed


def build_paid_plans_embed():
    embed = discord.Embed(
        title="💰 Paid Plans",
        description="Purchase with credits\nUse .buywc <plan>",
        color=0xE67E22,
    )

    paid = (
        "UL-1 - ⏣ 170/month\n"
        "• ⚡ CPU: 2 Cores\n"
        "• 💾 RAM: 8GB\n"
        "• 📦 Storage: 50GB\n"
        "• non ipv4 only nat Amd ryzen 9 7900\n\n"
        "UL-2 - ⏣ 320/month\n"
        "• ⚡ CPU: 4 Cores\n"
        "• 💾 RAM: 16GB\n"
        "• 📦 Storage: 100GB\n"
        "• non ipv4 only nat Amd ryzen 9 7900\n\n"
        "UL-3 - ⏣ 600/month\n"
        "• ⚡ CPU: 6 Cores\n"
        "• 💾 RAM: 32GB\n"
        "• 📦 Storage: 200GB\n"
        "• non ipv4 only nat Amd ryzen 9 7900\n\n"
        "XL-1 - ⏣ 900/month\n"
        "• ⚡ CPU: 8 Cores\n"
        "• 💾 RAM: 48GB\n"
        "• 📦 Storage: 350GB\n"
        "• non ipv4 only nat Amd ryzen 9 7900\n\n"
        "XL-2 - ⏣ 1200/month\n"
        "• ⚡ CPU: 12 Cores\n"
        "• 💾 RAM: 64GB\n"
        "• 📦 Storage: 500GB\n"
        "• non ipv4 only nat Amd ryzen 9 7900\n\n"
        "How to Purchase\nUse .buywc <plan_name> to purchase\nExample: .buywc basic"
    )

    embed.add_field(name="Available Plans", value=paid, inline=False)
    return embed


ensure_files()
config = load_config()
VPS_DATA = load_vps_data()
USER_DB = load_user_db()
GIVEAWAYS = load_giveaways()
TOKEN = config.get("token")
OWNER_ID = int(config.get("owner_id"))

PROMOS_PATH = DATABASE_DIR / "promos.json"

def ensure_promos():
    if not PROMOS_PATH.exists():
        PROMOS_PATH.write_text(json.dumps({"promos": {}}, indent=2))


def load_promos(path=PROMOS_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_promos(data, path=PROMOS_PATH):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


ensure_promos()
PROMOS = load_promos()


def _hex_to_int(hexstr: str, default: int = 0x95A5A6) -> int:
    try:
        if not hexstr:
            return default
        hs = str(hexstr).strip()
        if hs.startswith("#"):
            hs = hs[1:]
        return int(hs, 16)
    except Exception:
        return default


def get_embed_color(key: str = "primary") -> int:
    """Return an integer color for Discord embeds from `config['embed_colors']`."""
    try:
        cols = config.get("embed_colors", {}) if isinstance(config, dict) else {}
        val = cols.get(key) or cols.get("primary") or "#95A5A6"
        return _hex_to_int(val)
    except Exception:
        return 0x95A5A6


async def log_action(title: str, body: str):
    """Log an action both to the python logger and to the configured Discord log channel (if set)."""
    try:
        msg = f"{title}: {body}"
        logging.info(msg)
        cid = config.get("log_channel_id") if isinstance(config, dict) else None
        if not cid:
            return
        try:
            ch = bot.get_channel(int(cid)) or await bot.fetch_channel(int(cid))
            if not ch:
                return
            emb = discord.Embed(title=title, description=body, color=get_embed_color("purple"))
            emb.set_footer(text=f"At {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            await ch.send(embed=emb)
        except Exception:
            # fall back to buffered handler (it formats the message)
            logging.exception("Failed sending log embed to discord channel")
    except Exception:
        try:
            logging.exception("log_action failed")
        except Exception:
            pass

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=".", intents=intents)
# remove default help so a custom help command can be registered
try:
    bot.remove_command('help')
except Exception:
    pass


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    # attach discord log handler target so buffered startup logs are sent
    try:
        log_cid = config.get("log_channel_id") if isinstance(config, dict) else None
        if log_cid:
            DISCORD_LOG_HANDLER.set_target(bot, log_cid)
            logging.info(f"Discord log handler attached to channel {log_cid}")
    except Exception:
        pass
    # set presence to Do Not Disturb with versioned activity
    try:
        version = config.get("version", "1.0.0")
        activity = discord.Activity(type=discord.ActivityType.playing, name=f"Vortex Nodes v{version}")
        await bot.change_presence(status=discord.Status.dnd, activity=activity)
    except Exception:
        pass

    # schedule pending giveaways
    now = int(datetime.now(timezone.utc).timestamp())
    for gid, gw in list(GIVEAWAYS.get("giveaways", {}).items()):
        # ensure finalize tasks are scheduled for any pending or overdue giveaways
        asyncio.create_task(finalize_giveaway_by_id(gid))

    # schedule pending trial expiries
    now = int(datetime.now(timezone.utc).timestamp())
    for vid, v in list(VPS_DATA.get("vps", {}).items()):
        end_ts = v.get("trial_expires")
        if end_ts:
            try:
                end_ts_i = int(end_ts)
                asyncio.create_task(schedule_trial_expiry(vid, end_ts_i))
            except Exception:
                continue


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # handle reactions to the purge announcement message to register protection
    try:
        purge = VPS_DATA.get("purge", {})
        msg_id = purge.get("message_id")
        ch_id = purge.get("channel_id")
        if not msg_id or not ch_id:
            return
        if payload.message_id != msg_id:
            return
        # ignore bot reactions
        if payload.user_id == bot.user.id:
            return
        # record protection
        now = int(datetime.now(timezone.utc).timestamp())
        rec = {
            "user_id": str(payload.user_id),
            "emoji": str(payload.emoji),
            "protected_at": now,
            "expires_at": now + (72 * 3600)
        }
        pp = load_purge_protected()
        lst = pp.setdefault("protected", [])
        # replace existing record for user if present
        lst = [r for r in lst if str(r.get("user_id")) != str(payload.user_id)]
        lst.append(rec)
        pp["protected"] = lst
        save_purge_protected(pp)

        # DM the user to confirm (ephemeral-style)
        try:
            user = await bot.fetch_user(int(payload.user_id))
            emb = discord.Embed(title="✅ Purge Protection Granted", color=get_embed_color("green"))
            emb.add_field(name="Chosen Emoji", value=str(payload.emoji), inline=False)
            emb.add_field(name="Expires In", value="72 hours", inline=False)
            emb.set_footer(text="You reacted to the purge announcement; your VPS will be protected until expiry.")
            try:
                await user.send(embed=emb)
            except Exception:
                pass
        except Exception:
            pass
    except Exception:
        pass


def load_config(path=CONFIG_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_vps_data(path=VPS_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"vps": {}, "purge": {"active": False, "protected_vps": [], "protected_users": []}, "maintenance": False}


def load_user_db(path=USER_DB_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": {}}


def load_giveaways(path=GIVEAWAYS_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"giveaways": {}}


def save_giveaways(path=GIVEAWAYS_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(GIVEAWAYS, f, indent=2)
    except Exception:
        pass


def save_vps_data(path=VPS_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(VPS_DATA, f, indent=2)
    except Exception:
        pass


def save_user_db(path=USER_DB_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(USER_DB, f, indent=2)
    except Exception:
        pass


def save_config(path=CONFIG_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except Exception:
        pass


@bot.event
async def on_member_join(member: discord.Member):
    # assign member role if configured
    try:
        mrid = config.get("member_role_id")
        if mrid:
            role = member.guild.get_role(int(mrid))
            if role:
                try:
                    await member.add_roles(role, reason="Auto member role on join")
                except Exception:
                    pass
    except Exception:
        pass

    # welcome channel message and DM
    try:
        wcid = config.get("welcome_channel_id")
        version = config.get("version", "1.0.0")
        if wcid:
            ch = bot.get_channel(int(wcid)) or member.guild.get_channel(int(wcid))
            if ch:
                emb = discord.Embed(title=f"Welcome {member.display_name} to Vortex Nodes!", color=get_embed_color("primary"))
                emb.add_field(name="Get Started", value=("• Claim free VPS via boosts/invites\n"
                                                         "• Use .plans to view available plans\n"
                                                         "• Use .buywc <plan> to purchase with credits"), inline=False)
                emb.add_field(name="Paid Plans", value="Use .paidplans to see pricing or ask staff", inline=False)
                emb.set_footer(text=f"Vortex Nodes v{version}")
                try:
                    await ch.send(embed=emb)
                except Exception:
                    pass
    except Exception:
        pass


@bot.event
async def on_message(message: discord.Message):
    # ignore bots
    try:
        if message.author.bot:
            return
    except Exception:
        return

    # Message-based credits have been disabled. Credits are awarded only via giveaways.
    # (Previous logic that awarded 5 credits per message was intentionally removed.)

    # If a user mentions the owner, timeout them for 5 minutes and warn
    try:
        if message.mentions:
            for u in message.mentions:
                if u.id == OWNER_ID:
                    # warn and timeout
                    try:
                        if message.guild and hasattr(message.author, "timeout"):
                            until = datetime.now(timezone.utc) + timedelta(minutes=5)
                            try:
                                await message.author.timeout(until, reason="Pinged owner")
                            except Exception:
                                pass
                        await message.channel.send(f"{message.author.mention} Pleaase do not ping the owner")
                    except Exception:
                        pass
                    break
    except Exception:
        pass

    # allow commands to be processed
    try:
        await bot.process_commands(message)
    except Exception:
        pass

    # DM the user with welcome message
    try:
        dm = await member.create_dm()
        dm_msg = (
            "Welcome to Vortex Nodes!\n\n"
            "We're a community offering VPS plans.\n"
            "How to get free VPS: earn server boosts or invites to qualify for free plans.\n"
            "Paid plans are available; use .plans or ask staff for details.\n\n"
            "If you need help, open a ticket or contact staff. Enjoy!"
        )
        try:
            await dm.send(dm_msg)
        except Exception:
            pass
    except Exception:
        pass

    # assign vps role if user already has a VPS
    try:
        uid = str(member.id)
        has_vps = any(info.get("owner") == uid for info in VPS_DATA.get("vps", {}).values())
        vrole = config.get("vps_user_role_id")
        if vrole and has_vps:
            for guild in bot.guilds:
                try:
                    if guild.id != member.guild.id:
                        continue
                    role = guild.get_role(int(vrole))
                    if role and role not in member.roles:
                        try:
                            await member.add_roles(role, reason="Has VPS")
                        except Exception:
                            pass
                except Exception:
                    pass
    except Exception:
        pass


@bot.command(name="freeplans")
async def freeplans(ctx, channel: discord.TextChannel):
    if ctx.author.id != OWNER_ID:
        await ctx.send("Only the owner can use this command.")
        return
    embed = build_freeplans_embed()
    await channel.send(embed=embed)
    await ctx.send(f"Embed posted to {channel.mention}")
    await log_action("Free Plans Posted", f"{ctx.author} posted freeplans to {channel}")


@bot.command(name="trial")
async def trial(ctx, member: discord.Member = None):
    """Claim a 3-day trial VPS. Owner can grant to others by mentioning them; users can self-claim once."""
    # owner granting to another member
    if ctx.author.id == OWNER_ID and member:
        class ConfirmTrialView(ui.View):
            def __init__(self, target: discord.Member, requester: discord.User):
                super().__init__(timeout=60)
                self.target = target
                self.requester = requester

            @ui.button(label="Confirm Grant", style=discord.ButtonStyle.success)
            async def confirm(self, interaction: discord.Interaction, button: ui.Button):
                if interaction.user.id != self.requester.id and interaction.user.id != OWNER_ID:
                    return await interaction.response.send_message("Not authorized.", ephemeral=True)
                uid = str(self.target.id)
                ensure_user_record(uid)
                if USER_DB["users"][uid].get("trial_claimed"):
                    return await interaction.response.edit_message(content=f"{self.target.mention} already claimed a trial.", view=None)
                vid = create_vps_record(uid, 2, 1, 20)
                now = int(datetime.now(timezone.utc).timestamp())
                end = now + 3 * 24 * 3600
                VPS_DATA.setdefault("vps", {})[vid]["trial_expires"] = end
                VPS_DATA.setdefault("vps", {})[vid]["status"] = "trial"
                USER_DB["users"][uid]["trial_claimed"] = True
                USER_DB["users"][uid]["trial_active"] = True
                tc = int(config.get("trial_credits", 0))
                if tc:
                    USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + tc
                save_vps_data()
                save_user_db()
                try:
                    asyncio.create_task(schedule_trial_expiry(vid, end))
                except Exception:
                    pass
                try:
                    asyncio.create_task(update_vps_role_for_user(uid))
                except Exception:
                    pass
                await interaction.response.edit_message(content=f"Granted trial VPS #{vid} to {self.target.mention} (3 days).", view=None)
                await log_action("Trial Granted", f"Trial VPS #{vid} granted to {self.target} by {interaction.user}")

            @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, interaction: discord.Interaction, button: ui.Button):
                await interaction.response.edit_message(content="Cancelled.", view=None)

        await ctx.send(f"Grant trial to {member.mention}?", view=ConfirmTrialView(member, ctx.author))
        return

    # self-claim flow
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    if USER_DB["users"][uid].get("trial_claimed"):
        return await ctx.send("You have already claimed a trial before.")

    class ConfirmSelfClaim(ui.View):
        def __init__(self, requester: discord.User):
            super().__init__(timeout=60)
            self.requester = requester

        @ui.button(label="Claim Trial", style=discord.ButtonStyle.success)
        async def claim(self, interaction: discord.Interaction, button: ui.Button):
            if interaction.user.id != self.requester.id:
                return await interaction.response.send_message("Not authorized.", ephemeral=True)
            uid2 = str(interaction.user.id)
            ensure_user_record(uid2)
            USER_DB["users"][uid2]["trial_claimed"] = True
            USER_DB["users"][uid2]["trial_active"] = True
            vid = create_vps_record(uid2, 2, 1, 20)
            now = int(datetime.now(timezone.utc).timestamp())
            end = now + 3 * 24 * 3600
            VPS_DATA.setdefault("vps", {})[vid]["trial_expires"] = end
            VPS_DATA.setdefault("vps", {})[vid]["status"] = "trial"
            tc = int(config.get("trial_credits", 0))
            if tc:
                USER_DB["users"][uid2]["credits"] = USER_DB["users"][uid2].get("credits", 0) + tc
            save_vps_data()
            save_user_db()
            try:
                asyncio.create_task(schedule_trial_expiry(vid, end))
            except Exception:
                pass
            try:
                asyncio.create_task(update_vps_role_for_user(uid2))
            except Exception:
                pass
            await interaction.response.edit_message(content=f"You have been granted trial VPS #{vid} for 3 days.", view=None)
            await log_action("Trial Claimed", f"{interaction.user} claimed trial VPS #{vid}")

        @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
        async def cancel(self, interaction: discord.Interaction, button: ui.Button):
            await interaction.response.edit_message(content="Cancelled.", view=None)

    await ctx.send("Are you sure you want to claim a 3-day trial VPS? You may only claim once.", view=ConfirmSelfClaim(ctx.author))


class PlansView(ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=None)
        self.author_id = author_id

    @ui.button(label="💰 Paid Plans", style=discord.ButtonStyle.primary)
    async def paid(self, interaction: discord.Interaction, button: ui.Button):
        embed = build_paid_plans_embed()
        await interaction.response.send_message(embed=embed, ephemeral=False)
        await log_action("Plans Button", f"Paid Plans button clicked by {interaction.user}")

    @ui.button(label="🎁 Free Plans", style=discord.ButtonStyle.secondary)
    async def free(self, interaction: discord.Interaction, button: ui.Button):
        embed = build_freeplans_embed(bullet="•")
        await interaction.response.send_message(embed=embed, ephemeral=False)
        await log_action("Plans Button", f"Free Plans button clicked by {interaction.user}")


@bot.command(name="plans")
async def plans(ctx):
    # get user's credits
    user_key = str(ctx.author.id)
    users = USER_DB.get("users", {})
    amount = users.get(user_key, {}).get("credits", 0)

    embed = discord.Embed(
        title="🖥️ VPS Plans",
        description="Choose from our available plans",
        color=0x95A5A6,
    )

    embed.add_field(name="💰 Paid Plans", value="Purchase with credits\nUse .buywc <plan>", inline=False)
    embed.add_field(name="🎁 Free Plans", value="Earn through boosts/invites\nOpen a ticket to claim", inline=False)
    embed.add_field(name="💰 Your Balance", value=f"⏣ {amount}", inline=False)

    # thumbnail
    thumb = config.get("thumbnail")
    if thumb:
        embed.set_thumbnail(url=thumb)

    # footer with eastern timestamp
    version = config.get("version", "1.0.0")
    try:
        if ZoneInfo:
            now = datetime.now(ZoneInfo("America/New_York"))
        else:
            now = datetime.now(timezone.utc)
        ts = now.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        ts = datetime.now(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC")

    embed.set_footer(text=f"Vortex Nodes | v{version} • Today at {ts}")

    view = PlansView(author_id=ctx.author.id)
    await ctx.send(embed=embed, view=view)
    await log_action("Plans Viewed", f"{ctx.author} viewed plans")


@bot.command(name="giveawayc")
async def giveaway_credits(ctx, channel: discord.TextChannel, amount: int, duration: str):
    # owner-only
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can start giveaways.")
    secs = parse_duration(duration)
    if secs <= 0:
        return await ctx.send("Invalid duration. Use formats like 1s 1m 1h 1d 1w 1mo")

    end_ts = int((datetime.now(timezone.utc).timestamp()) + secs)

    embed = discord.Embed(title="💰 Credits Giveaway", color=get_embed_color("orange"))
    # include discord relative timestamp
    embed.description = f"Ends in <t:{end_ts}:R>\nHosted By: {ctx.author.mention}"
    embed.add_field(name="Participants", value="0", inline=False)
    version = config.get("version", "1.0.0")
    embed.set_footer(text=f"Vortex Nodes | v{version}")

    msg = await channel.send(embed=embed)
    try:
        await msg.add_reaction("🎉")
    except Exception:
        pass
    # persist giveaway
    gid = str(msg.id)
    GIVEAWAYS.setdefault("giveaways", {})[gid] = {
        "message_id": msg.id,
        "channel_id": channel.id,
        "end_ts": end_ts,
        "amount": amount,
        "host_id": ctx.author.id,
        "created_at": int(datetime.now(timezone.utc).timestamp())
    }
    save_giveaways()

    # schedule finalize task
    asyncio.create_task(finalize_giveaway_by_id(gid))
    await ctx.send(f"Giveaway started in {channel.mention} for {amount} credits, ends <t:{end_ts}:F>")
    await log_action("Giveaway Started", f"{ctx.author} started a giveaway in {channel} for {amount} credits until <t:{end_ts}:F>")


@bot.command(name="credits")
async def credits(ctx):
    user_key = str(ctx.author.id)
    users = USER_DB.get("users", {})
    amount = users.get(user_key, {}).get("credits", 0)

    embed = discord.Embed(
        title="💰 Credits Balance",
        description=f"**Available:** {amount}",
        color=0xE67E22,
    )

    prices = (
        "**VPS Prices**\n"
        "Basic: 150/Month {1,25,65}\n"
        "Standard: 200/Month {2,50,100}\n"
        "Pro: 300/Month {4,75,150}\n"
        "Premium: 500/Month {6,100,250}"
    )

    embed.add_field(name="", value=prices, inline=False)
    version = config.get("version", "1.0.0")
    embed.set_footer(text=f"Vortex Nodes | {version}")

    await ctx.send(embed=embed)


# Helper utilities
def ensure_user_record(user_id: str):
    users = USER_DB.setdefault("users", {})
    if user_id not in users:
        users[user_id] = {"credits": 0, "trial_claimed": False, "trial_active": False, "is_admin": False}
        save_user_db()


def is_admin_ctx(ctx) -> bool:
    try:
        if ctx.author.id == OWNER_ID:
            return True
        uid = str(ctx.author.id)
        return USER_DB.get("users", {}).get(uid, {}).get("is_admin", False)
    except Exception:
        return False


def is_admin_userid(uid) -> bool:
    try:
        if int(uid) == OWNER_ID:
            return True
        return USER_DB.get("users", {}).get(str(uid), {}).get("is_admin", False)
    except Exception:
        return False


def get_next_vps_id() -> str:
    existing = VPS_DATA.get("vps", {})
    if not existing:
        return "1"
    nums = [int(x) for x in existing.keys() if x.isdigit()]
    return str(max(nums) + 1) if nums else "1"


def create_vps_record(owner_id: str, ram: int, cpu: int, storage: int):
    vid = get_next_vps_id()
    VPS_DATA.setdefault("vps", {})[vid] = {
        "id": vid,
        "owner": owner_id,
        "ram": ram,
        "cpu": cpu,
        "storage": storage,
        "status": "active",
        "shared_with": [],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    save_vps_data()
    # provision via Docker if enabled and available, else try LXD
    try:
        cfg = config or load_config()
        dconf = cfg.get("docker", {})
        if dconf.get("enabled") and DOCKER_AVAILABLE:
            try:
                asyncio.create_task(provision_docker_for_vps(vid))
            except Exception:
                pass
        else:
            lxd_conf = cfg.get("lxd", {})
            if lxd_conf.get("enabled") and LXD_PYLXD_AVAILABLE:
                try:
                    asyncio.create_task(provision_lxd_for_vps(vid))
                except Exception:
                    pass
    except Exception:
        pass
    return vid


def ensure_lxd_client():
    global LXD_CLIENT
    if LXD_CLIENT:
        return LXD_CLIENT
    if not LXD_PYLXD_AVAILABLE:
        return None
    try:
        cfg = load_config()
        lconf = cfg.get("lxd", {})
        # remote can be a LXD REST endpoint (https://host:8443) or 'local:' for local socket
        remote = lconf.get("remote_endpoint") or lconf.get("remote") or "local:"
        client_cert = lconf.get("client_cert")
        client_key = lconf.get("client_key")
        verify = lconf.get("verify", True)

        if not remote or str(remote).lower() in ("local:", "local"):
            LXD_CLIENT = pylxd.Client()
        else:
            # use TLS client certs if provided
            cert = None
            if client_cert and client_key:
                cert = (str(client_cert), str(client_key))
            # pylxd accepts 'verify' bool or path to CA bundle
            LXD_CLIENT = pylxd.Client(endpoint=str(remote), cert=cert, verify=verify)
    except Exception:
        LXD_CLIENT = None
    return LXD_CLIENT


import subprocess


async def provision_lxd_for_vps(vid: str):
    """Background task to provision an LXD container for the given vps id."""
    try:
        client = await asyncio.to_thread(ensure_lxd_client)
        if not client:
            # can't provision; record that provisioning was skipped
            v = VPS_DATA.get("vps", {}).get(vid)
            if v:
                v["provisioned"] = False
                v["lxd_error"] = "pylxd not available or LXD client not reachable"
                save_vps_data()
            return

        v = VPS_DATA.get("vps", {}).get(vid)
        if not v:
            return
        name = f"vortex-{vid}"
        cfg = load_config()
        lconf = cfg.get("lxd", {})
        image = lconf.get("default_image", "ubuntu/22.04")
        profile = lconf.get("default_profile", "default")

        def _create():
            try:
                # try create container from image alias
                config = {
                    "name": name,
                    "source": {"type": "image", "alias": image},
                    "profiles": [profile],
                }
                ct = client.containers.create(config, wait=True)
                ct.start(wait=True)
                return (True, None)
            except Exception as e:
                return (False, str(e))

        ok, err = await asyncio.to_thread(_create)
        if ok:
            v["provisioned"] = True
            v["lxd_name"] = name
            v["lxd_state"] = "running"
            save_vps_data()
            await log_action("LXD Provisioned", f"Provisioned LXD container {name} for VPS #{vid}")
        else:
            v["provisioned"] = False
            v["lxd_error"] = err
            save_vps_data()
            await log_action("LXD Provision Error", f"Failed to provision VPS #{vid}: {err}")
    except Exception as e:
        try:
            v = VPS_DATA.get("vps", {}).get(vid)
            if v:
                v["provisioned"] = False
                v["lxd_error"] = str(e)
                save_vps_data()
        except Exception:
            pass


async def provision_docker_for_vps(vid: str):
    """Background task to provision a Docker container for the given vps id."""
    try:
        v = VPS_DATA.get("vps", {}).get(vid)
        if not v:
            return
        name = f"vortex-{vid}"
        cfg = load_config()
        dconf = cfg.get("docker", {})
        image = dconf.get("image", "ubuntu:22.04")
        network = dconf.get("network")
        cpu_shares = dconf.get("cpu_shares")
        mem_limit = dconf.get("mem_limit")

        cmd = ["docker", "run", "-d", "--name", name]
        if network:
            cmd += ["--network", str(network)]
        if cpu_shares:
            cmd += ["--cpu-shares", str(cpu_shares)]
        if mem_limit:
            cmd += ["--memory", str(mem_limit)]
        # keep container running
        cmd += [image, "tail", "-f", "/dev/null"]


        def _run():
            try:
                logs_parts = []
                # pull image
                pull = subprocess.run(["docker", "pull", image], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    logs_parts.append("== docker pull stdout ==\n" + (pull.stdout.decode() if pull.stdout else "") + "\n" + (pull.stderr.decode() if pull.stderr else ""))
                except Exception:
                    pass

                # run container detached, keep it alive
                p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    logs_parts.append("== docker run stdout ==\n" + (p.stdout.decode() if p.stdout else "") + "\n" + (p.stderr.decode() if p.stderr else ""))
                except Exception:
                    pass

                # run package installation inside container
                # install commonly needed packages for user workflows
                pkgs = "curl neofetch tmate git openssh-server python3 python3-pip docker.io"
                # install sudo first, then run the update/install commands using sudo
                bash_cmd = (
                    f"apt-get update && apt-get install -y sudo || true; "
                    f"sudo apt-get update || true; sudo apt-get install -y {pkgs} || true"
                )
                exec_cmd = ["docker", "exec", name, "bash", "-lc", bash_cmd]
                e = subprocess.run(exec_cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    logs_parts.append("== docker exec (apt) stdout ==\n" + (e.stdout.decode() if e.stdout else "") + "\n" + (e.stderr.decode() if e.stderr else ""))
                except Exception:
                    pass

                logs = "\n\n".join(logs_parts)
                return (True, logs)
            except subprocess.CalledProcessError as e:
                msg = (e.stderr.decode() if getattr(e, 'stderr', None) else str(e))
                return (False, msg)
            except Exception as e:
                return (False, str(e))

        ok, result = await asyncio.to_thread(_run)
        if ok:
            v["provisioned"] = True
            v["docker_name"] = name
            v["docker_state"] = "running"
            v["docker_logs"] = result
            save_vps_data()
            # send logs to configured log channel via log_action
            try:
                await log_action("Docker Provisioned", f"Provisioned Docker container {name} for VPS #{vid}\n\nLogs:\n{result}")
            except Exception:
                pass
        else:
            v["provisioned"] = False
            v["docker_error"] = result
            save_vps_data()
            try:
                await log_action("Docker Provision Error", f"Failed to provision VPS #{vid}: {result}")
            except Exception:
                pass
    except Exception as e:
        try:
            v = VPS_DATA.get("vps", {}).get(vid)
            if v:
                v["provisioned"] = False
                v["docker_error"] = str(e)
                save_vps_data()
        except Exception:
            pass


async def update_vps_role_for_user(user_id: str):
    """Add or remove the configured vps_user_role_id for this user across guilds."""
    try:
        role_id = config.get("vps_user_role_id")
        if not role_id:
            return
        role_id = int(role_id)
    except Exception:
        return
    has_vps = any(info.get("owner") == str(user_id) for info in VPS_DATA.get("vps", {}).values())
    for guild in bot.guilds:
        try:
            role = guild.get_role(role_id)
            if not role:
                continue
            member = guild.get_member(int(user_id))
            if not member:
                continue
            if has_vps:
                if role not in member.roles:
                    try:
                        await member.add_roles(role, reason="Assigned vps_user_role")
                    except Exception:
                        pass
            else:
                if role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Removed vps_user_role")
                    except Exception:
                        pass
        except Exception:
            continue


async def remove_vps_role_from_all_guilds():
    try:
        role_id = config.get("vps_user_role_id")
        if not role_id:
            return
        role_id = int(role_id)
    except Exception:
        return
    for guild in bot.guilds:
        try:
            role = guild.get_role(role_id)
            if not role:
                continue
            members = list(role.members)
            for m in members:
                try:
                    await m.remove_roles(role, reason="VPS records cleared")
                except Exception:
                    pass
        except Exception:
            continue


def list_user_vps(owner_id: str):
    out = []
    for vid, info in VPS_DATA.get("vps", {}).items():
        if info.get("owner") == owner_id:
            out.append(info)
    return out


def save_all():
    save_vps_data()
    save_user_db()


def is_owner(ctx):
    return ctx.author.id == OWNER_ID


@bot.command(name="adminc")
async def admin_add_credits(ctx, member: discord.Member, amount: int):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can use this command.")
    uid = str(member.id)
    ensure_user_record(uid)
    USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + amount
    save_user_db()
    await ctx.send(f"Added {amount} credits to {member.mention}.")
    await log_action("Credits Added", f"{amount} credits added to {member} by {ctx.author}")


@bot.command(name="adminrc")
async def admin_remove_credits(ctx, member: discord.Member, amount: str):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can use this command.")
    uid = str(member.id)
    ensure_user_record(uid)
    if amount.lower() == "all":
        USER_DB["users"][uid]["credits"] = 0
    else:
        try:
            a = int(amount)
            USER_DB["users"][uid]["credits"] = max(0, USER_DB["users"][uid].get("credits", 0) - a)
        except ValueError:
            return await ctx.send("Amount must be a number or 'all'.")
    save_user_db()
    await ctx.send(f"Updated credits for {member.mention}.")
    await log_action("Credits Removed/Updated", f"Credits updated for {member} by {ctx.author}")


@bot.command(name="create")
async def create_custom_vps(ctx, member: discord.Member, cpu: int, ram: int, storage: int):
    """Owner-only custom VPS creation: .create @user <cpu> <ram> <storage>
    Uses LXC-style metadata (does not provision real containers).
    """
    if not (is_owner(ctx) or is_admin_ctx(ctx)):
        return await ctx.send("Only the owner or an admin can create VPS records.")
    if VPS_DATA.get("maintenance"):
        return await ctx.send("Cannot create VPS while maintenance mode is enabled.")
    uid = str(member.id)
    ensure_user_record(uid)
    vid = create_vps_record(uid, ram, cpu, storage)
    # mark custom
    VPS_DATA.get("vps", {})[vid]["custom"] = True
    VPS_DATA.get("vps", {})[vid]["name"] = f"custom-{vid}"
    save_vps_data()
    await ctx.send(f"Created custom VPS #{vid} for {member.mention} ({cpu} CPU, {ram}GB RAM, {storage}GB storage).")
    await log_action("Custom VPS Created", f"Custom VPS #{vid} created for {member} by {ctx.author}")
    # update assigned role for user if necessary
    try:
        asyncio.create_task(update_vps_role_for_user(uid))
    except Exception:
        pass


@bot.command(name="listall")
async def list_all(ctx):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can use this command.")
    vps = VPS_DATA.get("vps", {})
    users = USER_DB.get("users", {})
    embed = discord.Embed(title="All VPS & Users", color=0x3498db)
    embed.add_field(name="Total VPS", value=str(len(vps)), inline=True)
    embed.add_field(name="Total Users", value=str(len(users)), inline=True)
    # list up to 20 VPS
    lines = []
    for vid, info in list(vps.items())[:20]:
        lines.append(f"#{vid} — owner: <@{info.get('owner')}> ({info.get('status')})")
    embed.add_field(name="VPS (first 20)", value="\n".join(lines) or "None", inline=False)
    await ctx.send(embed=embed)
    await log_action("List All Executed", f"{ctx.author} requested listall")


@bot.command(name="dontpurgevps")
async def dont_purge(ctx, target: str):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can use this command.")
    # if target is a mention
    if target.startswith("<@"):
        # extract id
        tid = ''.join(ch for ch in target if ch.isdigit())
        if tid:
            VPS_DATA.setdefault("purge", {}).setdefault("protected_users", []).append(tid)
            save_vps_data()
            await ctx.send(f"Protected user <@{tid}> from purge.")
            await log_action("Purge Protection Added", f"User <@{tid}> protected from purge by {ctx.author}")
            return
    # otherwise assume vps id
    if target.isdigit():
        VPS_DATA.setdefault("purge", {}).setdefault("protected_vps", []).append(target)
        save_vps_data()
        await ctx.send(f"Protected VPS #{target} from purge.")
        await log_action("Purge Protection Added", f"VPS #{target} protected from purge by {ctx.author}")
        return
    await ctx.send("Could not parse target. Use @user or vps id.")


@bot.command(name="dontpurgevpsr")
async def remove_purge_protection(ctx, target: str):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can use this command.")
    # confirmation view
    await ctx.send(f"Confirm removal of purge protection for {target}", view=RemoveProtectionConfirm(target, ctx.author))


class RemoveProtectionConfirm(ui.View):
    def __init__(self, target: str, requester: discord.User):
        super().__init__(timeout=60)
        self.target = target
        self.requester = requester

    @ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.user.id != self.requester.id and interaction.user.id != OWNER_ID:
            return await interaction.response.send_message("Not authorized.", ephemeral=True)
        t = self.target
        if t.startswith("<@"):
            tid = ''.join(ch for ch in t if ch.isdigit())
            lst = VPS_DATA.setdefault("purge", {}).setdefault("protected_users", [])
            if tid in lst:
                lst.remove(tid)
                save_vps_data()
                await interaction.response.edit_message(content=f"Removed protection for <@{tid}>", view=None)
                await log_action("Purge Protection Removed", f"User <@{tid}> unprotected by {interaction.user}")
                return
        if t.isdigit():
            lst = VPS_DATA.setdefault("purge", {}).setdefault("protected_vps", [])
            if t in lst:
                lst.remove(t)
                save_vps_data()
                await interaction.response.edit_message(content=f"Removed protection for VPS #{t}", view=None)
                await log_action("Purge Protection Removed", f"VPS #{t} unprotected by {interaction.user}")
                return
        await interaction.response.edit_message(content="Target not found in protections.", view=None)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)


class VpsSelectView(ui.View):
    def __init__(self, ctx, user_vps, action="suspend"):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.action = action
        options = []
        for info in user_vps:
            vid = info.get("id")
            label = f"#{vid} — {info.get('cpu')} CPU • {info.get('ram')}GB"
            options.append(discord.SelectOption(label=label, value=str(vid)))

        self.select = ui.Select(placeholder="Select a VPS...", options=options, min_values=1, max_values=1)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        vid = self.select.values[0]
        if self.action == "suspend":
            v = VPS_DATA.get("vps", {}).get(vid)
            if v:
                v["status"] = "suspended"
                save_vps_data()
                await interaction.response.edit_message(content=f"VPS #{vid} suspended.", embed=None, view=None)
                await log_action("VPS Suspended", f"VPS #{vid} suspended by {interaction.user}")
                return
        if self.action == "delete":
            # show confirmation
            await interaction.response.send_message(f"Confirm deletion of VPS #{vid}", view=ConfirmView(vid, interaction.user), ephemeral=True)
            return
        await interaction.response.edit_message(content="Action completed.", embed=None, view=None)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="cancel_vps")
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", embed=None, view=None)


@bot.command(name="suspendvps")
async def suspend_vps(ctx, member: discord.Member):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can suspend VPS.")
    user_vps = list_user_vps(str(member.id))
    if not user_vps:
        return await ctx.send("User has no VPS.")
    view = VpsSelectView(ctx, user_vps, action="suspend")
    msg = await ctx.send("Select a VPS to suspend:", view=view)
    await log_action("Suspend Initiated", f"{ctx.author} initiated suspend for {member}")


@bot.command(name="stopall")
async def stop_all(ctx, *, reason: str = "No reason provided"):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can run this.")
    for vid, info in VPS_DATA.get("vps", {}).items():
        if VPS_DATA.get("maintenance"):
            # if already in maintenance, do not change
            continue
        info["status"] = "stopped"
        info["stop_reason"] = reason
    save_vps_data()
    await ctx.send(f"All VPS stopped: {reason}")
    await log_action("Stop All", f"All VPS stopped by {ctx.author}: {reason}")


@bot.command(name="unsuspend")
async def unsuspend(ctx, member: discord.Member, vps_id: str):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can unsuspend VPS.")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v or v.get("owner") != str(member.id):
        return await ctx.send("VPS not found for that user.")
    v["status"] = "active"
    save_vps_data()
    await ctx.send(f"VPS #{vps_id} unsuspended for {member.mention}.")
    await log_action("Unsuspend VPS", f"VPS #{vps_id} unsuspended for {member} by {ctx.author}")


@bot.command(name="purgestart")
async def purge_start(ctx):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can start the purge.")
    purge = VPS_DATA.setdefault("purge", {})
    purge["active"] = True
    save_vps_data()

    # determine channel to announce purge
    pch = None
    try:
        cfg = load_config()
        pch_id = cfg.get("purge_channel_id")
        if pch_id:
            try:
                pch = bot.get_channel(int(pch_id)) or await bot.fetch_channel(int(pch_id))
            except Exception:
                pch = None
    except Exception:
        pch = None

    # build start embed
    start_emb = discord.Embed(title="🔔 Purge Initiated", description=f"Purge started by {ctx.author.mention}", color=get_embed_color("orange"))
    start_emb.add_field(name="What Happens", value="Unprotected VPS records will be deleted. Protect yourself with `.purgeprotect` or ask an admin.", inline=False)
    start_emb.add_field(name="Reactions", value="React with any emoji below to register (optional).", inline=False)
    start_emb.set_footer(text=f"Initiated by {ctx.author}" )

    target_ch = pch or ctx.channel
    msg = await target_ch.send(embed=start_emb)
    # store purge message reference
    purge["message_id"] = msg.id
    purge["channel_id"] = target_ch.id
    save_vps_data()
    await log_action("Purge Started", f"Purge started by {ctx.author} (announced in {target_ch})")

    # add up to 20 reaction options for users to choose from
    emojis = [
        "1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟",
        "🅰️","🅱️","🆎","🆑","🔵","🟢","🟡","🔴","⚫","⚪"
    ]
    for e in emojis:
        try:
            await msg.add_reaction(e)
        except Exception:
            pass

    # set purge window: users have 72 hours to react and be protected
    now = int(datetime.now(timezone.utc).timestamp())
    end_ts = now + (72 * 3600)
    purge["start_ts"] = now
    purge["end_ts"] = end_ts
    save_vps_data()

    # schedule purge execution after window expires
    try:
        asyncio.create_task(schedule_purge_expiry(end_ts, msg.channel.id, msg.id, ctx.author.id))
    except Exception:
        pass
    # delete unprotected vps
    protected_vps = set(purge.get("protected_vps", []))
    protected_users = set(purge.get("protected_users", []))
    removed = []
    for vid, info in list(VPS_DATA.get("vps", {}).items()):
        if vid in protected_vps or info.get("owner") in protected_users:
            continue
        VPS_DATA.get("vps", {}).pop(vid, None)
        removed.append(vid)
        await log_action("VPS Purged", f"VPS #{vid} purged by {ctx.author}")
    save_vps_data()
    purge["active"] = False
    save_vps_data()

    # announce completion via embed
    complete_emb = discord.Embed(title="✅ Purge Completed", color=get_embed_color("green"))
    complete_emb.add_field(name="Removed VPS", value=(", ".join(removed) if removed else "none"), inline=False)
    complete_emb.set_footer(text=f"Completed by {ctx.author}")
    try:
        await target_ch.send(embed=complete_emb)
    except Exception:
        pass
    await log_action("Purge Completed", f"Removed: {', '.join(removed) if removed else 'none'}")


@bot.command(name="purgestop")
async def purge_stop(ctx):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can stop the purge.")
    purge = VPS_DATA.setdefault("purge", {})
    purge["active"] = False
    save_vps_data()
    # announce stop in configured purge channel if set
    cfg = load_config()
    pch = None
    try:
        pch_id = cfg.get("purge_channel_id")
        if pch_id:
            try:
                pch = bot.get_channel(int(pch_id)) or await bot.fetch_channel(int(pch_id))
            except Exception:
                pch = None
    except Exception:
        pch = None
    stop_emb = discord.Embed(title="⏹️ Purge Stopped", description=f"Purge stopped by {ctx.author}", color=get_embed_color("orange"))
    try:
        if pch:
            await pch.send(embed=stop_emb)
        else:
            await ctx.send(embed=stop_emb)
    except Exception:
        pass
    await log_action("Purge Stopped", f"Purge stopped by {ctx.author}")


async def schedule_purge_expiry(end_ts: int, channel_id: int, message_id: int, requested_by: int):
    """Wait until end_ts then perform the purge based on protections recorded in purge_protected.json and VPS_DATA."""
    now = int(datetime.now(timezone.utc).timestamp())
    wait = max(0, end_ts - now)
    await asyncio.sleep(wait)
    # perform purge
    await execute_scheduled_purge(requested_by)


async def execute_scheduled_purge(requested_by: int):
    purge = VPS_DATA.setdefault("purge", {})
    # load protections from purge_protected.json
    pp = load_purge_protected()
    protected_records = pp.get("protected", []) if isinstance(pp, dict) else []
    protected_user_ids = set()
    now = int(datetime.now(timezone.utc).timestamp())
    for r in protected_records:
        try:
            exp = int(r.get("expires_at", 0))
            if exp >= now:
                protected_user_ids.add(str(r.get("user_id")))
        except Exception:
            continue

    # include any manual protections in VPS_DATA
    protected_vps = set(purge.get("protected_vps", []))
    protected_users = set(purge.get("protected_users", []))

    removed = []
    for vid, info in list(VPS_DATA.get("vps", {}).items()):
        owner = info.get("owner")
        if vid in protected_vps or str(owner) in protected_users or str(owner) in protected_user_ids:
            continue
        VPS_DATA.get("vps", {}).pop(vid, None)
        removed.append(vid)
        await log_action("VPS Purged (Scheduled)", f"VPS #{vid} purged after scheduled window (requested by {requested_by})")
    save_vps_data()

    # announce completion in configured purge channel if available
    cfg = load_config()
    try:
        pch_id = cfg.get("purge_channel_id")
        if pch_id:
            ch = bot.get_channel(int(pch_id)) or await bot.fetch_channel(int(pch_id))
            if ch:
                emb = discord.Embed(title="✅ Scheduled Purge Completed", color=get_embed_color("green"))
                emb.add_field(name="Removed VPS", value=(", ".join(removed) if removed else "none"), inline=False)
                emb.set_footer(text=f"Requested by <@{requested_by}>")
                try:
                    await ch.send(embed=emb)
                except Exception:
                    pass
    except Exception:
        pass


@bot.command(name="purgeinfo")
async def purge_info(ctx):
    purge = VPS_DATA.get("purge", {})
    active = purge.get("active", False)
    pv = purge.get("protected_vps", [])
    pu = purge.get("protected_users", [])
    embed = discord.Embed(title="Purge Info", color=get_embed_color("orange"))
    embed.add_field(name="Active", value=str(active), inline=True)
    embed.add_field(name="Protected VPS Count", value=str(len(pv)), inline=True)
    embed.add_field(name="Protected Users Count", value=str(len(pu)), inline=True)
    await ctx.send(embed=embed)
    await log_action("Purge Info Viewed", f"{ctx.author} viewed purge info")
    


class ConfirmView(ui.View):
    def __init__(self, vps_id: str, requester: discord.User):
        super().__init__(timeout=60)
        self.vps_id = vps_id
        self.requester = requester

    @ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.user.id != self.requester.id and interaction.user.id != OWNER_ID:
            return await interaction.response.send_message("You are not authorized to confirm.", ephemeral=True)
        # perform deletion
        vid = self.vps_id
        removed = VPS_DATA.get("vps", {}).pop(vid, None)
        save_vps_data()
        await interaction.response.edit_message(content=f"Deleted VPS #{vid}.", embed=None, view=None)
        await log_action("VPS Deleted", f"VPS #{vid} deleted by {interaction.user}")
        # update roles for owner if needed
        try:
            if removed and removed.get("owner"):
                asyncio.create_task(update_vps_role_for_user(str(removed.get("owner"))))
        except Exception:
            pass

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", embed=None, view=None)


@bot.command(name="editvps")
async def edit_vps(ctx, member: discord.Member, vps_id: str, new_ram: int, new_cpu: int):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can edit VPS.")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v or v.get("owner") != str(member.id):
        return await ctx.send("VPS not found for that user.")
    v["ram"] = new_ram
    v["cpu"] = new_cpu
    save_vps_data()
    await ctx.send(f"VPS #{vps_id} updated: CPU {new_cpu}, RAM {new_ram}GB")
    await log_action("Edit VPS", f"VPS #{vps_id} updated by {ctx.author}: CPU {new_cpu}, RAM {new_ram}")


@bot.command(name="deletevps")
async def delete_vps(ctx, member: discord.Member):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can delete VPS.")
    user_vps = list_user_vps(str(member.id))
    if not user_vps:
        return await ctx.send("User has no VPS.")
    view = VpsSelectView(ctx, user_vps, action="delete")
    await ctx.send("Select a VPS to delete:", view=view)
    await log_action("Delete VPS Initiated", f"{ctx.author} initiated delete for {member}")
    await log_action("Delete VPS", f"Deleted VPS {', '.join(removed)} for {member} by {ctx.author}")


@bot.command(name="userinfo")
async def user_info(ctx, member: discord.Member):
    uid = str(member.id)
    users = USER_DB.get("users", {})
    u = users.get(uid, {"credits": 0})
    vps = list_user_vps(uid)
    embed = discord.Embed(title=f"User Info — {member}", color=0x1abc9c)
    embed.add_field(name="Credits", value=str(u.get("credits", 0)), inline=True)
    embed.add_field(name="VPS Count", value=str(len(vps)), inline=True)
    lines = [f"#{i.get('id')} — {i.get('cpu')} CPU • {i.get('ram')}GB • {i.get('storage')}GB ({i.get('status')})" for i in vps]
    embed.add_field(name="VPS", value="\n".join(lines) or "None", inline=False)
    await ctx.send(embed=embed)
    await log_action("User Info", f"{ctx.author} viewed info for {member}")


@bot.command(name="vpsinfo")
async def vps_info(ctx, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found.")
    owner = v.get("owner")
    embed = discord.Embed(title=f"VPS #{vps_id}", color=0x9b59b6)
    embed.add_field(name="Owner", value=f"<@{owner}", inline=True)
    embed.add_field(name="CPU", value=str(v.get("cpu")), inline=True)
    embed.add_field(name="RAM", value=f"{v.get('ram')}GB", inline=True)
    embed.add_field(name="Storage", value=f"{v.get('storage')}GB", inline=True)
    embed.add_field(name="Status", value=v.get("status"), inline=True)
    await ctx.send(embed=embed)
    await log_action("VPS Info", f"{ctx.author} viewed info for VPS #{vps_id}")


@bot.command(name="buywc")
async def buy_with_credits(ctx, plan: str):
    if VPS_DATA.get("maintenance"):
        return await ctx.send("Purchases are disabled while maintenance mode is enabled.")
    # simple purchase flow
    plans = {
        "ul-1": {"cost": 170, "cpu": 2, "ram": 8, "storage": 50},
        "ul-2": {"cost": 320, "cpu": 4, "ram": 16, "storage": 100},
        "ul-3": {"cost": 600, "cpu": 6, "ram": 32, "storage": 200},
        "xl-1": {"cost": 900, "cpu": 8, "ram": 48, "storage": 350},
        "xl-2": {"cost": 1200, "cpu": 12, "ram": 64, "storage": 500},
        "basic": {"cost": 150, "cpu": 1, "ram": 25, "storage": 65},
        "standard": {"cost": 200, "cpu": 2, "ram": 50, "storage": 100},
        "pro": {"cost": 300, "cpu": 4, "ram": 75, "storage": 150},
        "premium": {"cost": 500, "cpu": 6, "ram": 100, "storage": 250},
    }
    p = plans.get(plan.lower())
    if not p:
        return await ctx.send("Unknown plan. Available: ul-1, ul-2, ul-3, xl-1, xl-2, basic, standard, pro, premium")
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    balance = USER_DB["users"][uid].get("credits", 0)
    if balance < p["cost"]:
        return await ctx.send(f"Insufficient credits. You have {balance}, need {p['cost']}")
    USER_DB["users"][uid]["credits"] = balance - p["cost"]
    vid = create_vps_record(uid, p["ram"], p["cpu"], p["storage"])
    save_user_db()
    await ctx.send(f"Purchased plan {plan} as VPS #{vid}. Remaining credits: {USER_DB['users'][uid]['credits']}")
    await log_action("Plan Purchased", f"{ctx.author} purchased {plan} as VPS #{vid}")
    try:
        asyncio.create_task(update_vps_role_for_user(uid))
    except Exception:
        pass


# ---------- NEW COMMANDS (20+) ----------

@bot.command(name="transfer")
async def transfer(ctx, member: discord.Member, amount: int):
    if amount <= 0:
        return await ctx.send("Amount must be positive")
    uid = str(ctx.author.id)
    tid = str(member.id)
    ensure_user_record(uid)
    ensure_user_record(tid)
    if USER_DB["users"][uid].get("credits", 0) < amount:
        return await ctx.send("Insufficient balance")
    USER_DB["users"][uid]["credits"] -= amount
    USER_DB["users"][tid]["credits"] = USER_DB["users"][tid].get("credits", 0) + amount
    save_user_db()
    await ctx.send(f"Transferred {amount} credits to {member.mention}")
    await log_action("Transfer", f"{ctx.author} transferred {amount} to {member}")


@bot.command(name="aadmin")
async def add_admin(ctx, member: discord.Member):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can add admins.")
    uid = str(member.id)
    ensure_user_record(uid)
    USER_DB["users"][uid]["is_admin"] = True
    save_user_db()
    await ctx.send(f"{member.mention} is now an admin.")
    await log_action("Admin Added", f"{member} granted admin by {ctx.author}")


@bot.command(name="radmin")
async def remove_admin(ctx, member: discord.Member):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can remove admins.")
    uid = str(member.id)
    ensure_user_record(uid)
    USER_DB["users"][uid]["is_admin"] = False
    save_user_db()
    await ctx.send(f"{member.mention} is no longer an admin.")
    await log_action("Admin Removed", f"{member} admin removed by {ctx.author}")


@bot.command(name="daily")
async def daily(ctx):
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    now = int(datetime.now(timezone.utc).timestamp())
    last = USER_DB["users"][uid].get("last_daily", 0)
    if now - last < 24*3600:
        return await ctx.send("You already claimed your daily reward.")
    USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + 50
    USER_DB["users"][uid]["last_daily"] = now
    save_user_db()
    await ctx.send("You claimed 50 credits (daily reward).")
    await log_action("Daily Claimed", f"{ctx.author} claimed daily 50 credits")


@bot.command(name="weekly")
async def weekly(ctx):
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    now = int(datetime.now(timezone.utc).timestamp())
    last = USER_DB["users"][uid].get("last_weekly", 0)
    if now - last < 7*24*3600:
        return await ctx.send("You already claimed your weekly reward.")
    USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + 400
    USER_DB["users"][uid]["last_weekly"] = now
    save_user_db()
    await ctx.send("You claimed 400 credits (weekly reward).")
    await log_action("Weekly Claimed", f"{ctx.author} claimed weekly 400 credits")


@bot.command(name="gamble")
async def gamble(ctx, amount: int):
    if amount <= 0:
        return await ctx.send("Enter a positive amount.")
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    balance = USER_DB["users"][uid].get("credits", 0)
    if balance < amount:
        return await ctx.send("Insufficient credits.")
    # 45% win double, 5% jackpot x10, else lose
    roll = random.random()
    USER_DB["users"][uid]["credits"] = balance - amount
    if roll < 0.05:
        win = amount * 10
        USER_DB["users"][uid]["credits"] += win
        result = f"JACKPOT! You won {win} credits!"
    elif roll < 0.5:
        win = amount * 2
        USER_DB["users"][uid]["credits"] += win
        result = f"You won {win} credits!"
    else:
        result = "You lost your bet."
    save_user_db()
    await ctx.send(result)
    await log_action("Gamble", f"{ctx.author} gambled {amount}. Result: {result}")


@bot.command(name="leaderboard")
async def leaderboard(ctx):
    users = USER_DB.get("users", {})
    items = [(uid, data.get("credits", 0)) for uid, data in users.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    top = items[:10]
    lines = [f"{i+1}. <@{uid}> — {amt} credits" for i, (uid, amt) in enumerate(top)]
    embed = discord.Embed(title="Top Credit Holders", description="\n".join(lines) or "No data", color=get_embed_color("primary"))
    await ctx.send(embed=embed)


@bot.command(name="redeem")
async def redeem(ctx, code: str):
    c = PROMOS.get("promos", {}).get(code)
    if not c:
        return await ctx.send("Invalid promo code.")
    if c.get("uses", 0) <= 0:
        return await ctx.send("This promo has expired.")
    uid = str(ctx.author.id)
    ensure_user_record(uid)
    USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + int(c.get("amount", 0))
    c["uses"] = c.get("uses", 0) - 1
    save_promos(PROMOS)
    save_user_db()
    await ctx.send(f"Redeemed promo {code}: awarded {c.get('amount')} credits.")
    await log_action("Promo Redeemed", f"{ctx.author} redeemed {code}")


@bot.command(name="setlog")
async def set_log_channel(ctx, channel: discord.TextChannel):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can set the log channel.")
    config["log_channel_id"] = int(channel.id)
    save_config()
    try:
        DISCORD_LOG_HANDLER.set_target(bot, channel.id)
        logging.info(f"Log channel updated to {channel.id}")
    except Exception:
        pass
    await ctx.send(f"Log channel set to {channel.mention}")


@bot.command(name="exportdb")
async def export_db(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    path = DATABASE_DIR / f"user_database_export_{int(datetime.now(timezone.utc).timestamp())}.json"
    path.write_text(json.dumps(USER_DB, indent=2))
    await ctx.send(f"Exported user DB to {path}")


@bot.command(name="snapshot")
async def snapshot_vps(ctx, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    if not vps_manage_allowed(ctx.author, v):
        return await ctx.send("Not allowed")
    snaps = v.setdefault("snapshots", [])
    sid = secrets.token_hex(6)
    snaps.append({"id": sid, "created_at": datetime.now(timezone.utc).isoformat()})
    save_vps_data()
    await ctx.send(f"Snapshot {sid} created for VPS #{vps_id}")


@bot.command(name="backupvps")
async def backup_vps(ctx, vps_id: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    snaps = v.setdefault("snapshots", [])
    sid = secrets.token_hex(6)
    snaps.append({"id": sid, "created_at": datetime.now(timezone.utc).isoformat(), "type": "backup"})
    save_vps_data()
    await ctx.send(f"Backup {sid} created for VPS #{vps_id}")
    await log_action("Backup Created", f"Backup {sid} for VPS #{vps_id} by {ctx.author}")


@bot.command(name="restorevps")
async def restore_vps(ctx, vps_id: str, snapshot_id: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    snaps = v.get("snapshots", [])
    if not any(s.get("id") == snapshot_id for s in snaps):
        return await ctx.send("Snapshot not found")
    # simulate restore
    v["restored_from"] = snapshot_id
    save_vps_data()
    await ctx.send(f"VPS #{vps_id} restored from {snapshot_id}")
    await log_action("VPS Restored", f"VPS #{vps_id} restored from {snapshot_id} by {ctx.author}")


@bot.command(name="setprice")
async def set_price(ctx, plan: str, amount: int):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    prices = config.setdefault("prices", {})
    prices[plan.lower()] = int(amount)
    save_config()
    await ctx.send(f"Set price for {plan} to {amount}")


@bot.command(name="grantpromo")
async def grant_promo(ctx, code: str, amount: int, uses: int = 1):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    PROMOS.setdefault("promos", {})[code] = {"amount": int(amount), "uses": int(uses)}
    save_promos(PROMOS)
    await ctx.send(f"Created promo {code} for {amount} credits ({uses} uses)")


@bot.command(name="listpromos")
async def list_promos(ctx):
    lines = [f"{c} — {d['amount']} credits ({d['uses']} left)" for c, d in PROMOS.get("promos", {}).items()]
    await ctx.send("\n".join(lines) or "No promos")


@bot.command(name="removepromo")
async def remove_promo(ctx, code: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    if code in PROMOS.get("promos", {}):
        PROMOS.get("promos", {}).pop(code, None)
        save_promos(PROMOS)
        await ctx.send(f"Removed promo {code}")
    else:
        await ctx.send("Promo not found")


@bot.command(name="renamevps")
async def rename_vps(ctx, vps_id: str, *, name: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    if not vps_manage_allowed(ctx.author, v):
        return await ctx.send("Not allowed")
    v["name"] = name
    save_vps_data()
    await ctx.send(f"VPS #{vps_id} renamed to {name}")


@bot.command(name="setmaintenancewindow")
async def set_maintenance_window(ctx, start_ts: int, end_ts: int):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    VPS_DATA["maintenance_window"] = {"start": int(start_ts), "end": int(end_ts)}
    save_vps_data()
    await ctx.send("Maintenance window set")


@bot.command(name="promoinfo")
async def promo_info(ctx, code: str):
    p = PROMOS.get("promos", {}).get(code)
    if not p:
        return await ctx.send("Promo not found")
    await ctx.send(f"{code} — {p['amount']} credits ({p['uses']} uses remaining)")

# end of new commands


class HelpView(ui.View):
    def __init__(self, pages: list):
        super().__init__(timeout=None)
        self.pages = pages
        self.index = 0

    @ui.button(label="⏮️", style=discord.ButtonStyle.secondary)
    async def first(self, interaction: discord.Interaction, button: ui.Button):
        self.index = 0
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @ui.button(label="◀️", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        self.index = max(0, self.index - 1)
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @ui.button(label="▶️", style=discord.ButtonStyle.secondary)
    async def forward(self, interaction: discord.Interaction, button: ui.Button):
        self.index = min(len(self.pages) - 1, self.index + 1)
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @ui.button(label="⏭️", style=discord.ButtonStyle.secondary)
    async def last(self, interaction: discord.Interaction, button: ui.Button):
        self.index = len(self.pages) - 1
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)


@bot.command(name="help")
async def help_cmd(ctx, page: int = 1):
    # paginated help with navigation
    version = config.get("version", "1.0.0")
    now = datetime.now(timezone.utc)
    ts = now.strftime('%b %d, %Y %I:%M %p')
    # Page 1
    e1 = discord.Embed(title=f"📚 VPS Management Bot - Help (Page 1/3)", color=get_embed_color("primary"))
    e1.add_field(name="Vortex Nodes", value=f"v{version}", inline=False)
    e1.add_field(name="User Commands", value=(".plans — View available VPS plans\n"
                                               ".manage [user] — Manage your VPS (admin: others)\n"
                                               ".buywc <plan> — Buy VPS with credits\n"
                                               ".freeplans — View free plans (boost/invite)\n"
                                               ".credits — Check your credits balance\n"
                                               ".shareuser <user> <vps#> — Share VPS access\n"
                                               ".shareruser <user> <vps#> — Revoke shared access\n"
                                               ".manageshared <owner> <vps#> — Manage shared VPS"), inline=False)
    e1.set_footer(text=f"Vortex Nodes v{version} Today At {ts}")

    # Page 2
    e2 = discord.Embed(title=f"📚 VPS Management Bot - Help (Page 2/3)", color=get_embed_color("primary"))
    e2.add_field(name="Admin Commands", value=(".adminc <@user> <amount> — Add credits\n"
                                               ".adminrc <@user> <amount/all> — Remove credits\n"
                                               ".create <@user> <cpu> <ram> <disk> — Create custom VPS\n"
                                               ".listall — List all VPS and users\n"
                                               ".suspendvps <@user> — Suspend a VPS (interactive)\n"
                                               ".unsuspend <user> <vps#> — Unsuspend a VPS\n"
                                               ".deletevps <user> — Delete a user's VPS (interactive)"), inline=False)
    e2.set_footer(text=f"Vortex Nodes v{version} Today At {ts}")

    # Page 3
    e3 = discord.Embed(title=f"📚 VPS Management Bot - Help (Page 3/3)", color=get_embed_color("primary"))
    e3.add_field(name="Other Tools", value=(".giveawayc — Owner-only start giveaway\n"
                                               ".deploy <@user> — Owner-only deploy free VPS\n"
                                               ".purgeinfo/.purgestart/.purgestop — Purge system tools\n"
                                               ".maintenance on/off — Toggle maintenance \n"
                                               ".plans, .help, .manage — Navigation and info"), inline=False)
    e3.set_footer(text=f"Vortex Nodes v{version} Today At {ts}")

    pages = [e1, e2, e3]
    page = max(1, min(page, len(pages)))
    view = HelpView(pages)
    await ctx.send(embed=pages[page-1], view=view)


class DeploySelectView(ui.View):
    def __init__(self, target_member: discord.Member):
        super().__init__(timeout=120)
        self.target = target_member
        options = []
        for p in FREE_PLANS:
            label = f"{p['name']} — {p['cpu']} CPU • {p['ram']}GB • {p['storage']}GB"
            options.append(discord.SelectOption(label=label, value=p['id']))
        self.select = ui.Select(placeholder="Choose a plan to deploy", options=options, min_values=1, max_values=1)
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        pid = self.select.values[0]
        plan = next((x for x in FREE_PLANS if x['id'] == pid), None)
        if not plan:
            return await interaction.response.send_message("Plan not found.", ephemeral=True)
        # ask for confirmation
        embed = discord.Embed(title="Confirm Deployment", color=get_embed_color("orange"))
        embed.description = f"Are you sure you want to deploy {plan['name']} for {self.target.mention}?"
        embed.add_field(name="Specs", value=(f"• ⚡ CPU: {plan['cpu']}\n"
                                              f"• 💾 RAM: {plan['ram']}GB\n"
                                              f"• 📦 Storage: {plan['storage']}GB"), inline=False)
        class ConfirmDeployView(ui.View):
            def __init__(self, target_member: discord.Member, plan: dict):
                super().__init__(timeout=120)
                self.target = target_member
                self.plan = plan

            @ui.button(label="Confirm", style=discord.ButtonStyle.success)
            async def confirm(self, interaction: discord.Interaction, button: ui.Button):
                # create vps for target
                vid = create_vps_record(str(self.target.id), self.plan['ram'], self.plan['cpu'], self.plan['storage'])
                VPS_DATA.get("vps", {})[vid]["deployed_from"] = self.plan['id']
                save_vps_data()
                await interaction.response.edit_message(content=f"Deployed VPS #{vid} for {self.target.mention}.", embed=None, view=None)
                await log_action("Deploy Completed", f"{interaction.user} deployed {self.plan['name']} for {self.target}")
                try:
                    asyncio.create_task(update_vps_role_for_user(str(self.target.id)))
                except Exception:
                    pass

            @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, interaction: discord.Interaction, button: ui.Button):
                await interaction.response.edit_message(content="Deployment cancelled.", embed=None, view=None)

            @ui.button(label="Go Back", style=discord.ButtonStyle.primary)
            async def go_back(self, interaction: discord.Interaction, button: ui.Button):
                new_view = DeploySelectView(self.target)
                embed2 = discord.Embed(title="Deploy VPS", description=f"Please select the VPS you want to deploy for {self.target.mention}.", color=get_embed_color("orange"))
                await interaction.response.edit_message(content=None, embed=embed2, view=new_view)

        view = ConfirmDeployView(self.target, plan)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)


@bot.command(name="deploy")
async def deploy(ctx, member: discord.Member):
    if not (ctx.author.id == OWNER_ID or is_admin_ctx(ctx)):
        return await ctx.send("Only the owner or an admin can use this command.")
    embed = discord.Embed(title="Deploy VPS", description=f"Please select the VPS you want to deploy for {member.mention}. Choose a plan from the list below.", color=get_embed_color("orange"))
    view = DeploySelectView(member)
    await ctx.send(embed=embed, view=view)
    await log_action("Deploy Started", f"{ctx.author} started deploy flow for {member}")



@bot.command(name="manage")
async def manage(ctx):
    user_vps = list_user_vps(str(ctx.author.id))
    if not user_vps:
        embed = discord.Embed(
            title="ℹ️ No VPS Found",
            description="You don't have any virtual servers yet.",
            color=get_embed_color("primary"),
        )

        body = (
            "• Use .plans to view available plans\n"
            "• Use .buywc <plan> to purchase a VPS\n"
            "• Use .freeplans to view free options\n"
            "• Admins can create custom VPS with .create"
        )

        embed.add_field(name="", value=body, inline=False)
        thumb = config.get("thumbnail")
        if thumb:
            embed.set_image(url=thumb)

        version = config.get("version", "1.0.0")
        try:
            if ZoneInfo:
                now = datetime.now(ZoneInfo("America/New_York"))
            else:
                now = datetime.now(timezone.utc)
            ts = now.strftime("%b %d, %Y %I:%M %p")
        except Exception:
            ts = datetime.now(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC")

        embed.set_footer(text=f"Vortex Nodes | v{version} Today at {ts}")
        await ctx.send(embed=embed)
        await log_action("Manage Used", f"{ctx.author} used .manage (no VPS)")
        return

    # If user has VPS, show a simple list
    if len(user_vps) == 1:
        v = user_vps[0]
        await send_vps_management(ctx, v, requester=ctx.author)
        return

    # multiple vps -> show select menu
    options = [discord.SelectOption(label=f"#{v.get('id')} — {v.get('cpu')}CPU {v.get('ram')}GB", value=v.get('id')) for v in user_vps]
    view = ui.View()
    select = ui.Select(placeholder="Select a VPS to manage", options=options, min_values=1, max_values=1)

    async def select_cb(interaction: discord.Interaction):
        vid = select.values[0]
        v = VPS_DATA.get("vps", {}).get(vid)
        if not v:
            return await interaction.response.send_message("VPS not found.", ephemeral=True)
        await send_vps_management(interaction, v, requester=interaction.user)
        try:
            await interaction.message.delete()
        except Exception:
            pass

    select.callback = select_cb
    view.add_item(select)
    await ctx.send("Select a VPS to manage:", view=view)


def vps_manage_allowed(user: discord.User, v: dict) -> bool:
    # allowed if user is owner of VPS or global owner
    try:
        return str(user.id) == str(v.get("owner")) or user.id == OWNER_ID
    except Exception:
        return False


@bot.command(name="shareuser")
async def share_user(ctx, member: discord.Member, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found.")
    if not vps_manage_allowed(ctx.author, v):
        return await ctx.send("You are not allowed to share this VPS.")
    sw = v.setdefault("shared_with", [])
    if str(member.id) in sw:
        return await ctx.send(f"{member.mention} already has access to VPS #{vps_id}.")
    sw.append(str(member.id))
    save_vps_data()
    await ctx.send(f"Shared VPS #{vps_id} with {member.mention}.")
    await log_action("VPS Shared", f"VPS #{vps_id} shared with {member} by {ctx.author}")


@bot.command(name="shareruser")
async def revoke_shared_user(ctx, member: discord.Member, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found.")
    if not vps_manage_allowed(ctx.author, v):
        return await ctx.send("You are not allowed to revoke sharing for this VPS.")
    sw = v.setdefault("shared_with", [])
    if str(member.id) not in sw:
        return await ctx.send(f"{member.mention} does not have shared access to VPS #{vps_id}.")
    sw.remove(str(member.id))
    save_vps_data()
    await ctx.send(f"Revoked shared access for {member.mention} on VPS #{vps_id}.")
    await log_action("VPS Share Revoked", f"Shared access revoked for {member} on VPS #{vps_id} by {ctx.author}")


# ---------------- Additional Commands (to expand feature set) ----------------


@bot.command(name="allocateip")
async def allocate_ip(ctx, vps_id: str, ip: str):
    """Owner-only: record allocation of an IPv4 to a VPS (simulated)."""
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    VPS_DATA.setdefault("ips", {})[ip] = {"vps": vps_id, "assigned_at": int(datetime.now(timezone.utc).timestamp())}
    save_vps_data()
    await ctx.send(f"Allocated IP {ip} -> VPS #{vps_id}")
    await log_action("IP Allocated", f"{ip} allocated to VPS #{vps_id} by {ctx.author}")


@bot.command(name="releaseip")
async def release_ip(ctx, ip: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    ips = VPS_DATA.setdefault("ips", {})
    if ip in ips:
        ips.pop(ip, None)
        save_vps_data()
        await ctx.send(f"Released IP {ip}")
        await log_action("IP Released", f"{ip} released by {ctx.author}")
    else:
        await ctx.send("IP not tracked")


@bot.command(name="listips")
async def list_ips(ctx):
    ips = VPS_DATA.get("ips", {})
    if not ips:
        return await ctx.send("No IPs tracked")
    lines = [f"{ip} -> VPS #{data.get('vps')} (since <t:{data.get('assigned_at')}:R>)" for ip, data in ips.items()]
    await ctx.send("\n".join(lines))


@bot.command(name="assignip")
async def assign_ip_to_vps(ctx, vps_id: str, ip: str):
    # attach tracked ip to vps metadata
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    ips = v.setdefault("ips", [])
    if ip in ips:
        return await ctx.send("IP already assigned to this VPS")
    ips.append(ip)
    VPS_DATA.setdefault("ips", {})[ip] = {"vps": vps_id, "assigned_at": int(datetime.now(timezone.utc).timestamp())}
    save_vps_data()
    await ctx.send(f"Assigned {ip} to VPS #{vps_id}")
    await log_action("IP Assigned", f"{ip} assigned to VPS #{vps_id} by {ctx.author}")


@bot.command(name="removeip")
async def remove_ip_from_vps(ctx, vps_id: str, ip: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    ips = v.setdefault("ips", [])
    if ip in ips:
        ips.remove(ip)
        VPS_DATA.setdefault("ips", {}).pop(ip, None)
        save_vps_data()
        await ctx.send(f"Removed IP {ip} from VPS #{vps_id}")
        await log_action("IP Removed", f"{ip} removed from VPS #{vps_id} by {ctx.author}")
    else:
        await ctx.send("IP not assigned to that VPS")


@bot.command(name="vpsips")
async def vps_ips(ctx, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    ips = v.get("ips", [])
    await ctx.send("Assigned IPs: " + (", ".join(ips) if ips else "none"))


@bot.command(name="announce")
async def announce(ctx, channel: discord.TextChannel, *, message: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    await channel.send(message)
    await ctx.send(f"Announced to {channel.mention}")
    await log_action("Announcement", f"{ctx.author} announced to {channel}: {message}")


@bot.command(name="embedannounce")
async def embed_announce(ctx, channel: discord.TextChannel, title: str, *, body: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    embed = discord.Embed(title=title, description=body, color=get_embed_color("purple"))
    await channel.send(embed=embed)
    await ctx.send(f"Embed announced to {channel.mention}")
    await log_action("Embed Announcement", f"{ctx.author} sent embed to {channel}")


@bot.command(name="shutdown")
async def shutdown_bot(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    await ctx.send("Shutting down...")
    await log_action("Shutdown", f"Shutdown requested by {ctx.author}")
    await bot.close()


@bot.command(name="stats")
async def stats(ctx):
    vps = VPS_DATA.get("vps", {})
    users = USER_DB.get("users", {})
    total_credits = sum(d.get("credits", 0) for d in users.values())
    embed = discord.Embed(title="Bot Stats", color=get_embed_color("primary"))
    embed.add_field(name="Total VPS", value=str(len(vps)), inline=True)
    embed.add_field(name="Total Users", value=str(len(users)), inline=True)
    embed.add_field(name="Total Credits In System", value=str(total_credits), inline=False)
    await ctx.send(embed=embed)


@bot.command(name="cleardb")
async def clear_db(ctx, which: str = "users"):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    if which == "users":
        USER_DB["users"] = {}
        save_user_db()
        await ctx.send("User DB cleared")
        await log_action("DB Cleared", f"User DB cleared by {ctx.author}")
    elif which == "vps":
        VPS_DATA["vps"] = {}
        save_vps_data()
        await ctx.send("VPS DB cleared")
        await log_action("DB Cleared", f"VPS DB cleared by {ctx.author}")
    else:
        await ctx.send("Unknown DB. Use 'users' or 'vps'.")


@bot.command(name="importdb")
async def import_db(ctx, which: str, filename: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    path = Path(filename)
    if not path.exists():
        return await ctx.send("File not found on disk")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return await ctx.send(f"Failed to parse file: {e}")
    if which == "users":
        USER_DB.update(data)
        save_user_db()
        await ctx.send("Imported users DB")
    elif which == "vps":
        VPS_DATA.update(data)
        save_vps_data()
        await ctx.send("Imported VPS DB")
    else:
        await ctx.send("Unknown DB type")


@bot.command(name="purgesnapshots")
async def purge_snapshots_older(ctx, days: int = 30):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    cutoff = int((datetime.now(timezone.utc).timestamp()) - days * 86400)
    removed = 0
    for vid, v in VPS_DATA.get("vps", {}).items():
        snaps = v.get("snapshots", [])
        new_snaps = [s for s in snaps if int(datetime.fromisoformat(s.get("created_at")).timestamp()) >= cutoff]
        removed += (len(snaps) - len(new_snaps))
        v["snapshots"] = new_snaps
    save_vps_data()
    await ctx.send(f"Purged {removed} snapshots older than {days} days")
    await log_action("Snapshots Purged", f"{removed} snapshots purged by {ctx.author}")


@bot.command(name="addnote")
async def add_note(ctx, vps_id: str, *, note: str):
    if ctx.author.id != OWNER_ID and not str(ctx.author.id) == str(VPS_DATA.get("vps", {}).get(vps_id, {}).get("owner")):
        return await ctx.send("Not allowed")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    notes = v.setdefault("notes", [])
    nid = secrets.token_hex(4)
    notes.append({"id": nid, "author": str(ctx.author.id), "note": note, "ts": int(datetime.now(timezone.utc).timestamp())})
    save_vps_data()
    await ctx.send(f"Added note {nid} to VPS #{vps_id}")


@bot.command(name="viewnotes")
async def view_notes(ctx, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    notes = v.get("notes", [])
    if not notes:
        return await ctx.send("No notes")
    lines = [f"{n['id']} by <@{n['author']}>: {n['note']}" for n in notes]
    await ctx.send("\n".join(lines))


@bot.command(name="removenote")
async def remove_note(ctx, vps_id: str, note_id: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found")
    notes = v.get("notes", [])
    new = [n for n in notes if n.get("id") != note_id]
    v["notes"] = new
    save_vps_data()
    await ctx.send(f"Removed note {note_id} from VPS #{vps_id}")


@bot.command(name="senddm")
async def send_dm(ctx, member: discord.Member, *, message: str):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    try:
        dm = await member.create_dm()
        await dm.send(message)
        await ctx.send("Sent DM")
    except Exception:
        await ctx.send("Failed to send DM")


@bot.command(name="bulkgrant")
async def bulk_grant(ctx, amount: int, *members: discord.Member):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    for m in members:
        uid = str(m.id)
        ensure_user_record(uid)
        USER_DB["users"][uid]["credits"] = USER_DB["users"][uid].get("credits", 0) + amount
    save_user_db()
    await ctx.send(f"Granted {amount} credits to {len(members)} users")


@bot.command(name="giveawayinfo")
async def giveaway_info(ctx, gid: str):
    gw = GIVEAWAYS.get("giveaways", {}).get(gid)
    if not gw:
        return await ctx.send("Giveaway not found")
    await ctx.send(json.dumps(gw, indent=2))


@bot.command(name="debug")
async def debug_info(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    info = {
        "vps_count": len(VPS_DATA.get("vps", {})),
        "user_count": len(USER_DB.get("users", {})),
        "giveaways": len(GIVEAWAYS.get("giveaways", {})),
    }
    await ctx.send(f"Debug: {info}")


# mark the new commands section complete in the todo list
from datetime import timezone


class ConfirmDeleteAll(ui.View):
    def __init__(self, requester: discord.User):
        super().__init__(timeout=60)
        self.requester = requester

    @ui.button(label="Confirm Delete All", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.user.id != self.requester.id and interaction.user.id != OWNER_ID:
            return await interaction.response.send_message("Not authorized.", ephemeral=True)
        # create backup before deleting
        try:
            ts = int(datetime.now(timezone.utc).timestamp())
            backup_path = DATABASE_DIR / f"vps_backup_{ts}.json"
            backup_path.write_text(json.dumps({"vps": VPS_DATA.get("vps", {}), "meta": {"backup_ts": ts}}, indent=2), encoding="utf-8")
            # record last backup path
            VPS_DATA["last_backup"] = str(backup_path)
        except Exception:
            backup_path = None
        VPS_DATA["vps"] = {}
        save_vps_data()
        # remove role from all guilds for vps users
        try:
            asyncio.create_task(remove_vps_role_from_all_guilds())
        except Exception:
            pass
        msg = "All VPS records have been deleted."
        if backup_path:
            msg += f" Backup saved to {backup_path.name}. Use .restore-vps {backup_path.name} to restore."
        await interaction.response.edit_message(content=msg, embed=None, view=None)
        await log_action("Delete All", f"All VPS deleted by {interaction.user}; backup={backup_path}")

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", embed=None, view=None)


@bot.command(name="list-all")
async def list_all_detailed(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can use this command.")
    vps = VPS_DATA.get("vps", {})
    if not vps:
        return await ctx.send("No VPS records found.")
    lines = []
    for vid, info in sorted(vps.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else x[0]):
        owner = info.get("owner")
        status = info.get("status", "unknown")
        cpu = info.get("cpu")
        ram = info.get("ram")
        storage = info.get("storage")
        plan = info.get("deployed_from") or info.get("plan") or info.get("custom") or "-"
        ips = ", ".join(info.get("ips", [])) if info.get("ips") else "none"
        lines.append(f"#{vid} — owner:<@{owner}> — {status} — {cpu}CPU/{ram}GB/{storage}GB — plan:{plan} — ips:{ips}")
    # send in chunks if large
    chunk_size = 20
    for i in range(0, len(lines), chunk_size):
        await ctx.send("\n".join(lines[i:i+chunk_size]))
    await log_action("List All Detailed", f"{ctx.author} listed all VPS ({len(lines)})")


@bot.command(name="delete-all")
async def delete_all(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can use this command.")
    await ctx.send("Are you sure you want to DELETE ALL VPS records? This will create a backup and cannot be undone.", view=ConfirmDeleteAll(ctx.author))


@bot.command(name="restore-vps")
async def restore_vps_db(ctx, filename: str = None):
    """Owner-only: restore VPS DB from a backup file in the `database/` folder."""
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    # choose file: provided or last_backup
    if not filename:
        last = VPS_DATA.get("last_backup")
        if not last:
            return await ctx.send("No last backup recorded. Provide a filename in the database folder.")
        filename = Path(last).name
    path = DATABASE_DIR / filename
    if not path.exists():
        return await ctx.send("Backup file not found in database folder.")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return await ctx.send(f"Failed to read backup: {e}")
    # validate
    if not isinstance(data, dict) or "vps" not in data:
        return await ctx.send("Invalid backup format.")
    VPS_DATA.setdefault("vps", {}).clear()
    VPS_DATA["vps"] = data.get("vps", {})
    save_vps_data()
    await ctx.send(f"Restored VPS DB from {filename}")
    await log_action("VPS Restored", f"VPS DB restored from {filename} by {ctx.author}")


@bot.command(name="stop-all")
async def stop_all_with_reason(ctx, *, reason: str = "No reason provided"):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can run this.")
    stopped = 0
    for vid, info in VPS_DATA.get("vps", {}).items():
        if info.get("status") != "stopped":
            info["status"] = "stopped"
            info["stop_reason"] = reason
            stopped += 1
    save_vps_data()
    await ctx.send(f"Stopped {stopped} VPS: {reason}")
    await log_action("Stop All", f"All VPS stopped by {ctx.author}: {reason}")


@bot.command(name="export-vps")
async def export_vps(ctx):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Owner only")
    path = DATABASE_DIR / f"vps_export_{int(datetime.now(timezone.utc).timestamp())}.json"
    path.write_text(json.dumps(VPS_DATA, indent=2))
    await ctx.send(f"Exported VPS DB to {path}")
    await log_action("Export VPS", f"VPS DB exported by {ctx.author} -> {path}")


@bot.command(name="count-vps")
async def count_vps(ctx):
    total = len(VPS_DATA.get("vps", {}))
    await ctx.send(f"Total VPS: {total}")


@bot.command(name="paidplans")
async def paid_plans_cmd(ctx, channel: discord.TextChannel = None):
    if ctx.author.id != OWNER_ID:
        return await ctx.send("Only the owner can use this command.")
    plans = config.get("paid_plans", {})
    if not plans:
        return await ctx.send("No paid plans configured in config.json")
    embed = discord.Embed(title="💳 Paid Plans", description="Available paid plans and pricing", color=get_embed_color("orange"))
    for name, info in plans.items():
        cpu = info.get("cpu")
        ram = info.get("ram")
        disk = info.get("disk")
        monthly = info.get("monthly")
        lifetime = info.get("lifetime")
        embed.add_field(name=name, value=(f"• CPU: {cpu}\n• RAM: {ram}GB\n• Disk: {disk}GB\n"
                                          f"• ${monthly}/month • ${lifetime} lifetime"), inline=False)
    target = channel or ctx.channel
    await target.send(embed=embed)
    await ctx.send(f"Paid plans posted to {target.mention}")
    await log_action("Paid Plans Posted", f"{ctx.author} posted paid plans to {target}")




class ManageSharedView(ui.View):
    def __init__(self, vps_id: str, requester: discord.User, shared_list: list):
        super().__init__(timeout=120)
        self.vps_id = vps_id
        self.requester = requester
        options = [discord.SelectOption(label=f"User {i}", value=i) for i in shared_list]
        if options:
            self.select = ui.Select(placeholder="Select a shared user to revoke", options=options)
            self.select.callback = self.on_select
            self.add_item(self.select)
        else:
            # no shared users
            pass

    async def on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester.id and interaction.user.id != OWNER_ID:
            return await interaction.response.send_message("Not authorized.", ephemeral=True)
        vid = self.vps_id
        chosen = self.select.values[0]
        v = VPS_DATA.get("vps", {}).get(vid)
        if not v:
            return await interaction.response.send_message("VPS not found.", ephemeral=True)
        sw = v.setdefault("shared_with", [])
        if chosen in sw:
            sw.remove(chosen)
            save_vps_data()
            await interaction.response.edit_message(content=f"Removed shared user <@{chosen}> from VPS #{vid}", embed=None, view=None)
            await log_action("Shared User Removed", f"Removed <@{chosen}> from VPS #{vid} by {interaction.user}")
        else:
            await interaction.response.send_message("User not found in shared list.", ephemeral=True)


@bot.command(name="manageshared")
async def manage_shared(ctx, owner: discord.Member, vps_id: str):
    v = VPS_DATA.get("vps", {}).get(vps_id)
    if not v:
        return await ctx.send("VPS not found.")
    # only VPS owner or global owner can manage
    if not (str(ctx.author.id) == str(v.get("owner")) or ctx.author.id == OWNER_ID):
        return await ctx.send("You are not allowed to manage shared access for this VPS.")
    shared = v.get("shared_with", [])
    if not shared:
        return await ctx.send("No users have shared access to this VPS.")
    embed = discord.Embed(title=f"Manage Shared — VPS #{vps_id}", description="Select a user below to revoke access", color=get_embed_color("primary"))
    embed.add_field(name="Shared Users", value="\n".join([f"<@{u}>" for u in shared]), inline=False)
    view = ManageSharedView(vps_id=vps_id, requester=ctx.author, shared_list=shared)
    await ctx.send(embed=embed, view=view)


@bot.command(name="maintenance")
async def maintenance_toggle(ctx, mode: str):
    if not is_owner(ctx):
        return await ctx.send("Only the owner can toggle maintenance.")
    m = mode.lower()
    if m not in ("on", "off"):
        return await ctx.send("Usage: .maintenance on/off")
    VPS_DATA["maintenance"] = (m == "on")
    if m == "on":
        # stop all vps
        for vid, info in VPS_DATA.get("vps", {}).items():
            info["status"] = "stopped"
        save_vps_data()
        await ctx.send("Maintenance mode enabled. All VPS stopped and users cannot create/start VPS.")
        await log_action("Maintenance Enabled", f"Maintenance enabled by {ctx.author}")
    else:
        save_vps_data()
        await ctx.send("Maintenance mode disabled.")
        await log_action("Maintenance Disabled", f"Maintenance disabled by {ctx.author}")
    await log_action("Plan Purchased", f"{ctx.author} purchased {plan} as VPS #{vid}")


    @bot.command(name="rules")
    async def post_rules(ctx, channel: discord.TextChannel = None):
        """Owner-only: Post server rules to a channel (mention or ID)."""
        if ctx.author.id != OWNER_ID:
            return await ctx.send("Only the owner can use this command.")
        target = channel or ctx.channel
        embed = discord.Embed(title="📜 Server Rules", description="Please read and follow these rules:", color=get_embed_color("purple"))
        embed.add_field(name="1. Be Respectful", value="No harassment, hate, or discrimination. Treat others with respect.", inline=False)
        embed.add_field(name="2. No Illegal Activity", value="Do not request, provide, or discuss piracy or illegal services.", inline=False)
        embed.add_field(name="3. No Spamming or Advertising", value="No unsolicited ads, raids, or mass pings. Use approved channels for promotion.", inline=False)
        embed.add_field(name="4. Do Not Ping The Owner", value="Avoid pinging the owner directly; use staff or tickets for support.", inline=False)
        embed.add_field(name="5. Follow Discord TOS", value="All members must follow Discord Terms of Service and community guidelines.", inline=False)
        embed.set_footer(text=f"Vortex Nodes v{config.get('version','1.0.0')}")
        await target.send(embed=embed)
        await ctx.send(f"Rules posted to {target.mention}")
        await log_action("Rules Posted", f"{ctx.author} posted rules to {target}")


    @bot.command(name="information", aliases=["informatin"])
    async def post_information(ctx, channel: discord.TextChannel = None):
        """Owner-only: Post server information to a channel (mention or ID)."""
        if ctx.author.id != OWNER_ID:
            return await ctx.send("Only the owner can use this command.")
        target = channel or ctx.channel
        embed = discord.Embed(title="ℹ️ Server Information", description="Useful information about this server and how to get help.", color=get_embed_color("primary"))
        embed.add_field(name="Getting Started", value="Use `.plans`, `.freeplans`, or `.trial` to obtain a VPS. Use `.buywc <plan>` to purchase paid plans.", inline=False)
        embed.add_field(name="Support", value="If you need help, open a ticket or contact staff. Do not ping the owner.", inline=False)
        embed.add_field(name="Commands", value="Use `.help` to view bot commands. Staff-only commands require appropriate roles.", inline=False)
        embed.set_footer(text=f"Vortex Nodes v{config.get('version','1.0.0')}")
        await target.send(embed=embed)
        await ctx.send(f"Information posted to {target.mention}")
        await log_action("Information Posted", f"{ctx.author} posted server information to {target}")


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Missing token in config.json")
    bot.run(TOKEN)
