"""Throwaway macOS VMs (Tart, on Apple's `Virtualization.framework`) for the macOS runner hosts.

    python3 -m ci.jobs.scripts.macos_vm golden   build the provisioned image unless it exists
    python3 -m ci.jobs.scripts.macos_vm serve    keep SLOTS VMs, each registered for exactly one job

The host needs `/usr/bin/expect`; `tart` is installed on first use from its release. Jobs inside a VM
reach instance metadata through NAT, which needs `--http-put-response-hop-limit 2` on the instance.
"""

import hashlib
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

BASE_IMAGE = os.environ.get("MACOS_VM_BASE_IMAGE", "ghcr.io/cirruslabs/macos-tahoe-base:latest")
SLOTS = int(os.environ.get("MACOS_VM_SLOTS", "2"))
VM_CPU = int(os.environ.get("MACOS_VM_CPU", "6"))
VM_MEMORY_MIB = int(os.environ.get("MACOS_VM_MEMORY_MIB", "14336"))
GUEST_USER = "admin"
RUNNER_URL = "https://github.com/ClickHouse"

# Same tool set as `configure_darwin` in runner-init.py.
PROVISION = r"""
set -euxo pipefail
eval "$(/opt/homebrew/bin/brew shellenv)"
brew update
brew install ca-certificates curl gh jq pigz pv ripgrep zstd wget unzip gnu-sed grep bash coreutils llvm python@3 awscli rsync
brew cleanup --prune=all -s
"$(brew --prefix python@3)/bin/python3" -m venv ~/venv
~/venv/bin/pip install --upgrade pip
~/venv/bin/pip install --upgrade boto3 pygithub requests urllib3 unidiff dohq-artifactory pyjwt \
    numpy==2.3.2 pandas==2.3.3 Jinja2==3.1.6
VERSION=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest | jq -r '.tag_name | ltrimstr("v")')
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -fsSL "https://github.com/actions/runner/releases/download/v$VERSION/actions-runner-osx-arm64-$VERSION.tar.gz" | tar xz
sudo mdutil -a -i off || true
sudo tmutil disable || true
"""

# PATH for job steps inside the guest, as `Runner.macos_job_path` builds it on the host.
GUEST_PATH = ":".join(
    [
        f"/Users/{GUEST_USER}/venv/bin",
        "/opt/homebrew/opt/coreutils/libexec/gnubin",
        "/opt/homebrew/opt/gnu-sed/libexec/gnubin",
        "/opt/homebrew/opt/grep/libexec/gnubin",
        "/opt/homebrew/opt/llvm/bin",
        "/opt/homebrew/bin",
        "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    ]
)


def log(msg):
    print(f"[macos_vm {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def tart_home() -> Path:
    if "TART_HOME" in os.environ:
        home = Path(os.environ["TART_HOME"])
    else:
        # The instance-store SSD; the root volume is too small for images and per-job clones.
        mounts = [l.split()[-1] for l in subprocess.check_output(["df"], text=True).splitlines() if "tmp-mount" in l]
        assert mounts, "no instance-store SSD mounted; set TART_HOME"
        home = Path(mounts[0]) / "tart"
    if not home.is_dir():
        subprocess.run(["sudo", "mkdir", "-p", str(home)], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), str(home)], check=True)
    os.environ["TART_HOME"] = str(home)
    return home


def tart_bin() -> str:
    return str(tart_home() / "tart.app/Contents/MacOS/tart")


def tart(*args, capture=False, check=True):
    if capture:
        return subprocess.check_output([tart_bin(), *args], text=True).strip()
    subprocess.run([tart_bin(), *args], check=check)


def ensure_tart():
    # The release ships a signed `tart.app`; the Homebrew tap formula fails on current Homebrew.
    if not Path(tart_bin()).exists():
        log("installing tart")
        subprocess.run(
            f"curl -fsSL https://github.com/cirruslabs/tart/releases/latest/download/tart.tar.gz | tar xz -C {shlex.quote(str(tart_home()))}",
            shell=True,
            check=True,
        )
    log(f"tart {tart('--version', capture=True)}")


def vm_names():
    return [vm["Name"] for vm in json.loads(tart("list", "--source", "local", "--format", "json", capture=True))]


class VM:
    """A local Tart VM reachable over SSH with the host's key; deleted on context exit."""

    def __init__(self, name):
        self.name = name
        self.home = tart_home()
        self.key = self.home / "id_ed25519"
        self.proc = None

    @classmethod
    def clone(cls, source, name):
        tart("clone", source, name)
        return cls(name)

    def start(self):
        logf = open(self.home / f"{self.name}.log", "w")
        self.proc = subprocess.Popen([tart_bin(), "run", "--no-graphics", self.name], stdout=logf, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            assert self.proc.poll() is None, f"`tart run {self.name}` exited with {self.proc.returncode}:\n{self.run_log()}"
            for resolver in ("dhcp", "arp", "agent"):
                ip = subprocess.run([tart_bin(), "ip", "--resolver", resolver, self.name], capture_output=True, text=True)
                if ip.returncode == 0 and ip.stdout.strip():
                    self.ip = ip.stdout.strip()
                    log(f"{self.name} is up at {self.ip} (resolver {resolver})")
                    return self
            time.sleep(5)
        raise AssertionError(f"no IP for {self.name} after 300 s; last `tart ip`: {ip.stderr.strip()}\n{self.run_log()}")

    def run_log(self):
        return (self.home / f"{self.name}.log").read_text(errors="replace")[-4000:]

    def _ssh_args(self):
        return ["-i", str(self.key), "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR"]

    def ssh(self, script, check=True):
        return subprocess.run(
            ["ssh", *self._ssh_args(), f"{GUEST_USER}@{self.ip}", "bash -s"], input=script, text=True, check=check
        ).returncode

    def rsync(self, src, dst, to_guest=True, excludes=()):
        cmd = ["rsync", "-a", "--delete", "-e", shlex.join(["ssh", *self._ssh_args()])]
        cmd += [f"--exclude={e}" for e in excludes]
        cmd += [src, f"{GUEST_USER}@{self.ip}:{dst}"] if to_guest else [f"{GUEST_USER}@{self.ip}:{src}", dst]
        subprocess.run(cmd, check=True)

    def authorize_key(self):
        if not self.key.exists():
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.key)], check=True)
        # The Cirrus images ship with password `admin`; `expect` types it once for `ssh-copy-id`.
        script = (
            f"spawn ssh-copy-id -i {self.key}.pub -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f'{GUEST_USER}@{self.ip}\nexpect "assword:"\nsend "admin\\r"\nexpect eof\n'
        )
        subprocess.run(["/usr/bin/expect", "-c", script], check=True)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            tart("stop", self.name, check=False)
            self.proc.wait()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()
        tart("delete", self.name, check=False)
        (self.home / f"{self.name}.log").unlink(missing_ok=True)


def golden_name():
    digest = hashlib.sha256((BASE_IMAGE + PROVISION).encode()).hexdigest()[:8]
    return f"ch-golden-{digest}"


def ensure_golden():
    """Return the name of the provisioned image, building it when the tool set changed."""
    ensure_tart()
    tart_home()
    name = golden_name()
    names = vm_names()
    if name in names:
        return name
    for old in (n for n in names if n.startswith("ch-golden-")):
        tart("delete", old)
    log(f"building {name} from {BASE_IMAGE}")
    tart("clone", BASE_IMAGE, name)
    tart("set", name, "--cpu", str(VM_CPU), "--memory", str(VM_MEMORY_MIB), "--disk-size", "120")
    vm = VM(name).start()
    vm.authorize_key()
    vm.ssh(PROVISION)
    vm.ssh("sudo shutdown -h now", check=False)
    vm.proc.wait()
    log(f"{name} ready")
    return name


def imds(path):
    req = urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    token = urllib.request.urlopen(req, timeout=2).read().decode()
    req = urllib.request.Request(f"http://169.254.169.254/latest/meta-data/{path}", headers={"X-aws-ec2-metadata-token": token})
    return urllib.request.urlopen(req, timeout=2).read().decode()


def serve_slot(golden, slot, labels, instance):
    import boto3

    ssm = boto3.client("ssm", region_name="us-east-1")
    while True:
        token = ssm.get_parameter(Name="github_runner_registration_token", WithDecryption=True)["Parameter"]["Value"]
        with VM.clone(golden, f"job-{slot}-{int(time.time())}") as vm:
            vm.start()
            rc = vm.ssh(
                "cd ~/actions-runner && ./config.sh --unattended --ephemeral --replace "
                f"--url {RUNNER_URL} --token {shlex.quote(token)} --runnergroup Default "
                f"--labels {labels} --work _work --name {instance}-{slot} && ./run.sh",
                check=False,
            )
            log(f"slot {slot}: {vm.name} exited with {rc}")


def serve():
    import boto3

    golden = ensure_golden()
    for stale in (n for n in vm_names() if n.startswith("job-")):
        tart("delete", stale)
    instance = imds("instance-id")
    ec2 = boto3.client("ec2", region_name=imds("placement/region"))
    tags = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [instance]}, {"Name": "key", "Values": ["github:runner-type"]}])
    labels = f"self-hosted,darwin,{tags['Tags'][0]['Value']}"
    threads = [threading.Thread(target=serve_slot, args=(golden, s, labels, instance)) for s in range(1, SLOTS + 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    commands = {"golden": ensure_golden, "serve": serve}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print(__doc__)
        sys.exit(2)
    commands[sys.argv[1]]()
