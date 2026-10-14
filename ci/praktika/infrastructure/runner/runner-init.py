#!/usr/bin/env python3

from __future__ import annotations
import argparse
import json
import os
import sys
import time
import shutil
import tarfile
import shlex
import subprocess
import threading
from urllib import request
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass

import boto3


@dataclass
class Environment:
    """Environment type constants for runner initialization."""

    TEST: str = "test"
    PRODUCTION: str = "production"
    STAGING: str = "staging"
    MACOS: str = "macos"

    @classmethod
    def all(cls) -> List[str]:
        """Return all valid environment values."""
        return [cls.TEST, cls.PRODUCTION, cls.STAGING, cls.MACOS]


@dataclass
class RunnerConfig:
    """Configuration and runtime state for the GitHub Actions runner."""

    # Constants
    version: int = 74
    init_environment: str = Environment.TEST
    verbose = False
    script_path = os.path.abspath(__file__)

    # Runner defaults
    user: str = "ec2-user" if sys.platform == "darwin" else "ubuntu"
    runner_home: str = f"/Users/{user}/actions-runner" if sys.platform == "darwin" else f"/home/{user}/actions-runner"
    runner_org: str = "ClickHouse"
    runner_url: str = ""  # set in __post_init__
    max_total_errors: int = 5
    # TEMPORARY: set to 1 to pair with the max_chill disable above (avoids GH race);
    # style-checker runners override this to 10, which is fine as they are small instances.
    max_jobs: int = 1
    max_chill: int = 600
    max_life: int = 3600 * 24

    free_blocks_threshold: int = 3_000_000
    free_blocks_threshold_percent: int = 5

    RUNNER_VERSION_LABEL = "runner-init version"

    def __post_init__(self):
        self.runner_url = f"https://github.com/{self.runner_org}"
        self.log_dir = Path(self.runner_home) / "_diag"


config = RunnerConfig()


class EC2:
    region_name: str = "us-east-1"
    _session = None
    _ssm_client = boto3.session.Session(region_name=region_name).client("ssm")
    _s3_client = None
    _as_client = None
    _ec2_client = None
    _resource = None

    @classmethod
    def init_clients(cls, region: str = None) -> None:
        """Initialize (or reinitialize) AWS clients for the given region."""
        if region is None:
            region = cls.region_name
        cls._session = boto3.session.Session(region_name=region)
        cls._s3_client = cls._session.client("s3", region_name=region)
        cls._as_client = cls._session.client("autoscaling", region_name=region)
        cls._ec2_client = cls._session.client("ec2", region_name=region)
        cls._resource = boto3.resource("ec2", region_name=region)

    def __init__(self, instance_id: str = None):
        if instance_id is None:
            self._instance_id = EC2.get_instance_metadata("instance-id")
        else:
            self._instance_id: str = instance_id
        log(f"Instance ID: {self._instance_id}")

    @classmethod
    def autodetect_region(cls) -> str:
        """Detect the current AWS region from instance metadata, falling back to the default."""
        try:
            cls.region_name = EC2.get_instance_metadata("placement/region")
        except Exception:
            pass
        return cls.region_name

    @staticmethod
    def get_instance_metadata(path: str) -> str:
        url = f"http://169.254.169.254/latest/meta-data/{path}"
        if config.verbose:
            log("Fetching instance metadata: " + path)
        with request.urlopen(url, timeout=2) as resp:
            res = resp.read().decode().strip()
        if config.verbose:
            log(f" {path} = {res}")
        return res

    def get_instance(self) -> boto3.resources.factory.ec2.Instance:
        return self._resource.Instance(self._instance_id)

    @staticmethod
    def ssm_get_parameter(name: str, with_decryption: bool = True) -> str:
        """Get a single parameter from AWS Systems Manager Parameter Store."""
        resp = EC2._ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]

    def terminate(self) -> None:
        """Terminate an EC2 instance."""
        self._ec2_client.terminate_instances(InstanceIds=[self._instance_id])
        log(f"Requested ec2 terminate_instances for {self._instance_id}")

    def scale_down(self) -> None:
        """Terminate an EC2 instance and scale down."""
        self._as_client.terminate_instance_in_auto_scaling_group(
            InstanceId=self._instance_id, ShouldDecrementDesiredCapacity=True
        )
        log(
            f"Requested ec2 terminate_instance_in_auto_scaling_group for {self._instance_id}"
        )

    @staticmethod
    def upload_init_script() -> None:
        log(
            f"Uploading {config.script_path} to s3://github-runners-data/cloud-init/{config.init_environment}.py"
        )
        EC2._s3_client.upload_file(
            Filename=config.script_path,
            Bucket="github-runners-data",
            Key=f"cloud-init/{config.init_environment}.py",
        )

    @staticmethod
    def get_remote_init_version() -> int:
        VERSION_LINE_PREFIX = "    version: int = "

        obj = EC2._s3_client.get_object(
            Bucket="github-runners-data",
            Key=f"cloud-init/{config.init_environment}.py",
        )
        new_script = obj["Body"].read().decode("utf-8-sig")

        for line in new_script.splitlines():
            if line.startswith(VERSION_LINE_PREFIX):
                return int(line.split(VERSION_LINE_PREFIX, 1)[1])

        raise Exception("Version line not found in a remote copy of the init script")


EC2.autodetect_region()


class Runner:
    PROVISION_MARKER = Path.home() / ".clickhouse-ci-runner-init-version"

    def __init__(self, ec2: EC2):
        self.ec2 = ec2
        self._token = None
        self.total_errors: int = 0
        self.runner_start_time = int(time.time())
        self.completed_jobs: int = 0

        self.instance_type = EC2.get_instance_metadata("instance-type")
        log(f"instance-type: {self.instance_type}")

        self.runner_type = None
        self.asg_name = None

        instance = ec2.get_instance()
        for p in instance.tags:
            name = p.get("Key")
            value = p.get("Value")
            if name == "aws:autoscaling:groupName":
                self.asg_name = value
                log(f"aws:autoscaling:groupName: {self.asg_name}")
            elif name == "github:runner-type":
                self.runner_type = value
                log(f"github:runner-type: {self.runner_type}")

        if self.runner_type is None:
            raise RuntimeError("github:runner-type tag is not set for the instance")
        self.labels = f"self-hosted,{sys.platform},{self.runner_type}"

        if "style" in self.runner_type:
            config.max_chill = 7200
            config.max_jobs = 10

        log(f"max jobs: {config.max_jobs}")
        log(f"max chill: {config.max_chill}")
        log(f"labels: {self.labels}")

    def _get_token(self) -> str:
        """Get registration token from SSM Parameter Store."""
        self._token = self.ec2.ssm_get_parameter("github_runner_registration_token")
        return self._token

    def _check_conditions(self) -> None:
        """Check various conditions that might trigger termination."""

        # instances live only for two hours, does not make sense to check for script updates

        if self.total_errors > config.max_total_errors:
            self.collect_logs("configure")
            raise Exception(f"Too many errors ({self.total_errors})")

        # macOS runners run continuously without lifetime limits
        if config.init_environment == Environment.MACOS:
            return

        runner_age = int(time.time()) - self.runner_start_time
        if config.max_life < runner_age:
            raise Exception(f"Runner lifetime exceeded: {runner_age}")

    def _cleanup_workspace(self) -> None:
        """Remove _work directory to clean up workspace."""
        work_dir = Path(config.runner_home) / "_work"
        if work_dir.exists():
            shutil.rmtree(work_dir)
        shutil.rmtree(config.log_dir, ignore_errors=True)

    def remove_if_not_running(self) -> bool:
        """Unregister the runner from GitHub.
        Returns False if running a job and cannot be removed.
        Returns True if successfully removed or other errors."""
        os.chdir(config.runner_home)

        token = self._get_token()
        if not Path(config.runner_home, ".runner").exists():
            return True

        log("Try unregistering runner from GitHub")
        cmd = ["sudo", "-u", config.user, "./config.sh", "remove", "--token", token]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            log(result.stderr)
            self.collect_logs("unregistration")
            if LogParser.RUNNER_ERR_RUNNING in result.stderr:
                return False
            self.total_errors += 1

        return True

    def config(self) -> None:
        """Register the runner with GitHub."""
        self.remove_if_not_running()

        # Build configuration arguments
        config_args = [
            "--token",
            self._token,  # cached in remove()
            "--url",
            config.runner_url,
            "--ephemeral",
            "--unattended",
            "--replace",
            "--runnergroup",
            "Default",
            "--labels",
            self.labels,
            "--work",
            "_work",
            "--name",
            self.ec2._instance_id,
        ]

        cmd = ["sudo", "-u", config.user, "./config.sh"] + config_args
        # macOS runners need autoupdate to stay current; only disable it on Linux
        if config.init_environment != Environment.MACOS:
            cmd += ["--disableupdate"]
        try:
            run(cmd, check=True)
        except Exception as e:
            self.total_errors += 1
            log(f"Failed to register runner: {e}")
            self.collect_logs("registration")
            # Intentionally fall through without registering: `run.sh` exits
            # fast without a `.runner` config, `run_job` counts that as an
            # error, and the loop terminates once `max_total_errors` is hit.
            return
        log("Runner registered successfully")

    def run(self) -> None:
        """Main runner loop."""
        if config.init_environment == Environment.MACOS:
            Runner.configure_darwin()
        else:
            Runner.configure_linux()

        while True:
            try:
                self._check_conditions()

                self.config()
                self.run_job()
            except Exception:
                self.remove_if_not_running()
                raise

            log(f"Completed jobs: {self.completed_jobs}/{config.max_jobs}")
            # macOS runners run continuously without job limits
            if config.init_environment != Environment.MACOS and self.completed_jobs >= config.max_jobs:
                raise Exception("Runner completed max number of jobs")

    def run_job(self) -> None:
        """Execute one iteration of the runner loop."""
        os.chdir(config.runner_home)
        self._cleanup_workspace()

        result = subprocess.run(["df", "-h"], capture_output=True, text=True)
        log(f"Disk space before job:\n{result.stdout.strip()}")

        # Run the runner with hooks
        log("Starting runner execution")
        run_cmd = ["sudo", "-u", config.user, "./run.sh"]
        if config.init_environment == Environment.MACOS:
            # Job steps run as `bash --noprofile --norc` and inherit the listener
            # PATH, so give `run.sh` the PATH jobs need — `~/venv/bin` (the pip
            # deps installed in `configure_macos`) and the Homebrew GNU/llvm tool
            # dirs. `env` after `sudo` overrides sudo's `secure_path`.
            run_cmd = [
                "sudo", "-u", config.user,
                "env", f"PATH={Runner.macos_job_path()}",
                "./run.sh",
            ]
        try:
            run(run_cmd, check=True)
            self.completed_jobs += 1
        except Exception as e:
            self.total_errors += 1
            log(f"Runner execution failed: {e}")
            self.collect_logs("execution")

        LogParser.parse_worker_logs()

        error = LogParser.get_runner_error([LogParser.RUNNER_RESULT_ABANDONED])
        if error:
            self.total_errors += 1
            log(f"Runner execution failed: {error}")
            self.collect_logs("log_error")

        try:
            Monitor.check_termination_flag()
        except Exception:
            self.ec2.scale_down()
            raise
        Runner.check_post_run()
        Monitor.check_are_both_alive()

    def collect_logs(self, error_id: str) -> int:
        """
        Create tar.gz from runner _diag directory and upload to S3.
        Returns 0 on success, non-zero on failures.
        """
        log(f"Failure detected [{error_id}], collecting logs...")

        ts = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        archive_path = Path(f"/tmp/runner-logs-{ts}.tar.gz")
        s3_bucket = "clickhouse-test-reports-private"
        s3_key = f"gh_actions/{error_id}_logs_{self.ec2._instance_id}_{ts}.tar.gz"

        if not config.log_dir.exists():
            log(f"Log directory {config.log_dir} does not exist")
            return

        try:
            with tarfile.open(archive_path, "w:gz") as tf:
                tf.add(str(config.log_dir), arcname=".")
            log(f"Created archive {archive_path}")
        except Exception as e:
            log(f"Failed to create log archive: {e}")
            return

        try:
            self.ec2._s3_client.upload_file(
                Filename=archive_path,
                Bucket=s3_bucket,
                Key=s3_key,
            )
            log(f"Logs successfully uploaded to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            log(f"Failed to upload logs to S3: {e}")
            return

    @staticmethod
    def check_post_run() -> None:
        if config.init_environment == Environment.MACOS:
            return
        result = subprocess.run(["df", "/"], capture_output=True, text=True, check=True)
        if config.verbose:
            log(f"df / output:\n{result.stdout}", "post-run")
        last = result.stdout.splitlines()[-1].split()

        free_blocks = int(last[3])
        free_percent = int(last[3]) * 100 // int(last[1])

        if free_blocks < config.free_blocks_threshold:
            raise RuntimeError(f"Out of disk space: {free_blocks} blocks on rootfs")
        if free_percent < config.free_blocks_threshold_percent:
            raise RuntimeError(
                f"Out of disk space: {free_percent}% of free space on rootfs"
            )

        run_bash(
            """
docker ps --quiet | xargs --no-run-if-empty docker kill
docker ps --all --quiet | xargs --no-run-if-empty docker rm -f
sudo systemctl restart docker
"""
        )

    @staticmethod
    def configure_linux() -> None:
        run_bash(
            """
sudo sysctl --system # apply os image settings

# Map both the FQDN and the short name to loopback. `gethostname(2)` returns
# the short form, and without a short-name entry resolution depends on
# `search ec2.internal` expansion in `/etc/resolv.conf` to match the FQDN
# line - a fragile path that breaks the `Fast test` job (`clickhouse-client`
# logs a `DNSResolver::Impl::Impl` stack trace at startup, tripping the
# stderr-must-be-empty check).
fqdn=$(hostname --fqdn)
short=$(hostname --short)
echo "Current FQDN: $fqdn (short: $short)"
echo "127.0.0.1 $fqdn $short" | sudo tee -a /etc/hosts
resolvectl flush-caches
resolvectl query "$fqdn"
# Sometimes it tries to resolve the address using NSes:
#
#     ip-172-31-33-98.ec2.internal has address 127.0.0.1
#     Host ip-172-31-33-98.ec2.internal not found: 3(NXDOMAIN)
#
# So let's ignore the error
host "$fqdn" || true

# Wait for 2 min to start Tailscale
timeout --kill-after=150 120 bash /usr/local/share/scripts/init-network.sh || \
    echo "Network initialization failed"

# Refresh teams ssh keys
chown ubuntu: /home/ubuntu/.ssh -R

sudo systemctl enable amazon-cloudwatch-agent
sudo systemctl start amazon-cloudwatch-agent

timedatectl status | grep 'NTP service: active'
"""
        )

    @staticmethod
    def macos_job_path() -> str:
        # PATH for macOS job steps: the `~/venv` installed in `configure_macos`
        # and the Homebrew GNU/llvm tool dirs (llvm so `lldb` resolves to the
        # Homebrew build — `brew install llvm` does not link `lldb` system-wide),
        # ahead of the inherited PATH. Deduped to keep it tidy.
        brew = "/opt/homebrew" if os.uname().machine == "arm64" else "/usr/local"
        home = f"/Users/{config.user}"
        candidates = [
            f"{home}/venv/bin",
            f"{brew}/opt/coreutils/libexec/gnubin",
            f"{brew}/opt/gnu-sed/libexec/gnubin",
            f"{brew}/opt/grep/libexec/gnubin",
            f"{brew}/opt/llvm/bin",
            f"{brew}/bin",
            *os.environ.get("PATH", "").split(":"),
        ]
        seen = set()
        result = []
        for entry in candidates:
            if entry and entry not in seen:
                seen.add(entry)
                result.append(entry)
        return ":".join(result)

    @staticmethod
    def configure_darwin() -> None:
        # macOS EC2 instances cannot have their user_data updated after launch,
        # so the user_data script is kept minimal and all provisioning lives here.
        # The install is gated on `config.version`: when it differs from the
        # marker file written on the previous successful run, the install is
        # re-executed. The install wipes `$RUNNER_HOME`, which removes the
        # `.runner` registration file and forces `Runner.config` to re-run
        # `config.sh` against the freshly extracted actions-runner.

        current = str(config.version)
        try:
            previous = Runner.PROVISION_MARKER.read_text().strip()
        except FileNotFoundError:
            previous = ""
        if previous == current:
            log(f"macOS provisioning already at version {current}, skipping")
            return

        log(f"macOS provisioning version change: '{previous}' -> '{current}'")

        # Fetch and rewrite the CloudWatch agent config in Python rather than
        # piping through `jq` from bash — on a freshly bootstrapped macOS host
        # `jq` is not yet installed (it lands later in the user-mode `brew
        # install` block), so the privileged block cannot depend on it.
        ssm = boto3.client("ssm", region_name="us-east-1")
        cw_config = json.loads(
            ssm.get_parameter(Name="AmazonCloudWatch-github-runners")["Parameter"]["Value"]
        )

        def _retarget_macos_log_path(node):
            if isinstance(node, dict):
                if node.get("file_path") == "/var/log/cloud-init-output.log":
                    node["file_path"] = "/var/log/amazon/ec2/ec2-macos-init.log"
                for v in node.values():
                    _retarget_macos_log_path(v)
            elif isinstance(node, list):
                for v in node:
                    _retarget_macos_log_path(v)

        _retarget_macos_log_path(cw_config)
        Path("/tmp/amazon-cloudwatch-agent.json").write_text(json.dumps(cw_config))

        # Privileged steps (disable bluetooth, install CloudWatch agent into
        # `/opt/aws/`, drop the SSM-fetched config under `/opt/aws/...`) run
        # as root. CloudWatch is brought up first so it captures the brew
        # install spam that follows.
        run_bash(
            r"""
# Disable greedy system service
launchctl disable system/com.apple.bluetoothd || true

# Disable Spotlight indexing — CI runners never search interactively, and
# indexing the checkout/build tree wastes CPU/IO and ~3 GB on the index store.
# `mdutil -i off` stops indexing; removing `.Spotlight-V100` reclaims the space
# (on recent macOS `mdutil` alone does not reliably free it).
mdutil -a -i off || true
rm -rf /.Spotlight-V100 || true

# CloudWatch agent
case $(uname -m) in
    x86_64) CLOUDWATCH_ARCH=amd64 ;;
    arm64)  CLOUDWATCH_ARCH=arm64 ;;
esac
curl -fsSL -o /tmp/amazon-cloudwatch-agent.pkg \
    "https://s3.amazonaws.com/amazoncloudwatch-agent/darwin/${CLOUDWATCH_ARCH}/latest/amazon-cloudwatch-agent.pkg"
installer -pkg /tmp/amazon-cloudwatch-agent.pkg -target /

cp /tmp/amazon-cloudwatch-agent.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config \
    -m ec2 \
    -s \
    -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
""",
            sudo=True,
            verbose=True,
        )

        # User-mode steps (Homebrew, Python venv, GitHub Actions runner) refuse
        # to run as root, so this block runs as the current user (ec2-user).
        run_bash(
            rf"""
# Homebrew is bootstrapped by `user_data_macos.txt`; just make it callable in
# this non-interactive shell (the `brew shellenv` line lives in `.zprofile`,
# only sourced by a login zsh, but we run under `bash -euxo pipefail`). If brew
# is somehow missing, `brew update` below fails loudly and the instance retries.
if [ -x /opt/homebrew/bin/brew ]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
elif [ -x /usr/local/bin/brew ]; then
    eval "$(/usr/local/bin/brew shellenv)"
fi

brew update

# Essential tools. `llvm` is included for `lldb`, used to print C stack traces
# of ClickHouse server processes from `clickhouse-test` cleanup paths.
# `python@3` is bootstrapped by `user_data_macos.txt` (along with the
# `~/venv` and `boto3` that runner-init itself runs inside), so it is not
# re-installed here.
brew install \
    ca-certificates \
    curl \
    gh \
    jq \
    pigz \
    pv \
    ripgrep \
    zstd \
    wget \
    unzip \
    gnu-sed \
    grep \
    bash \
    coreutils \
    llvm

# Python packages used by jobs. `boto3` is bootstrapped in `user_data_macos.txt`
# because runner-init itself imports it; the rest are version-gated here.
"$HOME/venv/bin/pip" install --upgrade pip
"$HOME/venv/bin/pip" install --upgrade \
    pygithub \
    requests \
    urllib3 \
    unidiff \
    dohq-artifactory \
    pyjwt \
    numpy==2.3.2 \
    pandas==2.3.3 \
    scipy==1.16.1 \
    Jinja2==3.1.6

# GitHub Actions runner: wipe and re-extract so the next iteration of the
# runner loop registers with a fresh `config.sh`.
case $(uname -m) in
    x86_64) RUNNER_ARCH=x64 ;;
    arm64)  RUNNER_ARCH=arm64 ;;
esac
RUNNER_VERSION=$(curl -fsSL "https://api.github.com/repos/actions/runner/releases/latest" | jq -r '.tag_name | ltrimstr("v")')
rm -rf "{config.runner_home}"
mkdir -p "{config.runner_home}"
cd "{config.runner_home}"
RUNNER_ARCHIVE="actions-runner-osx-${{RUNNER_ARCH}}-${{RUNNER_VERSION}}.tar.gz"
curl -O -L "https://github.com/actions/runner/releases/download/v${{RUNNER_VERSION}}/${{RUNNER_ARCHIVE}}"
tar xzf "./${{RUNNER_ARCHIVE}}"
rm -f "./${{RUNNER_ARCHIVE}}"
""",
            verbose=True,
        )
        Runner.PROVISION_MARKER.write_text(current)
        log(f"macOS provisioning completed, marker updated to {current}")


class LogParser:
    RUNNER_ERR_RUNNING = "is currently running a job and cannot be deleted"
    RUNNER_RESULT_ABANDONED = "with result: Abandoned"
    WORKER_INFO_JOB_MESSAGE = "Worker] Job message:\n"
    WORKER_INFO_JOB_ID = "INFO JobRunner] Job ID"

    @staticmethod
    def get_latest_log(p: Path, glob: str) -> Optional[str]:
        files = list(p.glob(glob))
        if not files:
            return None

        latest = max(files, key=lambda f: f.stat().st_mtime)
        return str(latest)

    @staticmethod
    def get_runner_error(pattern: list[str]) -> Optional[str]:
        runner_log = LogParser.get_latest_log(
            Path(config.log_dir), "Runner_*.log"
        )
        if not runner_log:
            log("No runner log files found to parse")
            return None

        log(f"Checking log file: {runner_log}")
        with open(runner_log, "r", encoding="utf-8") as f:
            for line in f:
                for p in pattern:
                    if p in line:
                        log(f"Error: {runner_log}: {line}")
                        return line.strip()
        return None

    @staticmethod
    def get_job_url(message: dict) -> str:
        gh = message["contextData"]["github"]["d"]
        ghh = {e["k"]: e["v"] for e in gh}

        jb = message["contextData"]["job"]["d"]
        jbh = {e["k"]: e["v"] for e in jb}
        return "/".join(
            [
                ghh["server_url"],
                ghh["repository"],
                "actions/runs",
                ghh["run_id"],
                "job",
                str(int(jbh["check_run_id"])),
            ]
        )

    @staticmethod
    def parse_worker_logs() -> Optional[str]:
        worker_log = LogParser.get_latest_log(
            Path(config.log_dir), "Worker_*.log"
        )
        if not worker_log:
            log("No worker log files found to parse")
            return None

        log(f"Parsing log file: {worker_log}")
        message = None
        with open(worker_log, "r", encoding="utf-8") as f:
            for line in f:
                if message is not None:
                    if LogParser.WORKER_INFO_JOB_ID in line:
                        message = json.loads("".join(message))
                        log(f"Job url: {LogParser.get_job_url(message)}")
                        return message
                    message.append(line)
                    continue

                if line.endswith(LogParser.WORKER_INFO_JOB_MESSAGE):
                    message = []
                    continue

        return None


class Monitor:
    TERMINATION_REASON = "/tmp/termination-reason"
    # macOS: thread-based monitor
    _monitor_thread: Optional[threading.Thread] = None
    # Linux: fork-based monitor
    _runner_pid: Optional[int] = None
    _monitor_pid: Optional[int] = None

    def __init__(self, runner: Runner):
        self.runner = runner
        Monitor.clear_termination_flag()

    @staticmethod
    def write_termination_flag(reason: str, domain: str = "monitor") -> None:
        log(f"Writing termination reason: {reason}", domain)
        with open(Monitor.TERMINATION_REASON, "w") as f:
            f.write(reason)

    @staticmethod
    def clear_termination_flag() -> None:
        Path(Monitor.TERMINATION_REASON).unlink(missing_ok=True)

    @staticmethod
    def check_termination_flag() -> None:
        """Check for the presence of a termination flag file."""
        res = Path(Monitor.TERMINATION_REASON).exists()
        if res:
            with open(Monitor.TERMINATION_REASON, "r") as f:
                reason = f.read().strip()
                raise Exception(f"Termination reason: {reason}")

    @staticmethod
    def is_job_assigned() -> bool:
        # Brackets needed not to match pgrep itself
        p = subprocess.run(
            ["pgrep", "-f", "[R]unner.Worker"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return p.returncode == 0

    @staticmethod
    def log(msg: str) -> None:
        log(msg, "monitor")

    @staticmethod
    def check_are_both_alive() -> None:
        if sys.platform == "darwin":
            thread = Monitor._monitor_thread
            if thread is None:
                raise RuntimeError("Monitor thread was never started")
            if not thread.is_alive():
                raise RuntimeError("Monitor thread is not alive")
        else:
            # Each side checks the other: runner checks monitor_pid, monitor checks runner_pid
            current_pid = os.getpid()
            pid_to_check = (
                Monitor._monitor_pid
                if current_pid == Monitor._runner_pid
                else Monitor._runner_pid
            )
            if pid_to_check is None:
                raise RuntimeError("Peer process PID is not set")
            try:
                os.kill(pid_to_check, 0)
            except ProcessLookupError as e:
                raise RuntimeError("Peer process is not alive") from e
            except PermissionError:
                pass

    def run(self) -> None:
        start_time = int(time.time())
        last_print_time = start_time
        last_job_time = start_time
        runner_state = "starting"
        last_job_start_time = start_time
        Monitor.log("started")

        while True:
            time.sleep(20)
            Monitor.check_are_both_alive()

            current_time = int(time.time())
            if Monitor.is_job_assigned():
                if runner_state != "working":
                    Monitor.log("runner got a job")
                    last_job_start_time = current_time
                runner_state = "working"
                last_job_time = current_time
                if current_time - last_print_time >= 100:
                    Monitor.log(
                        f"the runner working for {current_time - last_job_start_time} seconds"
                    )
                    last_print_time = current_time
            else:
                runner_state = "idling"
                runner_idle_time = current_time - last_job_time
                if config.init_environment == Environment.MACOS:
                    last_job_time = current_time
                    continue
                if current_time - last_print_time >= 100:
                    Monitor.log(f"the runner idling for {runner_idle_time} seconds")
                    last_print_time = current_time
                # TEMPORARY: disabled max_chill idle-termination to avoid a race with GH;
                # the runner now terminates only after completing max_jobs.
                # if runner_idle_time > config.max_chill:
                #     Monitor.write_termination_flag(
                #         f"Runner idle for too long: {runner_idle_time} seconds"
                #     )
                #
                #     if not self.runner.remove_if_not_running():
                #         Monitor.log("Runner is busy, will not terminate")
                #         Monitor.clear_termination_flag()
                #         last_job_time = current_time
                #         continue
                #
                #     run(["pkill", "Runner.Listener"], check=False)
                #
                #     time.sleep(10)
                #     # normally should scale down from the main runner loop
                #     raise RuntimeError("Runner idle for too long, terminating")

    def daemonize(self) -> None:
        if sys.platform == "darwin":
            # os.fork() is unsafe with threads on macOS/Python 3.14+
            thread = threading.Thread(target=self.run, daemon=True, name="monitor")
            thread.start()
            Monitor._monitor_thread = thread
        else:
            Monitor._runner_pid = os.getpid()
            pid = os.fork()
            if pid > 0:
                Monitor._monitor_pid = pid
                return
            os.setsid()
            self.run()


#
# helpers
#
def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Actions Runner initialization and management script"
    )

    parser.add_argument(
        "--environment",
        type=str,
        choices=Environment.all(),
        help=f"Environment name. Valid options: {', '.join(Environment.all())}. Default: {Environment.TEST}",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument("--version", action="store_true", help="Show version and exit")

    parser.add_argument(
        "--remote-version",
        action="store_true",
        help="Show version of the script in s3 and exit",
    )

    parser.add_argument(
        "--deploy", action="store_true", help="Deploy the script to s3 and exit"
    )

    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help=f"AWS region for S3/EC2 operations. Default: {EC2.region_name}",
    )

    args = parser.parse_args()

    if args.environment:
        config.init_environment = args.environment

    if args.verbose:
        config.verbose = True

    if args.region:
        EC2.region_name = args.region

    return args


def now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(msg: str, domain: str = None) -> None:
    domain = domain + " " if domain else ""
    print(f"[{domain}{now_ts()}] {msg}", flush=True)


def run(
    cmd: List[str], check: bool = True, env: Optional[dict] = None
) -> None:
    # Redact secret values before echoing: the GitHub runner registration token
    # is passed to `config.sh` as `--token <value>`, and this process's stdout is
    # tailed by the CloudWatch agent, so an unredacted echo would land the token
    # in CloudWatch in plaintext.
    SECRET_FLAGS = {"--token", "--password", "--pat"}
    redacted = list(cmd)
    for i in range(len(redacted) - 1):
        if redacted[i] in SECRET_FLAGS:
            redacted[i + 1] = "***"
    safe_cmd = " ".join(shlex.quote(sh) for sh in redacted)
    log(f"+ {safe_cmd}")
    # Handle `check` ourselves rather than passing it to `subprocess.run`: a
    # raised `CalledProcessError` stringifies the original `cmd`, so callers
    # logging the exception (e.g. `Runner.config`) would leak the `--token`
    # value into CloudWatch. Raise with the redacted argv instead.
    result = subprocess.run(
        cmd, check=False, stdout=sys.stdout, stderr=sys.stderr, env=env
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, redacted)


def run_bash(cmd: str, sudo: bool = False, verbose: bool = False) -> None:
    # Default is fail-only logging: stdout/stderr are captured into pipes and
    # only emitted on a non-zero exit. Successful output is dropped because
    # `bash -x` traces every command substitution, and several Linux scripts
    # (e.g. `setup_tailscale` in `init-network.sh`) expand SSM-fetched
    # secrets — leaving them in plaintext in stderr would land them in
    # CloudWatch. `verbose=True` opts back into emitting on success, used by
    # `configure_macos` so the brew/install progress reaches the CloudWatch
    # agent (which tails the runner-init process output).
    argv = ["bash", "-euxo", "pipefail"]
    if sudo:
        argv = ["sudo"] + argv
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = proc.communicate(cmd)
    if proc.returncode != 0:
        log(f"\nstdout:\n {stdout}\nstderr:\n {stderr}")
        raise RuntimeError("bash script failed")
    if verbose:
        log(f"\nstdout:\n {stdout}\nstderr:\n {stderr}")


if __name__ == "__main__":
    args = parse_args()
    EC2.init_clients()

    if args.version:
        print(config.version)
        sys.exit(0)

    if args.remote_version:
        print(EC2.get_remote_init_version())
        sys.exit(0)

    if args.deploy:
        if config.init_environment != Environment.TEST:
            try:
                rv = EC2.get_remote_init_version()
            except Exception as e:
                log(f"Cannot get remote version: {e}, aborting deploy")
                sys.exit(1)
            if config.version <= rv:
                log(
                    f"The local script version {config.version} <= remote version {rv}, aborting deploy"
                )
                sys.exit(1)
        EC2.upload_init_script()
        sys.exit(0)

    try:
        ec2 = EC2()
    except Exception as e:
        log(f"Got exception {e}, shutting down")
        run(["sudo", "shutdown", "-h", "now"])
        raise

    log(f"{config.RUNNER_VERSION_LABEL} {config.init_environment}.{config.version}")
    try:
        runner = Runner(ec2)
        monitor = Monitor(runner)
        monitor.daemonize()
        runner.run()

    except Exception:
        if config.init_environment == Environment.MACOS:
            # Drop the provisioning marker so the next boot re-runs
            # `configure_darwin` (re-extracts `actions-runner`) instead of
            # skipping it on the version gate and failing the same way again.
            Runner.PROVISION_MARKER.unlink(missing_ok=True)
            log(f"Removed provisioning marker {Runner.PROVISION_MARKER}")
        else:
            ec2.terminate()
        raise
