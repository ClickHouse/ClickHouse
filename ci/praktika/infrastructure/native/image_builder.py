import base64
import json
from typing import Any, Dict, List, Optional

from ..image_builder import ImageBuilder


def _write_file_from_base64(path: str, content: str) -> str:
    payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return f"printf '%s' '{payload}' | base64 -d > {path}"


def _setup_component(name: str, *, with_docker: bool):
    commands = [
        # AL2023 ships curl-minimal, but Git-related dependencies can require
        # full curl. Allow dnf to replace the minimal package so Python does
        # not get skipped by a curl/curl-minimal solver conflict.
        "dnf install -y --allowerasing python3.12 python3.12-pip git jq unzip",
        "cd /tmp && curl -fsSL \"https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip\" -o awscliv2.zip && unzip -q awscliv2.zip && ./aws/install && rm -rf awscliv2.zip aws",
        "dnf install -y amazon-cloudwatch-agent",
        "ln -sf /usr/bin/python3.12 /usr/local/bin/python3",
        "curl -fsSL https://cli.github.com/packages/rpm/gh-cli.repo -o /etc/yum.repos.d/gh-cli.repo",
        "dnf install -y gh",
    ]
    if with_docker:
        commands.extend(
            [
                "dnf install -y docker",
                "mkdir -p /etc/docker",
                "printf '%s\n' '{' '  \"log-driver\": \"json-file\",' '  \"log-opts\": {' '    \"max-file\": \"5\",' '    \"max-size\": \"1000m\"' '  }' '}' > /etc/docker/daemon.json",
                "usermod -aG docker ec2-user || true",
                "systemctl enable docker || true",
            ]
        )
    return {
        "name": name,
        "platform": "Linux",
        "description": (
            "Install AL2023 system packages, GitHub CLI, and Docker"
            if with_docker
            else "Install AL2023 system packages and GitHub CLI"
        ),
        "commands": commands,
    }


def _ubuntu_setup_component(name: str, *, with_docker: bool):
    commands = [
        "export DEBIAN_FRONTEND=noninteractive",
        # Stop stock Ubuntu background apt/snap upgrades from holding the dpkg
        # lock -- both during this build (racing our own apt-get) and, once the
        # AMI is baked, at runner boot (racing the agent's first heartbeat).
        # Disabling the timers is not enough: a timer may have already activated
        # apt-daily{,-upgrade}.service, which are separate units still holding
        # the lock, so stop those too. Give every apt command a lock timeout
        # (via a global config so all invocations below inherit it) to ride out
        # any residual run rather than failing instantly on a held lock.
        "echo 'DPkg::Lock::Timeout \"120\";' > /etc/apt/apt.conf.d/99praktika-lock-timeout",
        "systemctl disable --now apt-daily.timer apt-daily-upgrade.timer unattended-upgrades.service || true",
        "systemctl stop apt-daily.service apt-daily-upgrade.service || true",
        "DEBIAN_FRONTEND=noninteractive apt-get purge -y unattended-upgrades || true",
        "apt-get update",
        "DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends apt-transport-https at atop binfmt-support build-essential ca-certificates curl git gnupg jq lsb-release moreutils pigz python3-dev python3-pip python3.12 python3.12-venv qemu-user-static ripgrep unzip wget zstd",
        "ln -sf /usr/bin/python3.12 /usr/local/bin/python3",
        "mkdir -p -m 755 /etc/apt/keyrings /etc/apt/sources.list.d",
        "out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg && cat $out > /etc/apt/keyrings/githubcli-archive-keyring.gpg && rm -f $out",
        "chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg",
        "echo \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main\" > /etc/apt/sources.list.d/github-cli.list",
        "apt-get update",
        "DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends gh",
        "cd /tmp && curl -fsSL \"https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip\" -o awscliv2.zip && unzip -q awscliv2.zip && ./aws/install && rm -rf awscliv2.zip aws",
        "deb_arch=$(dpkg --print-architecture); wget --directory-prefix=/tmp \"https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/${deb_arch}/latest/amazon-cloudwatch-agent.deb\" \"https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/${deb_arch}/latest/amazon-cloudwatch-agent.deb.sig\"",
        "gpg --recv-key --keyserver keyserver.ubuntu.com D58167303B789C72",
        "gpg --verify /tmp/amazon-cloudwatch-agent.deb.sig",
        "dpkg -i /tmp/amazon-cloudwatch-agent.deb || DEBIAN_FRONTEND=noninteractive apt-get install -f --yes --no-install-recommends",
        "rm -f /tmp/amazon-cloudwatch-agent.deb /tmp/amazon-cloudwatch-agent.deb.sig",
        "systemctl enable amazon-cloudwatch-agent.service || true",
        "echo 'vm.max_map_count = 2097152' > /etc/sysctl.d/01-increase-map-counts.conf",
        "echo 'vm.mmap_rnd_bits=28' > /etc/sysctl.d/02-vm-mmap_rnd_bits.conf",
        "echo 'kernel.yama.ptrace_scope=0' > /etc/sysctl.d/10-ptrace.conf",
        "echo 'kernel.dmesg_restrict = 0' > /etc/sysctl.d/10-dmesg.conf",
        "echo 'kernel.core_pattern = core.%e.%p-%P' > /etc/sysctl.d/99-core-dumps.conf",
        "echo 'fs.suid_dumpable = 1' >> /etc/sysctl.d/99-core-dumps.conf",
        "echo 'kernel.perf_event_paranoid = 1' > /etc/sysctl.d/99-perf.conf",
        "echo 'kernel.task_delayacct=1' > /etc/sysctl.d/99-task-delayacct.conf",
        "echo 'net.ipv4.ip_local_port_range = 40000 65535' > /etc/sysctl.d/99-ip-local-port-range.conf",
        "printf '%s\n' '* soft nofile 1048576' '* hard nofile 1048576' >> /etc/security/limits.conf",
    ]
    if with_docker:
        commands.extend(
            [
                "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg",
                "echo \"deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable\" > /etc/apt/sources.list.d/docker.list",
                "apt-get update",
                "DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends docker-ce docker-buildx-plugin docker-ce-cli containerd.io",
                "mkdir -p /etc/docker",
                "printf '%s\n' '{' '  \"ipv6\": true,' '  \"fixed-cidr-v6\": \"2001:db8:1::/64\",' '  \"log-driver\": \"json-file\",' '  \"log-opts\": {' '    \"max-file\": \"5\",' '    \"max-size\": \"1000m\"' '  }' '}' > /etc/docker/daemon.json",
                "usermod -aG docker ubuntu || true",
                "systemctl enable docker || true",
                "systemctl restart docker || true",
                "sudo -u ubuntu docker buildx version",
                "sudo -u ubuntu docker buildx rm default-builder || true",
                "sudo -u ubuntu docker buildx create --use --name default-builder",
            ]
        )
    return {
        "name": name,
        "platform": "Linux",
        "description": (
            "Install Ubuntu system packages, GitHub CLI, CloudWatch agent, and Docker"
            if with_docker
            else "Install Ubuntu system packages, GitHub CLI, and CloudWatch agent"
        ),
        "commands": commands,
    }


def _controller_runtime_component(
    name: str,
    *,
    controller_package: str,
    break_system_packages: bool = False,
    controller_install_option: str = "--force-reinstall",
):
    pip_args = " --break-system-packages" if break_system_packages else ""
    controller_pip_args = (
        f" {controller_install_option}" if controller_install_option else ""
    )
    return {
        "name": name,
        "platform": "Linux",
        "description": "Install Praktika controller runtime dependencies into the image",
        "commands": [
            "mkdir -p /opt/praktika /opt/praktika/work",
            f"python3.12 -m pip install boto3 pyjwt cryptography requests{pip_args}",
            f"python3.12 -m pip install{controller_pip_args} {controller_package}{pip_args}",
            "ln -sf /usr/bin/python3.12 /usr/local/bin/python3",
        ],
    }


def _praktika_controller_component(name: str):
    launcher = """#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
REGION=${AWS_DEFAULT_REGION:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)}
INSTANCE_ID=${INSTANCE_ID:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)}
PRAKTIKA_CONTROLLER_ROLE=${PRAKTIKA_CONTROLLER_ROLE:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/praktika_role || true)}
PRAKTIKA_CONTROLLER_QUEUE=${PRAKTIKA_CONTROLLER_QUEUE:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/praktika_queue || true)}
PRAKTIKA_PROJECT_SLUG=${PRAKTIKA_PROJECT_SLUG:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/praktika_project_slug || true)}
if [ -z "$PRAKTIKA_CONTROLLER_ROLE" ] || [ -z "$PRAKTIKA_CONTROLLER_QUEUE" ]; then
  echo "praktika_role or praktika_queue instance tag is unavailable" >&2
  exit 1
fi
export HOME=/root
export AWS_DEFAULT_REGION="$REGION"
export INSTANCE_ID="$INSTANCE_ID"
export PRAKTIKA_PROJECT_SLUG
export PRAKTIKA_CONTROLLER_ROLE
export PRAKTIKA_CONTROLLER_QUEUE
exec /usr/local/bin/praktika-controller
"""
    # Both streams are baked into every image. praktika-controller.log always
    # has events; praktika-system.log only fills up when the streamer service
    # is activated via the praktika_system_logs instance tag (see below), so
    # its CloudWatch stream stays empty (and free) on pools that don't opt in.
    collect_list: List[Dict[str, str]] = [
        {
            "file_path": "/var/log/praktika-controller.log",
            "log_group_name": "/${PRAKTIKA_PROJECT_SLUG}/praktika-controller",
            "log_stream_name": "{instance_id}",
            "timezone": "UTC",
        },
        {
            # Kernel / systemd / OOM-killer evidence lives in the journal, not
            # in the controller's own log, so ship it to a separate stream.
            "file_path": "/var/log/praktika-system.log",
            "log_group_name": "/${PRAKTIKA_PROJECT_SLUG}/praktika-system",
            "log_stream_name": "{instance_id}",
            "timezone": "UTC",
        },
    ]
    cloudwatch_config_json = json.dumps(
        {"logs": {"logs_collected": {"files": {"collect_list": collect_list}}}},
        indent=2,
    )
    # NOTE: unquoted heredoc so ${PRAKTIKA_PROJECT_SLUG} expands at runtime;
    # {instance_id} has no leading $, so the CloudWatch agent resolves it.
    cloudwatch_configure = (
        """#!/usr/bin/env bash
set -euo pipefail

TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
PRAKTIKA_PROJECT_SLUG=${PRAKTIKA_PROJECT_SLUG:-$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/praktika_project_slug || true)}
if [ -z "$PRAKTIKA_PROJECT_SLUG" ]; then
  echo "praktika_project_slug instance tag is unavailable" >&2
  exit 1
fi

cat > /etc/praktika/amazon-cloudwatch-agent.json <<EOF
"""
        + cloudwatch_config_json
        + """
EOF

# The praktika-system-logs streamer (kernel/OOM/systemd-kill evidence) is baked
# into every image but stays off unless the pool opts in via the
# praktika_system_logs instance tag. Empty stream => no CloudWatch cost when off.
PRAKTIKA_SYSTEM_LOGS=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/praktika_system_logs || true)
case "$(printf '%s' "${PRAKTIKA_SYSTEM_LOGS:-}" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on|enabled)
    systemctl enable --now praktika-system-logs || true
    ;;
  *)
    systemctl disable --now praktika-system-logs || true
    ;;
esac
"""
    )
    # Follow the journal for the reasons the controller process can die without
    # leaving a trace in its own log: kernel OOM killer (_TRANSPORT=kernel),
    # systemd manager kill/restart notices (_PID=1), systemd-oomd, and anything
    # systemd logs about (or the controller's cgroup emits under) the unit.
    system_log_stream = """#!/usr/bin/env bash
set -euo pipefail

mkdir -p /var/lib/praktika
exec journalctl \\
  --output=short-iso \\
  --follow \\
  --cursor-file=/var/lib/praktika/system-log.cursor \\
  _TRANSPORT=kernel + _PID=1 + _COMM=systemd-oomd \\
  + UNIT=praktika-controller.service + _SYSTEMD_UNIT=praktika-controller.service
"""
    system_logs_unit = """[Unit]
Description=Praktika System Log Streamer
After=systemd-journald.service
Wants=systemd-journald.service

[Service]
Type=simple
ExecStart=/usr/local/bin/praktika-system-log-stream
Restart=always
RestartSec=5
StandardOutput=append:/var/log/praktika-system.log
StandardError=append:/var/log/praktika-system.log

[Install]
WantedBy=multi-user.target
"""
    unit = """[Unit]
Description=Praktika Controller
After=network.target docker.service

[Service]
Type=simple
Environment=HOME=/root
ExecStart=/usr/local/bin/praktika-controller-start
Restart=always
RestartSec=5
StandardOutput=append:/var/log/praktika-controller.log
StandardError=append:/var/log/praktika-controller.log

[Install]
WantedBy=multi-user.target
"""
    commands = [
        "mkdir -p /etc/praktika",
        "touch /var/log/praktika-controller.log",
        "chmod 0644 /var/log/praktika-controller.log",
        _write_file_from_base64("/usr/local/bin/praktika-controller-start", launcher),
        "chmod 0755 /usr/local/bin/praktika-controller-start",
        _write_file_from_base64(
            "/usr/local/bin/praktika-configure-cloudwatch-agent",
            cloudwatch_configure,
        ),
        "chmod 0755 /usr/local/bin/praktika-configure-cloudwatch-agent",
        _write_file_from_base64(
            "/etc/systemd/system/praktika-controller.service", unit
        ),
        # Always baked; NOT enabled here. Activation is per-pool at boot via the
        # praktika_system_logs instance tag, handled by the cloudwatch configure
        # script above.
        "touch /var/log/praktika-system.log",
        "chmod 0644 /var/log/praktika-system.log",
        _write_file_from_base64(
            "/usr/local/bin/praktika-system-log-stream", system_log_stream
        ),
        "chmod 0755 /usr/local/bin/praktika-system-log-stream",
        _write_file_from_base64(
            "/etc/systemd/system/praktika-system-logs.service",
            system_logs_unit,
        ),
        "systemctl daemon-reload || true",
    ]
    return {
        "name": name,
        "platform": "Linux",
        "description": "Bake the Praktika controller service into the image",
        "commands": commands,
    }


def _image_test_component(
    name: str,
    *,
    with_docker: bool,
    prebuilt_venvs: List[ImageBuilder.PrebuiltVenv],
):
    commands = [
        "test -x /usr/local/bin/praktika-controller",
        "test -x /usr/local/bin/praktika-controller-start",
        "test -x /usr/local/bin/praktika-configure-cloudwatch-agent",
        "test -x /usr/local/bin/praktika-system-log-stream",
        "bash -n /usr/local/bin/praktika-controller-start",
        "bash -n /usr/local/bin/praktika-configure-cloudwatch-agent",
        "bash -n /usr/local/bin/praktika-system-log-stream",
        "systemctl cat praktika-controller",
        # Baked but intentionally not enabled at build time (tag-activated).
        "systemctl cat praktika-system-logs",
        "test -x /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl",
        "aws --version",
        "git --version",
        "gh --version",
        "jq --version",
        "python3.12 --version",
        "python3.12 -m pip --version",
        "python3.12 -m pip show praktika-controller",
        'python3.12 -c "import boto3, jwt, cryptography, requests"',
    ]
    if with_docker:
        commands.extend(
            [
                "docker --version",
                "docker buildx version",
                "systemctl is-enabled docker",
                "test -s /etc/docker/daemon.json",
            ]
        )
    for venv in prebuilt_venvs:
        path = venv.path or f"/opt/praktika/base-venvs/{venv.name}"
        commands.extend(
            [
                f"test -x {path}/bin/python",
                f"{path}/bin/python -m pip show praktika",
                # Only assert Praktika's own runtime deps (from the
                # `infrastructure` extra). pytest and other packages are
                # optional, project-chosen extras — not guaranteed in the venv.
                f'{path}/bin/python -c "import boto3, jwt, cryptography, requests"',
            ]
        )
    return {
        "name": name,
        "platform": "Linux",
        "phase": "test",
        "description": "Validate the baked Praktika runner image",
        "commands": commands,
    }


def create_image_test_component(
    name: str,
    commands: List[str],
    *,
    description: str = "Validate custom image requirements",
    platform: str = "Linux",
) -> Dict[str, Any]:
    return {
        "name": name,
        "platform": platform,
        "phase": "test",
        "description": description,
        "commands": [str(command) for command in commands if str(command).strip()],
    }


def create_awslinux_image_builder_config(
    *,
    name: str,
    version: str,
    instance_types: List[str],
    components: Optional[List[Dict[str, Any]]] = None,
    prebuilt_venvs: Optional[List[ImageBuilder.PrebuiltVenv]] = None,
    controller_package: str = "praktika-controller",
) -> ImageBuilder.Config:
    return ImageBuilder.Config(
        name=name,
        image_recipe_version=version,
        inline_components=[
            _setup_component("praktika-controller-setup", with_docker=True),
            _controller_runtime_component(
                "praktika-controller-runtime",
                controller_package=controller_package,
            ),
            _praktika_controller_component("praktika-controller"),
            *(components or []),
        ],
        prebuilt_venvs=list(prebuilt_venvs or []),
        instance_types=instance_types,
    )


def create_ubuntu_image_builder_config(
    *,
    name: str,
    version: str,
    instance_types: List[str],
    components: Optional[List[Dict[str, Any]]] = None,
    prebuilt_venvs: Optional[List[ImageBuilder.PrebuiltVenv]] = None,
    controller_package: str = "praktika-controller",
) -> ImageBuilder.Config:
    family = (instance_types[0] if instance_types else "").split(".")[0]
    is_arm = family.endswith("g")
    if is_arm:
        from .configs import resolve_ubuntu_24_04_arm64_ami

        parent_image_resolver = resolve_ubuntu_24_04_arm64_ami
    else:
        from .configs import resolve_ubuntu_24_04_x86_64_ami

        parent_image_resolver = resolve_ubuntu_24_04_x86_64_ami

    return ImageBuilder.Config(
        name=name,
        image_recipe_version=version,
        parent_image_resolver=parent_image_resolver,
        image_tests_enabled=True,
        image_tests_timeout_minutes=60,
        inline_components=[
            _ubuntu_setup_component("praktika-controller-ubuntu-setup", with_docker=True),
            _controller_runtime_component(
                "praktika-controller-ubuntu-runtime",
                controller_package=controller_package,
                break_system_packages=True,
                controller_install_option="--ignore-installed",
            ),
            _praktika_controller_component("praktika-controller"),
            _image_test_component(
                "praktika-controller-ubuntu-image-test",
                with_docker=True,
                prebuilt_venvs=list(prebuilt_venvs or []),
            ),
            *(components or []),
        ],
        prebuilt_venvs=list(prebuilt_venvs or []),
        instance_types=instance_types,
    )
