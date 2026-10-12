#!/usr/bin/env bash
# Prototype: run each GitHub Actions job on a macOS runner host in a throwaway Tart VM.
#
#   macos-vm-runner.sh golden   build the provisioned VM image once per host (or per tool update)
#   macos-vm-runner.sh serve    keep SLOTS ephemeral VMs, each registered for exactly one job
#
# The host needs `tart` and `sshpass` (`brew install cirruslabs/cli/tart sshpass`), the AWS CLI,
# and IMDS reachable from VMs behind NAT: `--http-put-response-hop-limit 2` on the instance.
set -euo pipefail

BASE_IMAGE=${BASE_IMAGE:-ghcr.io/cirruslabs/macos-tahoe-base:latest}
GOLDEN=${GOLDEN:-ch-runner-golden}
SLOTS=${SLOTS:-2}
VM_CPU=${VM_CPU:-6}
VM_MEMORY_MIB=${VM_MEMORY_MIB:-14336}
GUEST_USER="admin"
RUNNER_URL=https://github.com/ClickHouse

log() { echo "[$(date -u +%FT%TZ)] $*"; }

imds() {
    local token
    token=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
    curl -fsS -H "X-aws-ec2-metadata-token: $token" "http://169.254.169.254/latest/meta-data/$1"
}

guest() {
    local vm=$1; shift
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
        -i "$TART_HOME/id_ed25519" "$GUEST_USER@$(tart ip --wait 180 "$vm")" "$@"
}

golden() {
    [ -f "$TART_HOME/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -f "$TART_HOME/id_ed25519"
    tart delete "$GOLDEN" 2>/dev/null || true
    tart clone "$BASE_IMAGE" "$GOLDEN"
    tart set "$GOLDEN" --cpu "$VM_CPU" --memory "$VM_MEMORY_MIB" --disk-size 120
    tart run --no-graphics "$GOLDEN" > "$TART_HOME/$GOLDEN.log" 2>&1 &
    local ip
    ip=$(tart ip --wait 180 "$GOLDEN")
    # The Cirrus images ship with password `admin`; the key replaces it for every later login.
    sshpass -p admin ssh-copy-id -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -i "$TART_HOME/id_ed25519.pub" "$GUEST_USER@$ip"

    # Same tool set as `configure_darwin` in runner-init.py.
    guest "$GOLDEN" bash -euxo pipefail -s <<'EOF'
eval "$(/opt/homebrew/bin/brew shellenv)"
brew update
brew install ca-certificates curl gh jq pigz pv ripgrep zstd wget unzip gnu-sed grep bash coreutils llvm python@3 awscli
brew cleanup --prune=all -s
"$(brew --prefix python@3)/bin/python3" -m venv ~/venv
~/venv/bin/pip install --upgrade pip
~/venv/bin/pip install --upgrade boto3 pygithub requests urllib3 unidiff dohq-artifactory pyjwt \
    numpy==2.3.2 pandas==2.3.3 Jinja2==3.1.6
case $(uname -m) in arm64) ARCH=arm64 ;; *) ARCH=x64 ;; esac
VERSION=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest | jq -r '.tag_name | ltrimstr("v")')
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -fsSL "https://github.com/actions/runner/releases/download/v$VERSION/actions-runner-osx-$ARCH-$VERSION.tar.gz" | tar xz
sudo mdutil -a -i off || true
sudo tmutil disable || true
EOF
    guest "$GOLDEN" sudo shutdown -h now || true
    while tart list --format json | jq -e --arg n "$GOLDEN" '.[] | select(.Name == $n and .State == "running")' >/dev/null; do sleep 5; done
    log "golden image $GOLDEN ready"
}

# One job: clone, boot, register with `--ephemeral`, wait for `run.sh` to exit, delete.
run_slot() {
    local slot=$1 instance runner_type labels vm token
    instance=$(imds instance-id)
    runner_type=$(aws ec2 describe-tags --region "$(imds placement/region)" \
        --filters "Name=resource-id,Values=$instance" Name=key,Values=github:runner-type \
        --query 'Tags[0].Value' --output text)
    labels="self-hosted,darwin,$runner_type"
    while true; do
        vm="job-$slot-$(date +%s)"
        token=$(aws ssm get-parameter --region us-east-1 --name github_runner_registration_token \
            --with-decryption --query Parameter.Value --output text)
        log "slot $slot: starting $vm"
        tart clone "$GOLDEN" "$vm"
        tart run --no-graphics "$vm" > "$TART_HOME/$vm.log" 2>&1 &
        guest "$vm" "cd ~/actions-runner && ./config.sh --unattended --ephemeral --replace \
            --url $RUNNER_URL --token $token --runnergroup Default --labels $labels \
            --work _work --name $instance-$slot && ./run.sh" || log "slot $slot: $vm exited with $?"
        tart stop "$vm" || true
        tart delete "$vm"
        rm -f "$TART_HOME/$vm.log"
    done
}

serve() {
    tart list --format json | jq -e --arg n "$GOLDEN" '.[] | select(.Name == $n)' >/dev/null \
        || { log "no golden image $GOLDEN; run '$0 golden' first"; exit 1; }
    # VMs left by a crashed host process would hold the 2-VM limit.
    tart list --format json | jq -r '.[] | select(.Name | startswith("job-")) | .Name' | xargs -n1 tart delete 2>/dev/null || true
    for slot in $(seq 1 "$SLOTS"); do
        run_slot "$slot" &
    done
    wait
}

case ${1:-} in
    golden | serve) ;;
    *) sed -n 2,8p "$0"; exit 2 ;;
esac

# The 460 GB instance-store SSD; the root volume is too small for images and per-job clones.
SSD=$(df | awk '/tmp-mount/ {print $NF; exit}')
export TART_HOME=${TART_HOME:-$SSD/tart}
[ "$TART_HOME" != /tart ] || { log "no instance-store SSD mounted; set TART_HOME"; exit 1; }
mkdir -p "$TART_HOME"
"$1"
