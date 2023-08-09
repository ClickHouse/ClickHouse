#!/usr/bin/env bash
set -uo pipefail

####################################
#            IMPORTANT!            #
# EC2 instance should have         #
# `github:runner-type` tag         #
# set accordingly to a runner role #
####################################

echo "Running init script"
export DEBIAN_FRONTEND=noninteractive
export RUNNER_HOME=/home/ubuntu/actions-runner

export RUNNER_URL="https://github.com/ClickHouse"
# Funny fact, but metadata service has fixed IP
INSTANCE_ID=$(ec2metadata --instance-id)
export INSTANCE_ID

# Add cloudflare DNS as a fallback
# Get default gateway interface
IFACE=$(ip --json route list | jq '.[]|select(.dst == "default").dev' --raw-output)
# `Link 2 (eth0): 172.31.0.2`
ETH_DNS=$(resolvectl dns "$IFACE") || :
CLOUDFLARE_NS=1.1.1.1
if [[ "$ETH_DNS" ]] && [[ "${ETH_DNS#*: }" != *"$CLOUDFLARE_NS"* ]]; then
  # Cut the leading legend
  ETH_DNS=${ETH_DNS#*: }
  # shellcheck disable=SC2206
  new_dns=(${ETH_DNS} "$CLOUDFLARE_NS")
  resolvectl dns "$IFACE" "${new_dns[@]}"
fi

# combine labels
RUNNER_TYPE=$(/usr/local/bin/aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" --query "Tags[?Key=='github:runner-type'].Value" --output text)
LABELS="self-hosted,Linux,$(uname -m),$RUNNER_TYPE"
export LABELS

# Refresh CloudWatch agent config
aws ssm get-parameter --region us-east-1 --name AmazonCloudWatch-github-runners --query 'Parameter.Value' --output text > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
systemctl restart amazon-cloudwatch-agent.service

# Refresh teams ssh keys
TEAM_KEYS_URL=$(aws ssm get-parameter --region us-east-1 --name team-keys-url --query 'Parameter.Value' --output=text)
curl "${TEAM_KEYS_URL}" > /home/ubuntu/.ssh/authorized_keys2
chown ubuntu: /home/ubuntu/.ssh -R


# Create a pre-run script that will restart docker daemon before the job started
mkdir -p /tmp/actions-hooks
cat > /tmp/actions-hooks/pre-run.sh << 'EOF'
#!/bin/bash
set -xuo pipefail

echo "Runner's public DNS: $(ec2metadata --public-hostname)"
EOF

cat > /tmp/actions-hooks/post-run.sh << 'EOF'
#!/bin/bash
set -xuo pipefail

terminate-and-exit() {
  echo "Going to terminate the runner"
  INSTANCE_ID=$(ec2metadata --instance-id)
  # We execute it with at to not have it as an orphan process
  # GH Runners kill all remain processes
  echo "sleep 10; aws ec2 terminate-instances --instance-ids $INSTANCE_ID" | at now
  exit 0
}

# Free KiB, free percents
ROOT_STAT=($(df / | awk '/\// {print $4 " " int($4/$2 * 100)}'))
if [[ ${ROOT_STAT[0]} -lt 3000000 ]] || [[ ${ROOT_STAT[1]} -lt 5 ]]; then
  echo "The runner has ${ROOT_STAT[0]}KiB and ${ROOT_STAT[1]}% of free space on /"
  terminate-and-exit
fi

# shellcheck disable=SC2046
docker kill $(docker ps -q) ||:
# shellcheck disable=SC2046
docker rm -f $(docker ps -a -q) ||:

# If we have hanged containers after the previous commands, than we have a hanged one
# and should restart the daemon
if [ "$(docker ps -a -q)" ]; then
  # Systemd service of docker has StartLimitBurst=3 and StartLimitInterval=60s,
  # that's why we try restarting it for long
  for i in {1..25};
  do
    sudo systemctl restart docker && break || sleep 5
  done

  for i in {1..10}
  do
    docker info && break || sleep 2
  done
  # Last chance, otherwise we have to terminate poor instance
  docker info 1>/dev/null || { echo Docker unable to start; terminate-and-exit; }
fi
EOF

while true; do
    runner_pid=$(pgrep run.sh)
    echo "Got runner pid $runner_pid"

    cd $RUNNER_HOME || exit 1
    if [ -z "$runner_pid" ]; then
        echo "Receiving token"
        RUNNER_TOKEN=$(/usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value)

        echo "Will try to remove runner"
        sudo -u ubuntu ./config.sh remove --token "$RUNNER_TOKEN" ||:

        echo "Going to configure runner"
        sudo -u ubuntu ./config.sh --url $RUNNER_URL --token "$RUNNER_TOKEN" --name "$INSTANCE_ID" --runnergroup Default --labels "$LABELS" --work _work

        echo "Run"
        sudo -u ubuntu \
          ACTIONS_RUNNER_HOOK_JOB_STARTED=/tmp/actions-hooks/pre-run.sh \
          ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/tmp/actions-hooks/post-run.sh \
          ./run.sh &
        sleep 15
    else
        echo "Runner is working with pid $runner_pid, nothing to do"
        sleep 10
    fi
done
