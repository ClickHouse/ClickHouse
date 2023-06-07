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
curl -s "${TEAM_KEYS_URL}" > /home/ubuntu/.ssh/authorized_keys2
chown ubuntu: /home/ubuntu/.ssh -R


# Create a pre-run script that will provide diagnostics info
mkdir -p /tmp/actions-hooks
cat > /tmp/actions-hooks/common.sh << 'EOF'
#!/bin/bash
terminate-delayed() {
  sleep=7
  echo "Going to terminate the runner's instance in $sleep seconds"
  INSTANCE_ID=$(ec2metadata --instance-id)
  # We execute it with `at` to not have it as an orphan process, but launched independently
  # GH Runners kill all remain processes
  echo "sleep '$sleep'; aws ec2 terminate-instances --instance-ids $INSTANCE_ID" | at now || \
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID"  # workaround for complete out of space or non-installed `at`
  exit 0
}

terminate-and-exit() {
  echo "Going to terminate the runner's instance"
  INSTANCE_ID=$(ec2metadata --instance-id)
  aws ec2 terminate-instances --instance-ids "$INSTANCE_ID"
}

check-terminating-metadata() {
  # If there is a rebalance event, then the instance could die soon
  # Let's don't wait for it and terminate proactively
  if curl -s --fail http://169.254.169.254/latest/meta-data/events/recommendations/rebalance; then
    echo 'The received recommendation to rebalance, checking the uptime'
    UPTIME=$(< /proc/uptime)
    UPTIME=${UPTIME%%.*}
    # We don't shutdown the instances younger than 30m
    if (( 1800 < UPTIME )); then
      # To not shutdown everything at once, use the 66% to survive
      if (( $((RANDOM % 3)) == 0 )); then
        echo 'The instance is older than 30m and won the roulette'
        terminate-and-exit
      fi
      echo 'The instance is older than 30m, but is not chosen for rebalance'
    else
      echo 'The instance is younger than 30m, do not shut it down'
    fi
  fi

  # Here we check if the autoscaling group marked the instance for termination, and it's wait for the job to finish
  ASG_STATUS=$(curl -s http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state)
  if [ "$ASG_STATUS" == "Terminated" ]; then
    INSTANCE_ID=$(ec2metadata --instance-id)
    ASG_NAME=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" --query "Tags[?Key=='aws:autoscaling:groupName'].Value" --output text)
    LIFECYCLE_HOOKS=$(aws autoscaling describe-lifecycle-hooks --auto-scaling-group-name "$ASG_NAME" --query "LifecycleHooks[].LifecycleHookName" --output text)
    for LCH in $LIFECYCLE_HOOKS; do
      aws autoscaling complete-lifecycle-action --lifecycle-action-result CONTINUE \
        --lifecycle-hook-name "$LCH" --auto-scaling-group-name "$ASG_NAME" \
        --instance-id "$INSTANCE_ID"
    done
    echo 'The runner is marked as "Terminated" by the autoscaling group, we are terminating'
    terminate-and-exit
  fi
}
EOF

cat > /tmp/actions-hooks/pre-run.sh << EOF
#!/bin/bash
set -uo pipefail

echo "Runner's public DNS: $(ec2metadata --public-hostname)"
echo "Runner's labels: ${LABELS}"
EOF

# Create a post-run script that will restart docker daemon before the job started
cat > /tmp/actions-hooks/post-run.sh << 'EOF'
#!/bin/bash
set -xuo pipefail

source /tmp/actions-hooks/common.sh

# Free KiB, free percents
ROOT_STAT=($(df / | awk '/\// {print $4 " " int($4/$2 * 100)}'))
if [[ ${ROOT_STAT[0]} -lt 3000000 ]] || [[ ${ROOT_STAT[1]} -lt 5 ]]; then
  echo "The runner has ${ROOT_STAT[0]}KiB and ${ROOT_STAT[1]}% of free space on /"
  terminate-delayed
fi

# shellcheck disable=SC2046
docker ps --quiet | xargs --no-run-if-empty docker kill ||:
# shellcheck disable=SC2046
docker ps --all --quiet | xargs --no-run-if-empty docker rm -f ||:

# If we have hanged containers after the previous commands, than we have a hanged one
# and should restart the daemon
if [ "$(docker ps --all --quiet)" ]; then
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
  docker info 1>/dev/null || { echo Docker unable to start; terminate-delayed ; }
fi
EOF

source /tmp/actions-hooks/common.sh

while true; do
    runner_pid=$(pgrep Runner.Listener)
    echo "Got runner pid $runner_pid"

    if [ -z "$runner_pid" ]; then
        cd $RUNNER_HOME || exit 1
        # If runner is not active, check that it needs to terminate itself
        echo "Checking if the instance suppose to terminate"
        check-terminating-metadata

        echo "Receiving token"
        RUNNER_TOKEN=$(/usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value)

        echo "Going to configure runner"
        sudo -u ubuntu ./config.sh --url $RUNNER_URL --token "$RUNNER_TOKEN" --ephemeral \
          --runnergroup Default --labels "$LABELS" --work _work --name "$INSTANCE_ID"

        echo "Another one check to avoid race between runner and infrastructure"
        check-terminating-metadata

        echo "Run"
        sudo -u ubuntu \
          ACTIONS_RUNNER_HOOK_JOB_STARTED=/tmp/actions-hooks/pre-run.sh \
          ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/tmp/actions-hooks/post-run.sh \
          ./run.sh &
        sleep 15
    else
        echo "Runner is working with pid $runner_pid, nothing to do"
        # The runner does not provide a way to determine, if it runs the job,
        # neither the way to determine if it just litens. But there should be a
        # process for Runner.Worker. So if the runner just hangs around for long,
        # we check if it's fine to let it go
        if ! pgrep Runner.Worker > /dev/null; then
            RUNNER_AGE=$(( $(date +%s) - $(stat -c +%Y /proc/"$runner_pid" 2>/dev/null || date +%s) ))
            echo "The runner is launched $RUNNER_AGE seconds ago and still doesn't have launched Runner.Worker"
            if (( 60 < RUNNER_AGE )); then
                echo "Check if the instance should tear down"
                check-terminating-metadata
            fi
        fi
        sleep 5
    fi
done

# vim:ts=4:sw=4
