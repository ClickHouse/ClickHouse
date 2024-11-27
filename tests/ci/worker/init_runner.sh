#!/usr/bin/env bash

cat > /dev/null << 'EOF'
The following content is embedded into the s3 object via the script
deploy-runner-init.sh {staging,production}
with additional helping information

In the `user data` you should define as the following text
between `### COPY BELOW` and `### COPY ABOVE`

### COPY BELOW
Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash
INSTANCE_ID=$(ec2metadata --instance-id)
INIT_ENVIRONMENT=$(/usr/local/bin/aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" --query "Tags[?Key=='github:init-environment'].Value" --output text)
echo "Downloading and using $INIT_ENVIRONMENT cloud-init.sh"
aws s3 cp "s3://github-runners-data/cloud-init/${INIT_ENVIRONMENT:-production}.sh" /tmp/cloud-init.sh
chmod 0700 /tmp/cloud-init.sh
exec bash /tmp/cloud-init.sh
--//
### COPY ABOVE
EOF

# THE SCRIPT START

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

export RUNNER_ORG="ClickHouse"
export RUNNER_URL="https://github.com/${RUNNER_ORG}"
# Funny fact, but metadata service has fixed IP
INSTANCE_ID=$(ec2metadata --instance-id)
export INSTANCE_ID

bash /usr/local/share/scripts/init-network.sh

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
EOF

terminate_delayed() {
    # The function for post hook to gracefully finish the job and then tear down
    # The very specific sleep time is used later to determine in the main loop if
    # the instance is tearing down
    # IF `sleep` IS CHANGED, CHANGE ANOTHER VALUE IN `pgrep`
    sleep=13.14159265358979323846
    echo "Going to terminate the runner's instance in $sleep seconds"
    INSTANCE_ID=$(ec2metadata --instance-id)
    # We execute it with `at` to not have it as an orphan process, but launched independently
    # GH Runners kill all remain processes
    echo "sleep '$sleep'; aws ec2 terminate-instances --instance-ids $INSTANCE_ID" | at now || \
        aws ec2 terminate-instances --instance-ids "$INSTANCE_ID"  # workaround for complete out of space or non-installed `at`
    exit 0
}

detect_delayed_termination() {
    # The function look for very specific sleep with pi
    if pgrep 'sleep 13.14159265358979323846'; then
        echo 'The instance has delayed termination, sleep the same time to wait if it goes down'
        sleep 14
    fi
}

declare -f terminate_delayed >> /tmp/actions-hooks/common.sh

terminate_and_exit() {
    # Terminate instance and exit from the script instantly
    echo "Going to terminate the runner's instance"
    INSTANCE_ID=$(ec2metadata --instance-id)
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID"
    exit 0
}

declare -f terminate_and_exit >> /tmp/actions-hooks/common.sh

check_spot_instance_is_old() {
    # This function should be executed ONLY BETWEEN runnings.
    # It's unsafe to execute while the runner is working!
    local LIFE_CYCLE
    LIFE_CYCLE=$(curl -s --fail http://169.254.169.254/latest/meta-data/instance-life-cycle)
    if [ "$LIFE_CYCLE" == "spot" ]; then
        local UPTIME
        UPTIME=$(< /proc/uptime)
        UPTIME=${UPTIME%%.*}
        if (( 3600 < UPTIME )); then
            echo "The spot instance has uptime $UPTIME, it's time to shut it down"
            return 0
        fi
    fi
    return 1
}

check_proceed_spot_termination() {
    # The function checks and proceeds spot instance termination if exists
    # The event for spot instance termination
    local FORCE
    FORCE=${1:-}
    if TERMINATION_DATA=$(curl -s --fail http://169.254.169.254/latest/meta-data/spot/instance-action); then
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html#instance-action-metadata
        _action=$(jq '.action' -r <<< "$TERMINATION_DATA")
        _time=$(jq '.time | fromdate' <<< "$TERMINATION_DATA")
        _until_action=$((_time - $(date +%s)))
        echo "Received the '$_action' event that will be effective in $_until_action seconds"
        if (( _until_action <= 30 )) || [ "$FORCE" == "force" ]; then
            echo "The action $_action will be done in $_until_action, killing the runner and exit"
            local runner_pid
            runner_pid=$(pgrep Runner.Listener)
            if [ -n "$runner_pid" ]; then
                # Kill the runner to not allow it cancelling the job
                # shellcheck disable=SC2046
                kill -9 "$runner_pid" $(list_children "$runner_pid")
            fi
            sudo -u ubuntu ./config.sh remove --token "$(get_runner_token)"
            terminate_and_exit
        fi
    fi
}

no_terminating_metadata() {
    # The function check that instance could continue work
    # Returns 1 if any of termination events are received

    # The event for rebalance recommendation. Not strict, so we have some room to make a decision here
    if curl -s --fail http://169.254.169.254/latest/meta-data/events/recommendations/rebalance; then
        echo 'Received recommendation to rebalance, checking the uptime'
        local UPTIME
        UPTIME=$(< /proc/uptime)
        UPTIME=${UPTIME%%.*}
        # We don't shutdown the instances younger than 30m
        if (( 1800 < UPTIME )); then
            # To not shutdown everything at once, use the 66% to survive
            if (( $((RANDOM % 3)) == 0 )); then
                echo 'The instance is older than 30m and won the roulette'
                return 1
            fi
            echo 'The instance is older than 30m, but is not chosen for rebalance'
        else
            echo 'The instance is younger than 30m, do not shut it down'
        fi
    fi

    # Checks if the ASG in a lifecycle hook state
    local ASG_STATUS
    ASG_STATUS=$(curl -s http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state)
    if [ "$ASG_STATUS" == "Terminated" ]; then
        echo 'The instance in ASG status Terminating:Wait'
        return 1
    fi
}

terminate_on_event() {
    # If there is a rebalance event, then the instance could die soon
    # Let's don't wait for it and terminate proactively
    if curl -s --fail http://169.254.169.254/latest/meta-data/events/recommendations/rebalance; then
        terminate_and_exit
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
            true  # autoformat issue
        done
        echo 'The runner is marked as "Terminated" by the autoscaling group, we are terminating'
        terminate_and_exit
    fi
}

cat > /tmp/actions-hooks/pre-run.sh << EOF
#!/bin/bash
set -uo pipefail

echo "Runner's public DNS: $(ec2metadata --public-hostname)"
echo "Runner's labels: ${LABELS}"
echo "Runner's instance type: $(ec2metadata --instance-type)"
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
  terminate_delayed
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
  docker info 1>/dev/null || { echo Docker unable to start; terminate_delayed ; }
fi
EOF

get_runner_token() {
    /usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value
}

is_job_assigned() {
    local runner_pid
    runner_pid=$(pgrep Runner.Listener)
    if [ -z "$runner_pid" ]; then
        # if runner has finished, it's fine
        return 0
    fi
    local log_file
    log_file=$(lsof -p "$runner_pid" 2>/dev/null | grep -o "$RUNNER_HOME/_diag/Runner.*log")
    if [ -z "$log_file" ]; then
        # assume, the process is over or just started
        return 0
    fi
    # So far it's the only solid way to determine that the job is starting
    grep -q 'Terminal] .* Running job:' "$log_file" \
        && return 0 \
        || return 1
}

list_children () {
    local children
    children=$(ps --ppid "$1" -o pid=)
    if [ -z "$children" ]; then
        return
    fi

    for pid in $children; do
        list_children "$pid"
    done
    echo "$children"
}

while true; do
    runner_pid=$(pgrep Runner.Listener)
    echo "Got runner pid '$runner_pid'"

    if [ -z "$runner_pid" ]; then
        cd $RUNNER_HOME || terminate_and_exit
        detect_delayed_termination
        # If runner is not active, check that it needs to terminate itself
        echo "Checking if the instance suppose to terminate"
        no_terminating_metadata || terminate_on_event
        check_spot_instance_is_old && terminate_and_exit
        check_proceed_spot_termination force

        echo "Going to configure runner"
        sudo -u ubuntu ./config.sh --url $RUNNER_URL --token "$(get_runner_token)" \
          --ephemeral --disableupdate --unattended \
          --runnergroup Default --labels "$LABELS" --work _work --name "$INSTANCE_ID"

        echo "Another one check to avoid race between runner and infrastructure"
        no_terminating_metadata || terminate_on_event
        check_spot_instance_is_old && terminate_and_exit
        check_proceed_spot_termination force

        echo "Run"
        sudo -u ubuntu \
          ACTIONS_RUNNER_HOOK_JOB_STARTED=/tmp/actions-hooks/pre-run.sh \
          ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/tmp/actions-hooks/post-run.sh \
          ./run.sh &
        sleep 10
    else
        echo "Runner is working with pid $runner_pid, checking the metadata in background"
        check_proceed_spot_termination

        if ! is_job_assigned; then
            RUNNER_AGE=$(( $(date +%s) - $(stat -c +%Y /proc/"$runner_pid" 2>/dev/null || date +%s) ))
            echo "The runner is launched $RUNNER_AGE seconds ago and still has hot received the job"
            if (( 60 < RUNNER_AGE )); then
                echo "Attempt to delete the runner for a graceful shutdown"
                sudo -u ubuntu ./config.sh remove --token "$(get_runner_token)" \
                    || continue
                echo "Runner didn't launch or have assigned jobs after ${RUNNER_AGE} seconds, shutting down"
                terminate_and_exit
            fi
        fi
    fi
    sleep 5
done

# vim:ts=4:sw=4
