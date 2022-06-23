#!/usr/bin/env bash
set -uo pipefail

echo "Running init script"
export DEBIAN_FRONTEND=noninteractive
export RUNNER_HOME=/home/ubuntu/actions-runner

export RUNNER_URL="https://github.com/ClickHouse"
# Funny fact, but metadata service has fixed IP
export INSTANCE_ID=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

while true; do
    runner_pid=`pgrep run.sh`
    echo "Got runner pid $runner_pid"

    cd $RUNNER_HOME
    if [ -z "$runner_pid" ]; then
        echo "Receiving token"
        RUNNER_TOKEN=`/usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value`

        echo "Will try to remove runner"
        sudo -u ubuntu ./config.sh remove --token $RUNNER_TOKEN ||:

        echo "Going to configure runner"
        sudo -u ubuntu ./config.sh --url $RUNNER_URL --token $RUNNER_TOKEN --name $INSTANCE_ID --runnergroup Default --labels 'self-hosted,Linux,X64,func-tester' --work _work

        echo "Run"
        sudo -u ubuntu ./run.sh &
        sleep 15
    else
        echo "Runner is working with pid $runner_pid, nothing to do"
        sleep 10
    fi
done
