#!/usr/bin/bash
set -euo pipefail

echo "Running init script"
export DEBIAN_FRONTEND=noninteractive
export RUNNER_HOME=/home/ubuntu/actions-runner

echo "Receiving token"
export RUNNER_TOKEN=`/usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value`
export RUNNER_URL="https://github.com/ClickHouse"

cd $RUNNER_HOME

echo "Going to configure runner"
sudo -u ubuntu ./config.sh --url $RUNNER_URL --token $RUNNER_TOKEN --name `hostname -f` --runnergroup Default --labels 'self-hosted,Linux,X64' --work _work

echo "Run"
sudo -u ubuntu ./run.sh
