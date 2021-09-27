#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export RUNNER_VERSION=2.283.1
export RUNNER_HOME=/home/ubuntu/actions-runner

apt-get update

apt-get install --yes --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    python3-pip

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update

apt-get install --yes --no-install-recommends docker-ce docker-ce-cli containerd.io

usermod -aG docker ubuntu

pip install boto3 pygithub requests urllib3 unidiff

mkdir -p $RUNNER_HOME && cd $RUNNER_HOME

curl -O -L https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-x64-$RUNNER_VERSION.tar.gz

tar xzf ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz
rm -f ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz

./bin/installdependencies.sh
