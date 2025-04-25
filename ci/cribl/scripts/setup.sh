#!/bin/bash
set -ex

THIS_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
source ${THIS_DIR}/utils.sh

git config --global --add safe.directory ${WORKSPACE}

# need the AWS CLI to make things easier..
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install

# install and setup docker CLI
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(get_arch) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" >> /etc/apt/sources.list.d/docker.list
apt-get update && apt-get install -y docker-ce-cli

git submodule update --init -j $(nproc)