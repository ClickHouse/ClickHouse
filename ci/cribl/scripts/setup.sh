#!/bin/bash
set -ex

git config --global --add safe.directory ${WORKSPACE}

# need the AWS CLI to make things easier..
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install

git submodule update --init -j $(nproc)