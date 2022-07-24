#!/usr/bin/env bash
set -euo pipefail

cd $REPO_PATH/docs/tools
mkdir venv
virtualenv -p $(which python3) venv
source venv/bin/activate
python3 -m pip install --ignore-installed -r requirements.txt
mkdir -p ~/.ssh && ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
./release.sh 2>&1 | tee tee $OUTPUT_PATH/output.log
