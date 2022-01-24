#!/usr/bin/env bash
set -euo pipefail

cd $REPO_PATH/docs/tools
rm -rf venv
mkdir venv
virtualenv -p $(which python3) venv
source venv/bin/activate
python3 -m pip install --ignore-installed -r requirements.txt
./build.py --skip-git-log 2>&1 | tee $OUTPUT_PATH/output.log
