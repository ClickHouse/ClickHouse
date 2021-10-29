#!/usr/bin/env bash
set -euo pipefail


cd $REPO_PATH/docs/tools
./build.py --skip-git-log 2>&1 | tee $OUTPUT_PATH/output.log
