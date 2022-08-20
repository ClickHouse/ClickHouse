#!/usr/bin/env bash
set -euo pipefail

cd "$REPO_PATH/docs/tools"
if ! [ -d venv ]; then
  mkdir -p venv
  virtualenv -p "$(which python3)" venv
  source venv/bin/activate
  python3 -m pip install --ignore-installed -r requirements.txt
fi
source venv/bin/activate
./release.sh 2>&1 | tee "$OUTPUT_PATH/output.log"
