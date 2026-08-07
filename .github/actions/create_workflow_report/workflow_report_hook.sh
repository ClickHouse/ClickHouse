#!/bin/bash
# This script is for generating preview reports when invoked as a post-hook from a praktika job
# Newer runners have these deps preinstalled; only install on Ubuntu 22.
if [[ -f /etc/os-release ]] && . /etc/os-release && [[ "$VERSION_ID" == "22.04" ]]; then
  pip install clickhouse-driver==0.2.8 numpy==1.26.4 pandas==2.0.3 jinja2==3.1.5
fi
ARGS="--mark-preview --known-fails --cves --actions-run-url $GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID --pr-number $PR_NUMBER"
CMD="python3 .github/actions/create_workflow_report/create_workflow_report.py"
$CMD $ARGS

