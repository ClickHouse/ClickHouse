#!/bin/bash

# yaml check is not the best one

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv
echo "Check duplicates" | ts
./check-duplicate-includes.sh |& tee /test_output/duplicate_output.txt
echo "Check style" | ts
./check-style -n              |& tee /test_output/style_output.txt
echo "Check python formatting with black" | ts
./check-black -n              |& tee /test_output/black_output.txt
echo "Check typos" | ts
./check-typos                 |& tee /test_output/typos_output.txt
echo "Check whitespaces" | ts
./check-whitespaces -n        |& tee /test_output/whitespaces_output.txt
echo "Check workflows" | ts
./check-workflows             |& tee /test_output/workflows_output.txt
echo "Check shell scripts with shellcheck" | ts
./shellcheck-run.sh           |& tee /test_output/shellcheck_output.txt
/process_style_check_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
