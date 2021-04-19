#!/bin/bash

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv
./check-style -n              |& tee /test_output/style_output.txt
./check-typos                 |& tee /test_output/typos_output.txt
./check-whitespaces -n        |& tee /test_output/whitespaces_output.txt
./check-duplicate-includes.sh |& tee /test_output/duplicate_output.txt
./shellcheck-run.sh           |& tee /test_output/shellcheck_output.txt
/process_style_check_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
