#!/bin/bash

# yaml check is not the best one

cd /ClickHouse/utils/check-style || echo -e "failure\tRepo not found" > /test_output/check_status.tsv

./run.sh "$@"
