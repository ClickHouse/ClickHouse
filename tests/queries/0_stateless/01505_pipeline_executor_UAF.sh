#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Regression for UAF in ThreadPool.
# (Triggered under TSAN)
for i in {1..10}; do
    ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'
    # Binding to specific CPU is not required, but this makes the test more reliable.
    taskset --cpu-list 0 ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'
done
