#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In debug build abort_conf:true is set by default, disable it explicitly
# to avoid "Number of CPUs is not deterministic" error from jemalloc.
export MALLOC_CONF=abort_conf:false

# Regression for UAF in ThreadPool.
# (Triggered under TSAN)
for _ in {1..10}; do
    ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'
    # Binding to specific CPU is not required, but this makes the test more reliable.
    taskset --cpu-list 0 ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'
done
