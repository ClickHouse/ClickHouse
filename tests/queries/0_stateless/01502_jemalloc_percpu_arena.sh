#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan
#       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# NOTE: jemalloc is disabled under sanitizers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ncpus="$(getconf _NPROCESSORS_ONLN)"

# In debug build abort_conf:true is set by default, disable it explicitly
export MALLOC_CONF=abort_conf:false

# Regression for:
#
#     $ taskset --cpu-list 8 ./clickhouse local -q 'select 1'
#     <jemalloc>: ../contrib/jemalloc/src/jemalloc.c:321: Failed assertion: "ind <= narenas_total_get()"
#     Aborted (core dumped)
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select 1'
# just in case something more complicated
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'

# this command should fail because percpu arena will be disabled,
# and with abort_conf:true it is not allowed
(
    # subshell is required to suppress "Aborted" message from the shell.
    MALLOC_CONF=abort_conf:true
    taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select 1'
) |& grep -F 'Number of CPUs is not deterministic'
