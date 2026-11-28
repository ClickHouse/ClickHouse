#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug
#       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# NOTE: jemalloc is disabled under sanitizers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ncpus="$(getconf _NPROCESSORS_ONLN)"

# percpu arenas are sized by the possible CPU count (nr_cpu_ids), so a
# restricted affinity mask must not disable them or abort, even with abort_conf:true.
# NOTE: abort:false for compatibility with debug builds.
export MALLOC_CONF=abort_conf:true,abort:false

# Regression for:
#
#     $ taskset --cpu-list 8 ./clickhouse local -q 'select 1'
#     <jemalloc>: ../contrib/jemalloc/src/jemalloc.c:321: Failed assertion: "ind <= narenas_total_get()"
#     Aborted (core dumped)
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select 1' 2>&1

# just in case something more complicated
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select count() from numbers_mt(100000000) settings max_threads=100' 2>&1

MALLOC_CONF=$MALLOC_CONF,narenas:$((ncpus)) taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select 1' 2>&1
