#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ncpus="$(getconf _NPROCESSORS_ONLN)"

# to hit possible issues even in unbundled builds:
# (although likiley jemalloc will be compiled with NDEBUG there)
export MALLOC_CONF=percpu_arena:percpu

# Regression for:
#
#     $ taskset --cpu-list 8 ./clickhouse local -q 'select 1'
#     <jemalloc>: ../contrib/jemalloc/src/jemalloc.c:321: Failed assertion: "ind <= narenas_total_get()"
#     Aborted (core dumped)
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select 1'
# just in case something more complicated
taskset --cpu-list $((ncpus-1)) ${CLICKHOUSE_LOCAL} -q 'select * from numbers_mt(100000000) settings max_threads=100 FORMAT Null'
