#!/usr/bin/env bash
# Tags: race, no-parallel, no-flaky-check
# Tag no-parallel: runs 4 concurrent copies of 00097_long_storage_buffer_race_condition.sh, amplifying its CPU-contention timing sensitivity; times out under concurrent-suite load on busy machines
#  - no-parallel: FIXME start to timeout on bigger machines under high load, no-flaky-check - too long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export NO_SHELL_CONFIG=1

for _ in {1..4}; do
    "$CURDIR"/00097_long_storage_buffer_race_condition.sh  > /dev/null 2>&1 &
done

wait

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
