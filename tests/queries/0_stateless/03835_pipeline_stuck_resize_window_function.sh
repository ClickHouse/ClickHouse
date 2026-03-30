#!/usr/bin/env bash
# Tags: race

# Regression test for pipeline stuck with window functions and numbers_mt.
# The ResizeProcessor in the scatter-gather topology could get stuck
# due to stale entries in its internal queues when outputs finish
# while the queue still holds references to them.
# https://github.com/ClickHouse/ClickHouse/issues/57728

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use 1000 rows (not 10000) and 20 iterations to keep total runtime under
# the test timeout even under sanitizers, while still exercising the resize
# processor topology.
OUTPUT=$(for _ in {1..20}; do
    echo "SELECT sum(a[length(a)]) FROM (SELECT groupArray(number) IGNORE NULLS OVER (PARTITION BY modulo(number, 11) ORDER BY modulo(number, 1111) ASC, number ASC) AS a FROM numbers_mt(1000)) SETTINGS max_block_size = 7;"
done | $CLICKHOUSE_CLIENT -n) || { echo "Client failed with exit code $?"; exit 1; }

MISMATCHES=$(echo "$OUTPUT" | grep -vcE '^499500$') || true

if [ "$MISMATCHES" -ne 0 ]; then
    echo "Fail: $MISMATCHES unexpected results"
    exit 1
fi

echo "OK"
