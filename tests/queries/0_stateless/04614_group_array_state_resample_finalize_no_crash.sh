#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Finalizing a `-State` aggregate under the `-Resample` combinator aliases the nested states into a
# `ColumnAggregateFunction` one at a time. `groupArrayStateResample(0, 1048576, 1)` allocates ~1M empty
# buckets cheaply, so memory climbs during the finalization transfer; a memory limit set just below
# the peak makes the transfer throw part-way. Before the fix the already-transferred states were
# freed both by the aggregator and by the result column (double free / server crash). The loop below
# hits that window repeatedly; the server must survive and just report `MEMORY_LIMIT_EXCEEDED`.

for _ in {1..30}; do
    $CLICKHOUSE_CLIENT --max_memory_usage 150000000 --max_rows_to_read 0 --query \
        "SELECT arrayMap(x -> finalizeAggregation(x), state)
         FROM (SELECT groupArrayStateResample(0, 1048576, 1)(number, number % 20) AS state FROM numbers(100000))
         FORMAT Null" 2>&1 | grep -o -F -m1 'MEMORY_LIMIT_EXCEEDED' | head -n1
done > /dev/null

# The server is still up: correct results are produced without a memory limit.
$CLICKHOUSE_CLIENT --query \
    "SELECT arrayMap(x -> finalizeAggregation(x), groupArrayStateResample(0, 5, 1)(number, number % 5)) FROM numbers(20)"
