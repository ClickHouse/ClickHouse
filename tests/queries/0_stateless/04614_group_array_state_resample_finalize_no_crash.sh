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
# max_threads is pinned so the per-query memory peak (hence the throwing window) is deterministic.
seen_limit=0
for _ in {1..30}; do
    out=$($CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage 150000000 --max_rows_to_read 0 --query \
        "SELECT arrayMap(x -> finalizeAggregation(x), state)
         FROM (SELECT groupArrayStateResample(0, 1048576, 1)(number, number % 20) AS state FROM numbers(100000))
         FORMAT Null" 2>&1)
    [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]] && seen_limit=1
done
echo "resample hit memory limit: $seen_limit"

# The `-Tuple` combinator transfers one `-State` element at a time into separate subcolumns. When an
# element is itself a looping combinator (here `-Resample`), finalizing a later element can still throw
# after an earlier element's states are already aliased into its ColumnAggregateFunction subcolumn.
# Before the fix that produced the same double free during cleanup; the loop must survive here too.
seen_limit=0
for _ in {1..30}; do
    out=$($CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage 285000000 --max_rows_to_read 0 --query \
        "SELECT groupArrayStateResampleTuple(0, 1048576, 1)((number, number + 1), (number % 20, number % 20))
         FROM numbers(100000)
         FORMAT Null" 2>&1)
    [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]] && seen_limit=1
done
echo "resample tuple hit memory limit: $seen_limit"

# Same window through a transparent wrapper (`-If`) between `-Tuple` and the looping `-State` element:
# the wrapper forwards result insertion, so it must forward the reservation too, otherwise the nested
# reservation runs during the transfer and reopens the double-free window for a later tuple element.
seen_limit=0
for _ in {1..30}; do
    out=$($CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage 285000000 --max_rows_to_read 0 --query \
        "SELECT groupArrayStateResampleIfTuple(0, 1048576, 1)((number, number + 1), (number % 20, number % 20), (number > 0, number > 0))
         FROM numbers(100000)
         FORMAT Null" 2>&1)
    [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]] && seen_limit=1
done
echo "resample if tuple hit memory limit: $seen_limit"

# The server is still up: correct results are produced without a memory limit.
$CLICKHOUSE_CLIENT --query \
    "SELECT arrayMap(x -> finalizeAggregation(x), groupArrayStateResample(0, 5, 1)(number, number % 5)) FROM numbers(20)"
$CLICKHOUSE_CLIENT --query \
    "SELECT finalizeAggregation(tupleElement(t, 1)), finalizeAggregation(tupleElement(t, 2))
     FROM (SELECT sumStateTuple((number, number + 1)) AS t FROM numbers(10))"
