#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Finalizing a `-State` aggregate under a looping combinator (`-Resample`, and `-Resample` nested in
# `-Tuple` / a transparent `-If` wrapper) aliases the nested states into a `ColumnAggregateFunction`
# one at a time. A memory limit hit part-way through that transfer used to double free the
# already-transferred states (once by the aggregator cleanup, once by the result column), crashing the
# server. The queries below drive that transfer under a range of memory limits so the throw lands mid
# transfer on an unfixed server; a fixed server must stay up. Their output is intentionally discarded:
# whether a given limit trips `MEMORY_LIMIT_EXCEEDED` is not portable across sanitizers, so the
# regression signal is the deterministic tail below still running (i.e. the server survived), exactly
# like 01302_aggregate_state_exception_memory_leak.
for lim in 120000000 150000000 200000000; do
    $CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage $lim --max_rows_to_read 0 --query \
        "SELECT arrayMap(x -> finalizeAggregation(x), state)
         FROM (SELECT groupArrayStateResample(0, 1048576, 1)(number, number % 20) AS state FROM numbers(100000))
         FORMAT Null" >/dev/null 2>&1
done

for lim in 200000000 285000000 400000000; do
    $CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage $lim --max_rows_to_read 0 --query \
        "SELECT groupArrayStateResampleTuple(0, 1048576, 1)((number, number + 1), (number % 20, number % 20))
         FROM numbers(100000)
         FORMAT Null" >/dev/null 2>&1

    $CLICKHOUSE_CLIENT --max_threads 1 --max_memory_usage $lim --max_rows_to_read 0 --query \
        "SELECT groupArrayStateResampleIfTuple(0, 1048576, 1)((number, number + 1), (number % 20, number % 20), (number > 0, number > 0))
         FROM numbers(100000)
         FORMAT Null" >/dev/null 2>&1
done

# The server is still up: correct results are produced without a memory limit.
$CLICKHOUSE_CLIENT --query \
    "SELECT arrayMap(x -> finalizeAggregation(x), groupArrayStateResample(0, 5, 1)(number, number % 5)) FROM numbers(20)"
$CLICKHOUSE_CLIENT --query \
    "SELECT finalizeAggregation(tupleElement(t, 1)), finalizeAggregation(tupleElement(t, 2))
     FROM (SELECT sumStateTuple((number, number + 1)) AS t FROM numbers(10))"
