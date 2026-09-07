#!/usr/bin/env bash
# Tags: long, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# arrayFold folds whole arrays inside a single executeImpl() call. The pipeline-level time check only
# runs between blocks, so a fold over a long array in one block cannot be interrupted by it. Without the
# in-function check this PR adds, such a fold ignores max_execution_time (the original report saw a fold
# run for 2709s after cancellation). The check stops it within max_execution_time instead.
#
# The signal is latency: WITH the fix every query below stops in ~1s; WITHOUT it the fold runs for tens of
# seconds (a result-growing arrayPushFront is O(N^2)), the `timeout` wrapper kills the client, and the
# output differs from the reference. range(120000) over numbers(4) keeps the whole fold in one block while
# holding only four growing accumulators, so the run stays well under 30 MiB (a single huge array would
# scatter into N shards and use multiple GiB, which is too heavy for the parallel flaky check).
FOLD="arrayFold((acc, x) -> arrayPushFront(acc, x), arr, emptyArrayUInt64())"

run() {
    # $1 = overflow mode, $2 = the "arr" expression, $3 = label
    local mode="$1" arr="$2" label="$3"
    if [ "$mode" = "throw" ]; then
        # throw mode: the in-function check throws TIMEOUT_EXCEEDED promptly.
        timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode throw \
            --query "SELECT $FOLD FROM (SELECT $arr AS arr FROM numbers(4)) FORMAT Null" 2>&1 \
            | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "$label throw: no timeout"
    else
        # break mode: checkTimeLimit() returns false instead of throwing. A half-fold has no meaningful
        # partial result, so the in-function check stops the fold; the pipeline absorbs the stop and the
        # query ends without a client-visible error. Before this PR the false return was discarded and the
        # fold kept running to completion.
        timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode break \
            --query "SELECT $FOLD FROM (SELECT $arr AS arr FROM numbers(4)) FORMAT Null" > /dev/null 2>&1 \
            && echo "$label break: stopped without error" || echo "$label break: unexpected failure"
    fi
}

# A materialized array forces the runtime (non-const) path.
run throw "range(materialize(toUInt64(120000)))" "runtime"
run break "range(materialize(toUInt64(120000)))" "runtime"

# A constant array argument. arrayFold is not subject to constant folding (it stays a runtime function
# node regardless of input size), so break does not diverge between analysis and execution: it is the same
# clean stop as the materialized path, not a surfaced error.
run throw "range(120000)" "const"
run break "range(120000)" "const"
