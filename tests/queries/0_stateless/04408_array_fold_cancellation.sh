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
# The signal differs by mode. In throw mode the fold raises TIMEOUT_EXCEEDED, which the grep below finds.
# In break mode the fold is cancelled and emits no rows; WITHOUT the fix it runs to completion (a
# result-growing arrayPushFront is O(N^2), ~7.6s here) and emits all four rows, so the break line diverges
# from the reference. Asserting the row count (not just the exit code) keeps break mode meaningful without
# a longer workload: the uncancelled fold finishes well under the outer `timeout`, so a regressed build
# would still exit 0. range(60000) over numbers(4) keeps the whole fold in one block while holding only
# four growing accumulators (peak < 20 MiB, parallel-safe).
#
# Keep the array short. The per-element setup before the fold's first cancellation check is
# uninterruptible; a longer array makes that setup rival max_execution_time, so under sanitizers the query
# can be killed by the outer `timeout` before the in-function check fires. A short array reaches the
# interruptible fold quickly while the O(N^2) work still runs far longer than the 1s limit.
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
        # query ends without a client-visible error but with no rows. Before this PR the false return was
        # discarded and the fold ran to completion, emitting all four rows. Assert the row count, not just
        # the exit code: a regressed build finishes under the outer `timeout` and would exit 0, but it
        # emits four rows here instead of none, diverging from the reference.
        local out rc rows
        out=$(timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode break \
            --query "SELECT length($FOLD) FROM (SELECT $arr AS arr FROM numbers(4))" 2>/dev/null)
        rc=$?
        rows=$(printf '%s' "$out" | grep -c .)
        if [ "$rc" -ne 0 ]; then
            echo "$label break: unexpected failure"
        elif [ "$rows" -eq 0 ]; then
            echo "$label break: stopped without error"
        else
            echo "$label break: ran to completion ($rows rows)"
        fi
    fi
}

# A materialized array forces the runtime (non-const) path.
run throw "range(materialize(toUInt64(60000)))" "runtime"
run break "range(materialize(toUInt64(60000)))" "runtime"

# A constant array argument. arrayFold is not subject to constant folding (it stays a runtime function
# node regardless of input size), so break does not diverge between analysis and execution: it is the same
# clean stop as the materialized path, not a surfaced error.
run throw "range(60000)" "const"
run break "range(60000)" "const"
