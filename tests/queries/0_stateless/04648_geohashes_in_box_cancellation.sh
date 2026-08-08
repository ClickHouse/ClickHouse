#!/usr/bin/env bash
# Tags: long, no-fasttest, no-flaky-check
# no-flaky-check: every assertion is a cancellation latency, and the flaky check runs many copies of
# one test on a single runner; for the cases whose oracle is a fixed number of milliseconds, that
# contention overlaps the unfixed distribution, so no bound both passes fixed and fails unfixed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every case below expands one box over a whole block, and such a block runs for ten seconds or more
# against a one second deadline, so the cancellation lands inside the row loop however the load moves
# the absolute numbers. Rows that expand to nothing are the opposite shape: they cost a few hundred
# milliseconds per gigabyte of block, so a block of them can only outlast a deadline by outgrowing
# the memory the job has, and a deadline that fits in memory lands before or after the row loop as
# often as inside it. That is what made the two `KILL` cases on reversed and NaN bounds flaky, so
# their promptness is no longer asserted; those boxes are kept in the liveness checks at the end.
#
# `max_block_size` and `max_threads` are pinned statement-level because the runner randomizes both and
# the effect size is a function of rows-per-block: at one row per block the limit is honoured even
# unfixed. `max_execution_time` and `timeout_overflow_mode` are the oracle and are not randomized.
SETTINGS="max_block_size = 60000, max_threads = 1"
BOX="materialize(0.0), materialize(0.0), 0.0000106, 0.0000053, toUInt8(12)"
BOX_F32="materialize(toFloat32(0.0)), materialize(toFloat32(0.0)), toFloat32(0.0000106), toFloat32(0.0000053), toUInt8(12)"
BOX_DEGENERATE="1.0, 1.0, 0.0, 0.0, materialize(toUInt8(12))"
BOX_NAN="nan, 1.0, 0.0, 0.0, materialize(toUInt8(12))"

# 4x the 1s limit absorbs clock granularity, a busy runner and the work in flight when the check
# fires; the unfixed path lands far above that.
check_duration() {
    local query_id="$1" label="$2"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT if(max(query_duration_ms) < 4000, '$label stopped promptly', '$label ran ' || toString(max(query_duration_ms)) || 'ms past a 1000ms limit')
        FROM system.query_log
        WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart'"
}

run_case() {
    # $1 = overflow mode, $2 = coordinate expression, $3 = label
    local mode="$1" box="$2" label="$3"
    local query_id="04648_${label}_${mode}_${CLICKHOUSE_DATABASE}"

    if [ "$mode" = "throw" ]; then
        ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time 1 --timeout_overflow_mode throw \
            -q "SELECT geohashesInBox($box) FROM numbers(60000) FORMAT Null SETTINGS $SETTINGS" 2>&1 \
            | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "$label throw: no timeout"
    else
        # In break mode `checkTimeLimit` returns false instead of throwing. A half-filled
        # `Array(String)` is a wrong value rather than a smaller one, so the call stops and the
        # pipeline absorbs it; unfixed, the false return was discarded and the block finished.
        ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time 1 --timeout_overflow_mode break \
            -q "SELECT geohashesInBox($box) FROM numbers(60000) FORMAT Null SETTINGS $SETTINGS" > /dev/null 2>&1 \
            && echo "$label break: stopped without error" || echo "$label break: unexpected failure"
    fi
    check_duration "$query_id" "$label $mode"
}

# Float64 coordinates: the execute<Float64, UInt8> instantiation.
run_case throw "$BOX" "float64"
run_case break "$BOX" "float64"

# `Float32` coordinates select the other instantiation at the `isFloat32` dispatch; the row loop
# carrying the check is shared, so this pins that both are covered.
run_case throw "$BOX_F32" "float32"

# `KILL` takes the same in-thread path (`checkTimeLimit` consults `is_killed`) and is the half the
# report showed. The oracle is how long `KILL ... SYNC` blocks, so no time limit is involved.
run_kill_case() {
    # $1 = label, $2 = argument expression, $3 = rows, $4 = settings, $5 = readiness predicate,
    # $6 = bound in ms for how long `KILL ... SYNC` may block
    local label="$1" box="$2" rows="$3" settings="$4" ready="$5" bound="$6"
    local query_id="04648_kill_${label}_${CLICKHOUSE_DATABASE}"
    local err="${CLICKHOUSE_TMP}/04648_${label}_${CLICKHOUSE_DATABASE}_err.txt"

    ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        -q "SELECT geohashesInBox($box) FROM numbers($rows) FORMAT Null SETTINGS $settings" > /dev/null 2>"$err" &
    local query_pid=$!

    # Killing a query id that never started also returns promptly, so the readiness poll must report
    # its own failure: otherwise a setup failure would print the same line as a genuine prompt
    # cancellation and the test would pass without exercising the fix.
    # The poll runs over HTTP rather than through the client: a fresh client process costs ~70ms per
    # iteration against ~10ms for a request, and that overhead lands inside the measured kill latency.
    local waited=0 ready_ok=0
    while [ "$waited" -lt 500 ]; do
        if [ "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT $ready FROM system.processes WHERE query_id = '$query_id'")" = "1" ]; then
            ready_ok=1
            break
        fi
        waited=$((waited + 1))
        sleep 0.02
    done

    if [ "$ready_ok" = "0" ]; then
        echo "$label kill: query never reached the row loop"
        cat "$err"
    fi

    ${CLICKHOUSE_CLIENT} --query_id "${query_id}_sync" -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
    wait $query_pid 2>/dev/null
    rm -f "$err"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT if(max(query_duration_ms) < $bound, '$label kill returned promptly', '$label kill blocked ' || toString(max(query_duration_ms)) || 'ms')
        FROM system.query_log
        WHERE query_id = '${query_id}_sync' AND current_database = currentDatabase() AND type != 'QueryStart'"
}

# Readiness waits for the query to have been running rather than merely being visible: `ProcessList`
# makes it visible before the executor is attached, and `addPipelineExecutor` raises a pending
# cancellation itself, so a kill winning that race would pass even unfixed. One second of the block's
# tens leaves the rest of it as the window in which the kill has to land. 4000ms as above.
run_kill_case "expanding" "$BOX" 60000 "$SETTINGS" "max(elapsed) > 1" 4000

# Liveness control: with no time limit the function must still return the documented result, so a
# "fix" that simply always threw would fail here rather than pass.
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4)"

# The case above stays below the throttle; this one crosses it, so the check runs at least once with
# no deadline set and must return cleanly.
${CLICKHOUSE_CLIENT} -q "SELECT sum(length(geohashesInBox($BOX))) FROM numbers(2000) SETTINGS max_block_size = 2000, max_threads = 1"

# Reversed and NaN bounds return no items at all, so they cross the same checkpoint only because a
# row counts as a work unit on its own, and they must still return their documented empty arrays.
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(geohashesInBox($BOX_DEGENERATE))) FROM numbers(2000000) SETTINGS max_block_size = 2000000, max_threads = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(geohashesInBox($BOX_NAN))) FROM numbers(2000000) SETTINGS max_block_size = 2000000, max_threads = 1"
