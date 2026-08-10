#!/usr/bin/env bash
# Tags: long, no-fasttest, no-flaky-check
# no-flaky-check: the kill assertion bounds a wall clock, and the flaky check runs many copies of one
# test on a single runner, where contention alone can exceed that bound.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Each of these four functions expands every row of a block inside one `executeImpl` call, and the
# pipeline only evaluates `max_execution_time` and `KILL QUERY` between blocks, so unfixed the whole
# block runs to completion however long that takes. Every case below runs at least four times the
# deadline unfixed while peaking under 55% of the 5G `max_memory_usage` the CI profile sets: a shape
# sized past memory would report `MEMORY_LIMIT_EXCEEDED` from the allocator instead.
#
# `max_block_size` and `max_threads` are pinned statement-level because the runner randomizes both.
# One block is what the fix is about: split into several, the executor's own between-blocks check
# bounds the query whatever the function does (measured, 18000 rows of `h3kRing`: one block ran
# 6415ms against a 1000ms deadline, eighteen blocks 1046ms). `max_execution_time` and
# `timeout_overflow_mode` are the oracle and are not randomized. `max_rows_to_read` and
# `max_result_bytes` are lifted because the CI profile caps them below what these row counts need.
LIMITS="max_threads = 1, max_memory_usage = 5000000000, max_rows_to_read = 0, max_result_bytes = 0"

# What a bound allows on top of its deadline, rather than a multiple of it, so shortening a deadline
# cannot tighten the assertion with it. A sanitizer or coverage build makes one throttle unit cost
# more, so the allowance scales; coverage never reaches CXX_FLAGS and has to be read from its own
# `system.build_options` row.
DEADLINE_MS=1000
# The row-loop bound is spent on CPU time (see `report_cpu`), which a descheduled thread does not
# accumulate, so it needs only the cost of one throttle unit and not a contention allowance.
CPU_SLACK_MS=1000
# The kill arm still bounds a wall clock, so it keeps the contention allowance and stays no tighter
# than the merged siblings `04648` (4000) and `04650` (4000 debug / 8000 sanitizer).
KILL_SLACK_MS=1500
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
CPU_BOUND=$((DEADLINE_MS + CPU_SLACK_MS * SCALE))
KILL_BOUND=$(((DEADLINE_MS + KILL_SLACK_MS * SCALE) * 2))
# `max_execution_time` is given in seconds; the deadline here is a whole number of milliseconds.
DEADLINE=$(printf '%d.%03d' "$((DEADLINE_MS / 1000))" "$((DEADLINE_MS % 1000))")

# Bound the query's CPU time rather than its wall clock. `max_execution_time` fires on the wall
# clock, but a shared runner can deschedule the thread for seconds, which inflates the wall clock
# without the row loop having run any longer: measured on one unfixed shape, contention moved the
# wall clock 4816ms -> 24227ms while its CPU time stayed at 4836ms. A missing row or a missing
# counter is reported rather than compared, so an absent measurement cannot read as a pass.
report_cpu() {
    local query_id="$1" label="$2"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT multiIf(
            rows = 0, '$label: no query log row',
            measured = 0, '$label: no cpu time measured',
            cpu_ms < $CPU_BOUND, '$label: stopped within bound',
            '$label: OVERSHOT ' || toString(cpu_ms) || ' ms')
        FROM (
            SELECT count() AS rows,
                   sum(mapContains(ProfileEvents, 'UserTimeMicroseconds')) AS measured,
                   intDiv(max(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']), 1000) AS cpu_ms
            FROM system.query_log
            WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart')"
}

# A query that never reached the row loop must not read as a pass: `CancellationChecker` can win the
# race against the in-function check and end the query without the function having been entered, and
# its message carries no elapsed part. That case is reported as inconclusive, so it is visible in the
# diff rather than silently satisfying the bound.
run_throw() {
    local label="$1" expr="$2" rows="$3"
    local query_id="04818_${label}_throw_${CLICKHOUSE_DATABASE}"
    local output
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time "$DEADLINE" --timeout_overflow_mode throw \
        -q "SELECT $expr FROM numbers($rows) FORMAT Null SETTINGS max_block_size = $rows, $LIMITS" 2>&1)
    if printf '%s' "$output" | grep -q 'elapsed'; then
        report_cpu "$query_id" "$label throw"
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label throw: no timeout"
    else
        echo "$label throw: cancelled before the pipeline started"
    fi
}

# In break mode `checkTimeLimit` returns false instead of throwing. A half-filled `Array(UInt64)` is
# a wrong value rather than a shorter one, so the call stops and the pipeline absorbs it into a clean
# finish, which leaves no message to read. The same CPU-time oracle is used for both modes.
run_break() {
    local label="$1" expr="$2" rows="$3"
    local query_id="04818_${label}_break_${CLICKHOUSE_DATABASE}"
    timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time "$DEADLINE" --timeout_overflow_mode break \
        -q "SELECT $expr FROM numbers($rows) FORMAT Null SETTINGS max_block_size = $rows, $LIMITS" > /dev/null 2>&1 \
        || echo "$label break: unexpected failure"
    report_cpu "$query_id" "$label break"
}

run_case() {
    run_throw "$1" "$2" "$3"
    run_break "$1" "$2" "$3"
}

# `h3kRing`: one row loop, sized by `maxGridDiskSize` then filled by `gridDisk`. A small ring over
# many rows rather than a large one over few: the per-row array is what the checkpoint's throttle
# counts, so few coarse rows make the stop time swing with where the last row lands (7 items a row is
# stable to 2%, 7651 spread over 6x).
KRING="h3kRing(materialize(644325529233966508), 1)"
run_case kring "$KRING" 16000000

# `h3HexRing` is two-pass. Only the fill loop needs a checkpoint; see the comment on its sizing loop.
# `gridRingUnsafe` is the cheapest per item of the four, so this case has the least room: at 55% of
# the memory cap no `k` reaches more than about five seconds unfixed.
HEXRING="h3HexRing(materialize(644325529233966508), toUInt16(20))"
run_case hexring "$HEXRING" 2230000

# `h3Line` is two-pass as well, and unlike `h3HexRing` BOTH its loops carry the check.
# This case is dominated by the fill loop (`gridPathCells`).
LINE="h3Line(materialize(621807531097128959), 622053654978461695)"
run_case line "$LINE" 2000

# The sizing loop on its own: at distance 0 the fill loop writes one item per row, so all the time is
# in `gridPathCellsSize`, which walks the grid and is not free even for that distance. Without a
# checkpoint of its own the sizing pass alone overshoots by an order of magnitude, so this case is
# what pins that `h3Line` needs two checkpoints and not one.
LINE_SIZING="h3Line(materialize(621807531097128959), 621807531097128959)"
run_case line_sizing "$LINE_SIZING" 50000000

# `h3ToChildren`: one row loop, sized by `cellToChildrenSize` then filled by `cellToChildren`. A res-9
# parent expanded to res 10 is 7 items per row, the cheapest items-per-byte of the four.
TOCHILDREN="h3ToChildren(materialize(617303931469955071), 10)"
run_case tochildren "$TOCHILDREN" 18000000

# `KILL` takes the same in-thread path: `checkTimeLimit` consults `is_killed`. The oracle is the wall
# clock of `KILL ... SYNC` from the query log, a different query floored by its own 100ms poll loop
# and round-trips rather than by the row loop, so it keeps a wall-clock bound and the siblings'
# contention allowance rather than the CPU-time bound the row-loop cases use.
run_kill_case() {
    local label="$1" expr="$2" rows="$3"
    local query_id="04818_kill_${label}_${CLICKHOUSE_DATABASE}"
    local err="${CLICKHOUSE_TMP}/04818_${label}_${CLICKHOUSE_DATABASE}_err.txt"

    ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        -q "SELECT $expr FROM numbers($rows) FORMAT Null SETTINGS max_block_size = $rows, $LIMITS" > /dev/null 2>"$err" &
    local query_pid=$!

    # Killing a query id that never started also returns promptly, so the readiness poll must report
    # its own failure: otherwise a setup failure would print the same line as a genuine prompt
    # cancellation and the test would pass without exercising the fix. Waiting for the query to have
    # been running rather than merely being visible matters too: `ProcessList` makes it visible before
    # the executor is attached, and `addPipelineExecutor` raises a pending cancellation itself, so a
    # kill winning that race would pass even unfixed.
    # The poll runs over HTTP rather than through the client: a fresh client process costs ~70ms per
    # iteration against ~10ms for a request, and that overhead lands inside the measured kill latency.
    local waited=0 ready_ok=0
    while [ "$waited" -lt 500 ]; do
        if [ "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT max(elapsed) > 1 FROM system.processes WHERE query_id = '$query_id'")" = "1" ]; then
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

    # Over zero rows `max` returns 0, which is below any bound, so the row has to be counted: an
    # absent log row would otherwise print the same line as a genuinely prompt cancellation.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT multiIf(
            count() = 0, '$label kill: no query log row',
            max(query_duration_ms) < $KILL_BOUND, '$label kill returned promptly',
            '$label kill blocked ' || toString(max(query_duration_ms)) || ' ms')
        FROM system.query_log
        WHERE query_id = '${query_id}_sync' AND current_database = currentDatabase() AND type != 'QueryStart'"
}

# One second of the block's twelve leaves the rest of it as the window in which the kill has to land.
run_kill_case line "$LINE" 2000

# Liveness controls with no time limit: each function must still return its documented result, so a
# "fix" that simply always threw would fail here rather than pass.
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(h3kRing(644325529233966508, 1))"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(h3HexRing(644325529233966508, toUInt16(1)))"
${CLICKHOUSE_CLIENT} -q "SELECT h3Line(621807531097128959, 621807531097128959)"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(h3ToChildren(617303931469955071, 10))"

# The controls above stay below the throttle; these cross it, so the check provably runs at least once
# with no deadline set and must return cleanly. `h3Line` gets two, one per loop.
${CLICKHOUSE_CLIENT} -q "SELECT sum(length($KRING)) FROM numbers(20000) SETTINGS max_block_size = 20000, $LIMITS"
${CLICKHOUSE_CLIENT} -q "SELECT sum(length($HEXRING)) FROM numbers(1000) SETTINGS max_block_size = 1000, $LIMITS"
${CLICKHOUSE_CLIENT} -q "SELECT sum(length($LINE)) FROM numbers(20) SETTINGS max_block_size = 20, $LIMITS"
${CLICKHOUSE_CLIENT} -q "SELECT sum(length($LINE_SIZING)) FROM numbers(200000) SETTINGS max_block_size = 200000, $LIMITS"
${CLICKHOUSE_CLIENT} -q "SELECT sum(length($TOCHILDREN)) FROM numbers(20000) SETTINGS max_block_size = 20000, $LIMITS"

# Degenerate rows: an invalid cell returns an empty array without ever reaching a size, so these cross
# the same checkpoint only because a row counts as one work unit on its own. Only the RESULT is
# asserted here: that floor admits no latency oracle, because removing it leaves a block of such rows
# running 1.86 seconds where the smallest bound above already allows 2.0. It is pinned instead by
# `src/Functions/tests/gtest_h3_array_expansion_cancellation.cpp`, whose oracle is whether the check
# fires at all rather than how long the block took.
DEGENERATE="max_block_size = 1000000, functions_h3_default_if_invalid = 1, $LIMITS"
BAD="materialize(toUInt64(1))"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(h3kRing($BAD, 100))) FROM numbers(1000000) SETTINGS $DEGENERATE"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(h3HexRing($BAD, toUInt16(100)))) FROM numbers(1000000) SETTINGS $DEGENERATE"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(h3Line($BAD, $BAD))) FROM numbers(1000000) SETTINGS $DEGENERATE"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(h3ToChildren($BAD, 9))) FROM numbers(1000000) SETTINGS $DEGENERATE"

# A call inside a stored expression, one per function: the instance built while a key, index or TTL is
# analysed is kept for the table's lifetime and executed by unrelated later queries, so the query to
# check is the one running the call and not the one that defined it. Resolving it from the executing
# thread covers this; a constructor-captured element would be the wrong deadline and, held alive, a
# permanent `CurrentMetrics::QueryNonInternal` leak. All four are here because that resolution is
# implemented separately in each of the four files, so one case would leave three of them unpinned.
# The rows must arrive in one block, or the pipeline's own check bounds the INSERT whatever the
# function does, and every distinct array length is a partition. A row count alone does not buy that
# block: the squasher also flushes on `min_insert_block_size_bytes`, whose 256MiB default caps a
# UInt64 column at 33550368 rows, so it is pinned here to the size of the block being asked for.
# $1 = label, $2 = the partition expression over the column `c`, $3 = the cell to insert, $4 = rows
run_stored_case() {
    local label="$1" partition_expr="$2" cell="$3" rows="$4"
    local table="t04818_stored_${label}"
    local query_id="04818_stored_${label}_${CLICKHOUSE_DATABASE}"
    local output
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS $table SYNC"
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE $table (c UInt64) ENGINE = MergeTree
        PARTITION BY length($partition_expr) ORDER BY tuple()
        SETTINGS max_partitions_per_insert_block = 0"
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time "$DEADLINE" --timeout_overflow_mode throw \
        -q "INSERT INTO $table SELECT $cell FROM numbers($rows)
            SETTINGS max_insert_block_size = $rows, min_insert_block_size_rows = $rows,
                     min_insert_block_size_bytes = $((rows * 8 + 1024)), $LIMITS" 2>&1)
    if printf '%s' "$output" | grep -q 'elapsed'; then
        report_cpu "$query_id" "stored $label"
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "stored $label: no timeout"
    else
        echo "stored $label: cancelled before the pipeline started"
    fi
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE $table SYNC"
}

run_stored_case kring      "h3kRing(c, 1)"              644325529233966508 16000000
run_stored_case hexring    "h3HexRing(c, toUInt16(20))" 644325529233966508  2230000
run_stored_case line       "h3Line(c, c)"               621807531097128959 16000000
run_stored_case tochildren "h3ToChildren(c, 10)"        617303931469955071 18000000
