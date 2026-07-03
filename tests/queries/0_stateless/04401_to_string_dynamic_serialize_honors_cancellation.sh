#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, long
# Serializing a composite value (Dynamic/Array/...) to text via toString runs a per-row loop that used to
# ignore the query's time and cancellation limits (they were only checked between pipeline blocks). A single
# large block of such values kept a thread serializing for a long time after KILL QUERY or max_execution_time,
# tripping the "Hung check failed, possible deadlock found" stress check. The loop now calls
# QueryStatus::checkTimeLimit() per row, which honors KILL QUERY and max_execution_time in BOTH the `throw` and
# `break` timeout overflow modes. no-random-settings: the assertion is timing-based, so randomized limits must
# not interfere.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# One big block (max_block_size covers all rows) of deeply nested Array values wrapped in Dynamic. Building the
# input is a small, fixed fraction of the run (~1/4); serializing it to text dominates and, without the per-row
# check, runs uninterrupted to the end. FORMAT Null discards the output. Everything below is derived from the
# measured natural (unlimited) run time, so the test adapts to the build type (debug/sanitizer are ~10x slower)
# without hard-coded seconds.
value_expr="toString(arrayMap(z -> arrayMap(y -> range(y % 4), range(z % 7)), range(number % 20))::Dynamic)"
common_settings="max_block_size = 8000000, max_threads = 1, max_memory_usage = 0"

# Reference: natural (unlimited) run time = input building + full serialization.
ref_id="04401_ref_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$ref_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null SETTINGS $common_settings" 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
NATURAL_MS=$($CLICKHOUSE_CLIENT -q "
    SELECT query_duration_ms FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$ref_id' AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC LIMIT 1")

# Limit at ~45% of the natural time: comfortably past input building (~25%), deep inside serialization. With the
# fix the query stops at ~LIMIT; without it, the whole block is serialized first and it runs for ~NATURAL_MS.
LIMIT=$(( (NATURAL_MS * 45 / 100 + 999) / 1000 ))   # max_execution_time is in seconds; round up
# Accept anything between "clearly reached serialization" (>= 70% of the limit) and the midpoint between the
# limit and the natural time. The fix lands near LIMIT (well under the midpoint); a regression runs to
# ~NATURAL_MS (well over the midpoint); an early failure / randomized limit stops it below the lower bound. All
# three are distinguished, and the midpoint gives ~1/4*NATURAL_MS of slack on each side.
LOW_MS=$(( LIMIT * 700 ))
HIGH_MS=$(( (LIMIT * 1000 + NATURAL_MS) / 2 ))

check_stopped_at_limit() {
    local query_id="$1"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT if(
            query_duration_ms BETWEEN ${LOW_MS} AND ${HIGH_MS},
            'ok',
            'FAIL: duration=' || toString(query_duration_ms) || 'ms limit=' || toString(${LIMIT} * 1000) ||
            'ms natural=' || toString(${NATURAL_MS}) || 'ms window=[' || toString(${LOW_MS}) || ',' ||
            toString(${HIGH_MS}) || '] (cancellation not observed inside serialization)')
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

# throw mode: the time limit must surface as a TIMEOUT_EXCEEDED error raised from inside the serialization loop.
throw_id="04401_throw_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$throw_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null
    SETTINGS $common_settings, max_execution_time = $LIMIT, timeout_overflow_mode = 'throw'" 2>&1 \
    | grep -o -m1 "TIMEOUT_EXCEEDED"
check_stopped_at_limit "$throw_id"

# break mode: the time limit must stop serialization without an error (returns what is done so far). This is the
# mode a plain throwIfKilled() check misses, because CancellationChecker::cancelTask does not set is_killed for
# break mode; only checkTimeLimit() observes it.
break_id="04401_break_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$break_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null
    SETTINGS $common_settings, max_execution_time = $LIMIT, timeout_overflow_mode = 'break'"
echo "break exit: $?"
check_stopped_at_limit "$break_id"
