#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, long
# Serializing a composite value (Dynamic/Array/...) to text via toString runs a per-row loop that used to
# ignore the query's time and cancellation limits (they were only checked between pipeline blocks), so a thread
# kept serializing for a long time after KILL QUERY or max_execution_time and tripped the "Hung check failed,
# possible deadlock found" stress check. Two shapes are covered here:
#   1. many expensive rows in one block  - interrupted by the per-row QueryStatus check;
#   2. one row holding a single huge value - interrupted by a cancellation-checking WriteBuffer that observes the
#      limit on each buffer-sized flush of the bytes the (nested) serializer emits, since serializeText() of one
#      value never returns control to the per-row loop.
# The check honors KILL QUERY and max_execution_time in BOTH the `throw` and `break` timeout overflow modes.
# no-random-settings: the assertions are timing-based, so randomized limits must not interfere.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Everything below is derived from the measured natural (unlimited) run time, so the test adapts to the build
# type (debug/sanitizer are ~10x slower) without hard-coded seconds. FORMAT Null discards the output.

# check that a query stopped near its limit rather than running to natural completion. Fails loudly in both
# directions: below the lower bound (finished early - a randomized limit or unrelated failure) or above the
# midpoint between the limit and the natural time (ran to the end - cancellation not observed).
check_stopped_at_limit() {
    local query_id="$1" limit_s="$2" low_ms="$3" high_ms="$4" natural_ms="$5"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT if(
            query_duration_ms BETWEEN ${low_ms} AND ${high_ms},
            'ok',
            'FAIL: duration=' || toString(query_duration_ms) || 'ms limit=' || toString(${limit_s} * 1000) ||
            'ms natural=' || toString(${natural_ms}) || 'ms window=[' || toString(${low_ms}) || ',' ||
            toString(${high_ms}) || '] (cancellation not observed inside serialization)')
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

##########################################################################################
# Shape 1: one big block of many deeply nested Array values wrapped in Dynamic. Building the input is a small,
# fixed fraction of the run (~1/4); serializing it to text dominates and, without the per-row check, runs
# uninterrupted to the end.
##########################################################################################
value_expr="toString(arrayMap(z -> arrayMap(y -> range(y % 4), range(z % 7)), range(number % 20))::Dynamic)"
common_settings="max_block_size = 8000000, max_threads = 1, max_memory_usage = 0"

ref_id="04401_ref_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$ref_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null SETTINGS $common_settings" 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
NATURAL_MS=$($CLICKHOUSE_CLIENT -q "
    SELECT query_duration_ms FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$ref_id' AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC LIMIT 1")

# Limit at ~45% of the natural time: comfortably past input building (~25%), deep inside serialization.
LIMIT=$(( (NATURAL_MS * 45 / 100 + 999) / 1000 ))   # max_execution_time is in seconds; round up
LOW_MS=$(( LIMIT * 700 ))
HIGH_MS=$(( (LIMIT * 1000 + NATURAL_MS) / 2 ))

# throw mode: the time limit must surface as a TIMEOUT_EXCEEDED error raised from inside the serialization loop.
throw_id="04401_throw_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$throw_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null
    SETTINGS $common_settings, max_execution_time = $LIMIT, timeout_overflow_mode = 'throw'" 2>&1 \
    | grep -o -m1 "TIMEOUT_EXCEEDED"
check_stopped_at_limit "$throw_id" "$LIMIT" "$LOW_MS" "$HIGH_MS" "$NATURAL_MS"

# break mode: with many rows the limit stops serialization without an error (returns what is done so far). This
# is the mode a plain throwIfKilled() check misses, because CancellationChecker::cancelTask does not set
# is_killed for break mode; only checkTimeLimit() observes it.
break_id="04401_break_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$break_id" --query "
    SELECT $value_expr FROM numbers(8000000) FORMAT Null
    SETTINGS $common_settings, max_execution_time = $LIMIT, timeout_overflow_mode = 'break'"
echo "break exit: $?"
check_stopped_at_limit "$break_id" "$LIMIT" "$LOW_MS" "$HIGH_MS" "$NATURAL_MS"

##########################################################################################
# Shape 2: one row holding a single huge value (max_block_size = 1). The per-row check fires once, before the
# value, and cannot interrupt the serialization of the value itself; only the in-serializer WriteBuffer check
# can. Serializing this one value takes seconds. There is no useful partial result for a single value, so break
# mode is also turned into a hard TIMEOUT_EXCEEDED (like throw).
##########################################################################################
single_expr="toString(arrayMap(x -> arrayMap(y -> range(y % 8), range(x % 40)), range(6000000))::Dynamic)"
single_settings="max_block_size = 1, max_threads = 1, max_memory_usage = 0"

sref_id="04401_sref_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$sref_id" --query "
    SELECT $single_expr FROM numbers(1) FORMAT Null SETTINGS $single_settings" 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
SNATURAL_MS=$($CLICKHOUSE_CLIENT -q "
    SELECT query_duration_ms FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$sref_id' AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC LIMIT 1")

SLIMIT=$(( (SNATURAL_MS * 45 / 100 + 999) / 1000 ))
SLOW_MS=$(( SLIMIT * 700 ))
SHIGH_MS=$(( (SLIMIT * 1000 + SNATURAL_MS) / 2 ))

for mode in throw break; do
    sid="04401_single_${mode}_${CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query_id "$sid" --query "
        SELECT $single_expr FROM numbers(1) FORMAT Null
        SETTINGS $single_settings, max_execution_time = $SLIMIT, timeout_overflow_mode = '$mode'" 2>&1 \
        | grep -o -m1 "TIMEOUT_EXCEEDED"
    check_stopped_at_limit "$sid" "$SLIMIT" "$SLOW_MS" "$SHIGH_MS" "$SNATURAL_MS"
done
