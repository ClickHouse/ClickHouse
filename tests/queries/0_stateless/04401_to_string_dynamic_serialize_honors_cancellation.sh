#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, long
# Converting composite values (Dynamic/Array/Tuple/Map/Object/JSON ...) through text used to run per-row loops that
# ignored the query's time and cancellation limits (only checked between pipeline blocks), so a thread kept working
# for a long time after KILL QUERY or max_execution_time and tripped the "Hung check failed, possible deadlock
# found" stress check. The loops covered here:
#   - toString / CAST(... AS String): serialize each row into a String column (ConvertImplGenericToString);
#   - CAST(Tuple/Map/Object AS JSON): serialize each row to a JSON string, then parse it back into a JSON column
#     (ConvertImplGenericFromString) - the parse-back half dominates its cost.
# Two failure shapes:
#   1. many expensive rows in one block  - interrupted by the per-row QueryStatus check at the top of each loop;
#   2. one row holding a single huge value - the per-row check fires once before the value, so serialization is
#      interrupted mid-value by a cancellation-checking WriteBuffer that observes the limit on each buffer-sized
#      flush of the bytes the (nested) serializer emits.
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
            toString(${high_ms}) || '] (cancellation not observed inside conversion)')
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

# measure the natural (unlimited) run time of one query, then rerun it with max_execution_time at ~45% of that in
# both overflow modes and assert it stopped near the limit. $3 selects the shape:
#   many   - a big block of many rows; break mode returns the partial result (exit 0) without an error;
#   single - one row holding a huge value; break mode has no useful partial result so it is a hard TIMEOUT_EXCEEDED.
# $1: unique id prefix   $2: SELECT expression   $3: many|single   $4: number of rows
run_case() {
    local prefix="$1" expr="$2" shape="$3" rows="$4"
    local settings="max_threads = 1, max_memory_usage = 0"
    if [ "$shape" = single ]; then
        settings="max_block_size = 1, $settings"
    else
        settings="max_block_size = ${rows}, $settings"   # force the whole input into one block
    fi

    local ref="04401_${prefix}_ref_${CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query_id "$ref" --query "
        SELECT $expr FROM numbers($rows) FORMAT Null SETTINGS $settings" 2>&1
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    local natural_ms
    natural_ms=$($CLICKHOUSE_CLIENT -q "
        SELECT query_duration_ms FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$ref' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1")

    local limit=$(( (natural_ms * 45 / 100 + 999) / 1000 ))   # max_execution_time is in seconds; round up
    local low_ms=$(( limit * 700 ))
    local high_ms=$(( (limit * 1000 + natural_ms) / 2 ))

    for mode in throw break; do
        local id="04401_${prefix}_${mode}_${CLICKHOUSE_DATABASE}"
        # throw always raises TIMEOUT_EXCEEDED; break raises it only for the single-value shape (no partial result).
        $CLICKHOUSE_CLIENT --query_id "$id" --query "
            SELECT $expr FROM numbers($rows) FORMAT Null
            SETTINGS $settings, max_execution_time = $limit, timeout_overflow_mode = '$mode'" 2>&1 \
            | grep -o -m1 "TIMEOUT_EXCEEDED"
        # For many rows, break returns the partial result without error - print its exit code to prove that.
        [ "$shape" = many ] && [ "$mode" = break ] && echo "break exit: ${PIPESTATUS[0]}"
        check_stopped_at_limit "$id" "$limit" "$low_ms" "$high_ms" "$natural_ms"
    done
}

# 1a. toString(... Dynamic), many rows in one block: building the input is a small fixed fraction (~1/4), the
#     per-row serialize loop dominates and, without the per-row check, runs uninterrupted to the end.
run_case "many" \
    "toString(arrayMap(z -> arrayMap(y -> range(y % 4), range(z % 7)), range(number % 20))::Dynamic)" \
    many 8000000

# 1b. CAST(Map AS JSON), many rows in one block: exercises the sibling serialize-then-parse-back loops one
#     conversion over (Map serializes to a JSON object that parses back). The per-row check in both loops must
#     interrupt it; the parse-back loop (ConvertImplGenericFromString) is the dominant half of this conversion.
run_case "json_many" \
    "CAST(map('id', number, 'vals', arrayMap(x -> x, range(number % 30))) AS JSON)" \
    many 6000000

# 2. one row holding a single huge value (max_block_size = 1). The per-row check fires once, before the value, and
#    cannot interrupt serializing the value itself; only the in-serializer WriteBuffer check can. There is no
#    useful partial result for a single value, so break mode is also a hard TIMEOUT_EXCEEDED (like throw).
run_case "single" \
    "toString(arrayMap(x -> arrayMap(y -> range(y % 8), range(x % 40)), range(6000000))::Dynamic)" \
    single 1
