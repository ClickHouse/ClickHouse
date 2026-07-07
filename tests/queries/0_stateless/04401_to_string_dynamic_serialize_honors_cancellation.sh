#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-flaky-check, long
# Converting composite values (Dynamic/Array/Tuple/Map/Object/JSON ...) through text used to run per-row loops that
# ignored the query's time and cancellation limits (only checked between pipeline blocks), so a thread kept working
# for a long time after KILL QUERY or max_execution_time and tripped the "Hung check failed, possible deadlock
# found" stress check. The loops covered here:
#   - toString / CAST(... AS String): serialize each row into a String column (ConvertImplGenericToString);
#   - CAST(Tuple/Map/Object AS JSON): serialize each row to a JSON string, then parse it back into a JSON column
#     (ConvertImplGenericFromString) - the parse-back half dominates its cost.
# The failure shape covered: many expensive rows in one block, interrupted by the per-row QueryStatus check at the
# top of each loop.
# The check honors KILL QUERY and max_execution_time in BOTH the `throw` and `break` timeout overflow modes.
# The assertion is that the query stops NEAR its limit rather than running to natural completion: a plain
# `serverError TIMEOUT_EXCEEDED` does not distinguish fixed from unfixed, because the between-block check still
# throws TIMEOUT, only later (after the whole loop finishes). So we measure durations instead.
# no-random-settings / no-flaky-check: the assertion is timing-based (wall-clock latency), so randomized limits
# must not interfere and running it 50x under flaky-check validates nothing while risking a timeout.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# max_result_bytes = 0 disables the result-size limit that CI's default profile sets (these queries build large
# results before FORMAT Null discards them). max_memory_usage = 0 likewise. Everything is derived from the
# measured natural (unlimited) run time, so the test adapts to the build type (debug/sanitizer are slower) without
# hard-coded seconds.
COMMON="max_threads = 1, max_memory_usage = 0, max_result_bytes = 0"

# check that a query stopped near its limit rather than running to natural completion. Fails loudly in both
# directions: below the lower bound (finished early - a randomized limit or unrelated failure) or above the
# upper bound (ran to the end - cancellation not observed).
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

# measure the natural (unlimited) run time of one query, then rerun it with max_execution_time well below that (see
# the limit computation below) in both overflow modes and assert it stopped near the limit. A big block of many
# rows: break mode returns the partial result (exit 0) without an error.
# $1: unique id prefix   $2: SELECT expression   $3: number of rows
run_case() {
    local prefix="$1" expr="$2" rows="$3"
    local settings="max_block_size = ${rows}, $COMMON"   # force the whole input into one block

    local ref="04401_${prefix}_ref_${CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query_id "$ref" --query "
        SELECT $expr FROM numbers($rows) FORMAT Null SETTINGS $settings" 2>&1
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    local natural_ms
    natural_ms=$($CLICKHOUSE_CLIENT -q "
        SELECT query_duration_ms FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$ref' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1")

    # max_execution_time is an integer number of seconds, so pick the largest whole second that stays safely below
    # the natural time: floor(natural * 0.35). This keeps a wide margin (limit <= ~0.35 * natural) so run-to-run
    # variance cannot push a single execution past the limit and skip the timeout, while still landing well past the
    # cheap input-build phase.
    local limit=$(( natural_ms * 35 / 100 / 1000 ))
    [ "$limit" -lt 1 ] && limit=1
    local low_ms=$(( limit * 500 ))                           # >= half the limit: guards against instant failure
    # < 70% of the natural time: the query stopped rather than running to completion. A fixed fraction of natural
    # (not the midpoint to the limit) keeps the window wide enough to absorb the cancellation overshoot on slow
    # builds while still failing loudly if the query ran anywhere near its natural end.
    local high_ms=$(( natural_ms * 70 / 100 ))

    for mode in throw break; do
        local id="04401_${prefix}_${mode}_${CLICKHOUSE_DATABASE}"
        # throw raises TIMEOUT_EXCEEDED; break returns the partial result without error.
        $CLICKHOUSE_CLIENT --query_id "$id" --query "
            SELECT $expr FROM numbers($rows) FORMAT Null
            SETTINGS $settings, max_execution_time = $limit, timeout_overflow_mode = '$mode'" 2>&1 \
            | grep -o -m1 "TIMEOUT_EXCEEDED"
        # break returns the partial result without error - print its exit code to prove that.
        [ "$mode" = break ] && echo "break exit: ${PIPESTATUS[0]}"
        check_stopped_at_limit "$id" "$limit" "$low_ms" "$high_ms" "$natural_ms"
    done
}

# 1a. toString(... Dynamic), many rows in one block: building the input is a small fixed fraction (~1/4), the
#     per-row serialize loop dominates and, without the per-row check, runs uninterrupted to the end.
run_case "many" \
    "toString(arrayMap(z -> arrayMap(y -> range(y % 4), range(z % 7)), range(number % 20))::Dynamic)" \
    3000000

# 1b. CAST(Map AS JSON), many rows in one block: exercises the sibling serialize-then-parse-back loops one
#     conversion over (Map serializes to a JSON object that parses back). The per-row check in both loops must
#     interrupt it; the parse-back loop (ConvertImplGenericFromString) is the dominant half of this conversion.
run_case "json_many" \
    "CAST(map('id', number, 'vals', arrayMap(x -> x, range(number % 30))) AS JSON)" \
    2500000
