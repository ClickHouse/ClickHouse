#!/usr/bin/env bash
# Tags: long, no-fasttest, no-coverage, no-flaky-check
# no-coverage: per-test coverage instrumentation stretches the fixed side of the timed cases.
# no-flaky-check: the cases verify timeout-based behavior, not suitable for rerun-based detection.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Each timed case puts over a second of work inside ONE function call and bounds the elapsed time the
# server reports. The pins below keep one call per block, so the executor's between-block check cannot
# bound the query instead of the function. Every case keeps at least 3.5x the bound unfixed, and the
# bound scales for sanitizer and coverage builds, which stretch both sides.
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
BOUND=$((SCALE * 3500))

PINS="max_threads = 1, preferred_block_size_bytes = 0"
# 11 MB with a match every 11 bytes: 1M matches inside a single row.
BIG="repeat('\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0a', 1000000)"

# The runner randomizes parallel replicas into every client call, and a `system` table read under it wants
# a cluster a single-node server may not have.
LOG_PINS="--enable_parallel_replicas 0"

# $1 = label, $2 = query, $3 = overflow mode (default "throw")
#
# A duration alone cannot tell a stopped call from a query that never ran, so the verdict asserts the
# termination cause too. Only `ExecutionSpeedLimits::checkTimeLimit` formats "elapsed N ms"; a kill landing
# before the pipeline starts omits it, and such a run is reported as inconclusive rather than as a pass.
run() {
    local label="$1" query="$2" mode="${3:-throw}"
    local query_id="04722_${label}_${CLICKHOUSE_DATABASE}" output elapsed_ms code rows breaks
    # `log_profile_events` is pinned: the `break` verdict reads a counter out of the `query_log` row.
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time 1 \
        --log_profile_events 1 --timeout_overflow_mode "$mode" --query "$query" 2>&1)

    if [ "$mode" = break ]; then
        # Under `break` the pipeline turns the throw into a graceful finish, so the client sees no error and
        # the cause is `ProfileEvents['OverflowBreak']`. The row count is read too: `max` over no rows
        # returns 0, which would otherwise be below the bound and score as a pass.
        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        # shellcheck disable=SC2086
        read -r rows elapsed_ms breaks < <(${CLICKHOUSE_CLIENT} $LOG_PINS --query "
            SELECT count(), max(query_duration_ms), max(ProfileEvents['OverflowBreak']) FROM system.query_log
            WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart'")
        if [ "${rows:-0}" = 0 ]; then
            echo "$label: NO QUERY LOG ROW"
        elif [ "${breaks:-0}" = 0 ]; then
            # A fast failure that never reached the deadline, e.g. an out-of-memory.
            echo "$label: DEADLINE NEVER OBSERVED (no OverflowBreak)"
        elif [ "$elapsed_ms" -lt "$BOUND" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
        return
    fi

    if ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label: NOT STOPPED BY THE DEADLINE: $(printf '%s' "$output" | grep -oE 'Code: [0-9]+' | head -1)"
        return
    fi

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    # shellcheck disable=SC2086
    code=$(${CLICKHOUSE_CLIENT} $LOG_PINS --query "
        SELECT max(exception_code) FROM system.query_log
        WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart'")
    elapsed_ms=$(printf '%s' "$output" | grep -oP 'elapsed \K[0-9]+(?=\.)' | head -1)

    if [ -n "$code" ] && [ "$code" != 159 ]; then
        echo "$label: WRONG EXCEPTION CODE $code"
    elif [ -z "$elapsed_ms" ]; then
        echo "$label: cancelled before the pipeline started"
    elif [ "$elapsed_ms" -lt "$BOUND" ]; then
        echo "$label: stopped within bound"
    else
        echo "$label: OVERSHOT ${elapsed_ms} ms"
    fi
}

# 1. `constantVector`, the shape the hung-check report showed: a constant haystack with a materialized
#    needle, so the all-constant fold does not apply and every row re-scans the whole 11 MB value.
run "constant_vector" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8($BIG, materialize('a'))) FROM numbers(1000) FORMAT Null SETTINGS max_block_size = 1000, $PINS"

# 2. `vectorVector`, both arguments materialized. The needle repeatedly almost matches, so the searcher
#    re-examines the row without advancing a match and only the per-row charge can stop it.
run "vector_vector" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8(materialize(repeat('Ж', 200)), materialize(repeat('Ж', 16) || 'Щ'))) FROM numbers(260000) FORMAT Null SETTINGS max_block_size = 260000, $PINS"

# 3. `vectorVector` with the cost inside its inner match loop instead: one row of many matches, so the
#    per-row charge is spent at row entry and only the per-match charge can stop the rest of the row.
run "vector_vector_inner" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8(materialize(repeat(repeat('Ж', 200) || 'Щ', 260000)), materialize(repeat('Ж', 16) || 'Щ'))) FROM numbers(1) FORMAT Null SETTINGS max_block_size = 1, $PINS"

# 4. `vectorConstant`, a materialized haystack with a constant needle. Its charge is per MATCH, so every
#    row here ends in one match and the bytes before it are what the searcher works through.
run "vector_constant" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8(materialize(repeat('Ж', 200) || 'Щ'), repeat('Ж', 16) || 'Щ')) FROM numbers(260000) FORMAT Null SETTINGS max_block_size = 260000, $PINS"

# 5. Rows with no match at all, so the inner search loop is never entered and only the per-row charge can
#    stop the query.
run "no_match_rows" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8(repeat('Ж', 1000000), materialize('ЩЩ'))) FROM numbers(1600) FORMAT Null SETTINGS max_block_size = 1600, $PINS"

# 6. `countSubstrings`, a distinct instantiation over `CaseSensitiveStringSearcher`.
run "case_sensitive" \
    "SELECT sum(countSubstrings(repeat('a', 1000000), materialize('a'))) FROM numbers(1800) FORMAT Null SETTINGS max_block_size = 1800, $PINS"

# 7. `countSubstringsCaseInsensitive`, the third instantiation, over `ASCIICaseInsensitiveStringSearcher`.
run "case_insensitive" \
    "SELECT sum(countSubstringsCaseInsensitive(repeat('a', 1000000), materialize('A'))) FROM numbers(2200) FORMAT Null SETTINGS max_block_size = 2200, $PINS"

# 8. The `break` overflow mode, where `checkTimeLimit` returns false instead of throwing, so a path that
#    never calls it ignores the deadline entirely.
run "constant_vector_break" \
    "SELECT sum(countSubstringsCaseInsensitiveUTF8($BIG, materialize('a'))) FROM numbers(1000) FORMAT Null SETTINGS max_block_size = 1000, $PINS" \
    break

# 9. `KILL QUERY`, the channel the report's `is_cancelled = 1` came from: it sets the killed flag and
#    surfaces through a separate branch of the same check. No time limit is set, so only the kill can stop
#    the query, and what is asserted is how long the synchronous kill blocks. It carries its own bound
#    because the two sides are three orders of magnitude apart here.
KILL_BOUND=$((SCALE * 8000))
kill_id="04722_kill_${CLICKHOUSE_DATABASE}"
# The row count is far above what the assertion needs: the target has to still be running when the poll
# below observes it, and one poll is a whole client invocation. Only the fixed side runs it to the end.
${CLICKHOUSE_CLIENT} --query_id "$kill_id" \
    --query "SELECT sum(countSubstringsCaseInsensitiveUTF8($BIG, materialize('a'))) FROM numbers(2500) FORMAT Null SETTINGS max_block_size = 2500, $PINS" \
    > /dev/null 2>&1 &
kill_bg=$!

# Waiting for the query to be RUNNING rather than merely visible: `ProcessList` makes it visible before the
# executor is attached, and a kill winning that race would pass even unfixed.
kill_seen=0
for _ in $(seq 1 200); do
    # shellcheck disable=SC2086
    if [ "$(${CLICKHOUSE_CLIENT} $LOG_PINS --query "SELECT max(elapsed) > 0.5 FROM system.processes WHERE query_id = '$kill_id'")" = "1" ]; then
        kill_seen=1
        break
    fi
    sleep 0.05
done

sync_id="${kill_id}_sync"
${CLICKHOUSE_CLIENT} --query_id "$sync_id" --query "KILL QUERY WHERE query_id = '$kill_id' SYNC" > /dev/null 2>&1
wait $kill_bg 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

if [ "$kill_seen" != 1 ]; then
    echo "kill query: TARGET NEVER REACHED THE SEARCH LOOP"
else
    # A kill returning in milliseconds never waited for a running call, so it measures the race and not the
    # checkpoint. Reported as inconclusive rather than as a pass.
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $LOG_PINS --query "
        SELECT multiIf(count() = 0, 'kill query: NO QUERY LOG ROW',
                       max(query_duration_ms) < 10, 'kill query: TARGET WAS NOT RUNNING WHEN KILLED',
                       max(query_duration_ms) < $KILL_BOUND, 'kill query returned promptly',
                       'kill query BLOCKED ' || toString(max(query_duration_ms)) || 'ms')
        FROM system.query_log
        WHERE query_id = '$sync_id' AND current_database = currentDatabase() AND type != 'QueryStart'"
fi

# 10. Liveness control: with no time limit the functions must still return the documented counts, so a
#     "fix" that always threw, or throttling that dropped occurrences, would fail here. All three
#     functions, all four dispatch branches, start positions, multibyte and empty needles.
${CLICKHOUSE_CLIENT} --multiquery --query "
SELECT countSubstrings(materialize('abcabcabc'), 'abc'), countSubstringsCaseInsensitive(materialize('AbCabc'), 'abc'), countSubstringsCaseInsensitiveUTF8(materialize('ПРИВЕТпривет'), 'привет');
SELECT countSubstrings(materialize('aaaa'), materialize('aa')), countSubstringsCaseInsensitive(materialize('XyXy'), materialize('xy')), countSubstringsCaseInsensitiveUTF8(materialize('ЁЖЁж'), materialize('ёж'));
SELECT countSubstrings('abcabcabc', materialize('abc')), countSubstringsCaseInsensitive('AbCabc', materialize('abc')), countSubstringsCaseInsensitiveUTF8('ПРИВЕТпривет', materialize('привет'));
SELECT countSubstrings('aaaa', 'aa'), countSubstringsCaseInsensitive('AAaa', 'aa'), countSubstringsCaseInsensitiveUTF8('ЁЁёё', 'ёё');
SELECT countSubstrings(materialize('abcabcabc'), 'abc', 4), countSubstrings('abcabcabc', materialize('abc'), 4), countSubstrings(materialize('abcabcabc'), materialize('abc'), materialize(toUInt64(7)));
SELECT countSubstrings(materialize(''), 'a'), countSubstrings(materialize('abc'), ''), countSubstrings('', materialize('a')), countSubstrings('abc', materialize(''));
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\\0\\0a', 5)), materialize('a')), countSubstringsCaseInsensitiveUTF8(repeat('\\0\\0a', 5), materialize('a'));
"

# The rows above stay below the throttle; this one crosses it, so the checkpoint runs at least once with
# no deadline set and must still return the exact count.
${CLICKHOUSE_CLIENT} --query "SELECT sum(countSubstringsCaseInsensitiveUTF8($BIG, materialize('a'))) FROM numbers(3) SETTINGS max_block_size = 3, $PINS"
