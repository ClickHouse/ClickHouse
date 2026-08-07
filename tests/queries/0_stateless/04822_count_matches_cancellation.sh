#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-flaky-check
# no-parallel: case 8 samples the process-wide `CurrentMetrics::QueryNonInternal`.
# no-flaky-check: The test verifies a timeout-based behavior and is not suitable for rerun-based flakiness detection.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every timing case bounds how long a `countMatches` call runs past the deadline it was given: the fix
# bounds it, the unfixed code does not. Each one keeps its prologue - materializing an argument, reading
# rows, building a block - small and puts the cost inside the function, because the prologue is not
# interruptible by this fix. A sanitizer or coverage build stretches both sides, so the bound scales;
# coverage never reaches CXX_FLAGS and has to be read from its own `system.build_options` row.
DEADLINE_MS=2000
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
BOUND=$((SCALE * 4000))
# What a bound allows on top of its deadline. A case that needs a deadline of its own keeps this
# allowance rather than the bound itself, so shortening a deadline does not loosen the assertion with it.
SLACK_MS=$((BOUND - DEADLINE_MS))
# `max_execution_time` is given in seconds; every deadline here is a whole number of milliseconds.
to_seconds() { printf '%d.%03d' "$(($1 / 1000))" "$(($1 % 1000))"; }
DEADLINE=$(to_seconds "$DEADLINE_MS")

# $1 = label, $2 = query, $3 = overflow mode (default "throw"), $4 = query id (default none),
# $5 = "fold" if the call is constant-folded (no pipeline), empty for a pipeline case,
# $6 = the deadline in milliseconds (default `DEADLINE_MS`)
#
# Regexp compilation is pinned off so a pattern this test has already run cannot arrive compiled: the
# compiled-regexp cache is server-global with a compile threshold of 3, and this test runs up to 5 times.
#
# Two messages report the same enforced deadline and only one carries a number: `CancellationChecker` can
# win the race against this function's own check, and its error then has no elapsed part to read. For a
# folded call that is still this function's stop to make, since the fold runs during analysis and there is
# no pipeline to cancel, so the wall clock is bounded instead. For a pipeline case it is not:
# `addPipelineExecutor` throws on the killed flag before the executor is registered, so the query can end
# without ever entering the function, and accepting it on wall time would assert query-level cancellation
# instead of the in-function checkpoint. That is reported as inconclusive rather than passed.
run() {
    local label="$1" query="$2" mode="${3:-throw}" query_id="${4:-}" shape="${5:-}" deadline_ms="${6:-$DEADLINE_MS}"
    local output start_ms elapsed_ms wall_ms
    local bound=$((deadline_ms + SLACK_MS))
    start_ms=$(date +%s%N)
    # shellcheck disable=SC2086
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$(to_seconds "$deadline_ms")" --timeout_overflow_mode "$mode" \
        --compile_regular_expressions 0 \
        ${query_id:+--query_id "$query_id"} \
        --query "$query" 2>&1)
    wall_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    elapsed_ms=$(printf '%s' "$output" | grep -oP 'elapsed \K[0-9]+(?=\.)' | head -1)
    # A verdict rather than the number itself keeps the reference stable across machine speeds.
    if [ -n "$elapsed_ms" ]; then
        if [ "$elapsed_ms" -lt "$bound" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label: no timeout"
    elif [ "$shape" = fold ]; then
        # Wall clock covers the whole client call, not server time alone, hence the doubled allowance.
        if [ "$wall_ms" -lt "$((bound * 2))" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${wall_ms} ms"
        fi
    else
        echo "$label: cancelled before the pipeline started"
    fi
}

# The haystack is NUL-heavy because that is the shape the fuzzer reports arrive in, and the pattern is a
# negated class so the matcher cannot prefilter with memchr and skip the scan the checkpoint is charged for.
HAYSTACK="repeat(repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000), 10)"

# 1. Constant folding: no pipeline exists while this runs, which is the shape the original report showed.
run "fold" "SELECT countMatches($HAYSTACK, '[^q]') FORMAT Null" throw "" fold

# 2. The same work in the pipeline, which makes case 1 non-vacuous: the defect is the missing in-function
#    checkpoint, not something specific to constant folding.
run "pipeline" "SELECT countMatches(materialize($HAYSTACK), '[^q]') FROM numbers(1) FORMAT Null"

# 3. A FixedString haystack, which reaches the match loop through its own entry point rather than the
#    ColumnString one. The rows must arrive in one block, or the pipeline's own between-block check bounds
#    the query whatever the function does.
run "fixed string" \
    "SELECT sum(countMatches(toFixedString(materialize(repeat('ab', 500000)), 1000000), '[^q]')) FROM numbers(200) SETTINGS max_block_size = 200"

# 4. Many small rows: the per-row loop, which a checkpoint scoped to a single value would never reach, and
#    which is also what requires the budget to live across rows rather than per call to `countMatches`.
run "many rows" \
    "SELECT sum(countMatches(materialize(repeat('ab', 10000)), '[^q]')) FROM numbers(20000) SETTINGS max_block_size = 20000"

# 5. One match per row over a megabyte: about two loop iterations per row, so an iteration counter alone
#    never reaches its threshold and the deadline is observed only because the bytes each match scanned are
#    charged too. This is the case that distinguishes charging bytes from counting iterations. The rows must
#    arrive in one block, which puts the whole haystack in memory at once, so the row count is what keeps
#    that block well inside the 5G `max_memory_usage` the test profile sets; the alternation pattern rather
#    than a larger block is what makes the call outlast the deadline.
run "sparse matches" \
    "SELECT sum(countMatches(materialize(concat(repeat('a', 1000000), 'b')), '(a|aa|aaa|aaaa)*b')) FROM numbers(1600) SETTINGS max_block_size = 1600"

# 5b. A pattern that matches empty, which advances a single byte per iteration and reaches its own charge
#     rather than the one a non-empty match reaches. A whole-match offset is a sentinel and not a position
#     when the match is empty, so this is also the shape that would read one if it were read unguarded.
run "empty matches" \
    "SELECT countMatches(materialize(repeat(repeat('a', 1000000), 60)), 'x*') FROM numbers(1) FORMAT Null"

# 6. `countMatchesCaseInsensitive`, the second registered function: the same template with
#    `case_insensitive = true`, so it is covered by construction rather than by its own charge.
run "case insensitive" \
    "SELECT countMatchesCaseInsensitive(repeat(repeat('\0\0\0\0\0\0\0\0\0\0A', 1000000), 10), '[^q]') FORMAT Null" throw "" fold

# 7. The "break" overflow mode, where checkTimeLimit() returns false instead of throwing, so a path that
#    never calls it ignores the deadline entirely. What is asserted is the wall time, not the presence of an
#    error: in the pipeline the executor's soft check and this function's check race, and both are correct
#    stops.
break_start=$(date +%s%N)
timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode break \
    --compile_regular_expressions 0 \
    --query "SELECT countMatches(materialize($HAYSTACK), '[^q]') FROM numbers(1) FORMAT Null" \
    > /dev/null 2>&1
break_ms=$(( ($(date +%s%N) - break_start) / 1000000 ))
if [ "$break_ms" -lt "$((BOUND * 2))" ]; then
    echo "break pipeline: stopped within bound"
else
    echo "break pipeline: OVERSHOT ${break_ms} ms"
fi

#     A folded call has no way to report a partial result, so there the timeout always surfaces as an error.
timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode break \
    --compile_regular_expressions 0 \
    --query "SELECT countMatches($HAYSTACK, '[^q]') FORMAT Null" 2>&1 \
    | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "break fold: no timeout"

# 8. KILL QUERY, a different channel from the elapsed-time limit: it sets the killed flag and surfaces
#    through a separate branch of the same check. No time limit is set, so only the kill can stop the query,
#    and what is asserted is how long the synchronous kill waits. Both halves are needed - that the target
#    was really running, and that KILL reported killing it - otherwise a query that never started would be
#    "killed" instantly.
kill_id="${CLICKHOUSE_DATABASE}_count_matches_kill"
${CLICKHOUSE_CLIENT} --query_id "$kill_id" --compile_regular_expressions 0 \
    --query "SELECT countMatches(repeat(repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000), 40), '[^q]') FORMAT Null" \
    > /dev/null 2>&1 &
kill_bg=$!
kill_seen=0
for _ in $(seq 1 100); do
    if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$kill_id'")" = 1 ]; then
        kill_seen=1
        break
    fi
    sleep 0.1
done
kill_start=$(date +%s%N)
kill_rows=$(${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$kill_id' SYNC FORMAT TSV" 2>/dev/null | grep -c "$kill_id")
kill_ms=$(( ($(date +%s%N) - kill_start) / 1000000 ))
wait $kill_bg 2>/dev/null
if [ "$kill_seen" != 1 ]; then
    echo "kill query: TARGET NEVER RAN"
elif [ "$kill_rows" != 1 ]; then
    echo "kill query: KILL DID NOT REPORT THE TARGET"
elif [ "$kill_ms" -lt "$((BOUND * 2))" ]; then
    echo "kill query: killed within bound"
else
    echo "kill query: OVERSHOT ${kill_ms} ms"
fi

# 9. A `countMatches` call inside a stored expression (a key, a skip index or a TTL). The instance built
#    while that expression is analysed is kept for the table's lifetime and executed by unrelated later
#    queries, so the query to check is the one running the call, not the one that defined it: a
#    definition-time query would be the wrong deadline and, held alive, a permanent
#    `CurrentMetrics::QueryNonInternal` leak. Asserted from both sides. The rows must arrive in one block,
#    or the pipeline's own check bounds the INSERT whatever the function does.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_count_matches_stored SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_count_matches_stored (k String) ENGINE = MergeTree
    PARTITION BY countMatches(k, '[^q]') % 2 ORDER BY tuple()"
run "stored expression" \
    "INSERT INTO t_count_matches_stored SELECT repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000) FROM numbers(20) SETTINGS max_insert_block_size = 20, min_insert_block_size_rows = 20"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_count_matches_stored SYNC"

#     The leak side: a retained QueryStatus keeps the `CurrentMetrics::QueryNonInternal` increment it owns,
#     so the count never returns to where it was. Only a rise is a retention: `~QueryStatus` releases the
#     increment, so the count can fall. Still no tolerance: the metric excludes internal queries, so
#     background work cannot move it, and the test is no-parallel, so every non-internal query in the
#     window is one of this test's own and cancels in the difference.
sample_queries() {
    local m=999999 v
    for _ in 1 2 3 4 5; do
        v=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.metrics WHERE metric = 'QueryNonInternal'")
        [ "$v" -lt "$m" ] && m=$v
    done
    echo "$m"
}

for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_count_matches_stored_$i SYNC"
done
queries_before=$(sample_queries)
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_count_matches_stored_$i (k String, v String) ENGINE = MergeTree ORDER BY k"
    # ALTER, not CREATE: a CREATE analyses the expression on a context carrying no query state, so nothing
    # could be captured there and the case would be vacuous.
    ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --query "
        ALTER TABLE t_count_matches_stored_$i ADD INDEX ix countMatches(v, '[0-9]') TYPE set(10) GRANULARITY 1"
done
queries_after=$(sample_queries)
if [ "$((queries_after - queries_before))" -le 0 ]; then
    echo "stored expression leak: no query state retained"
else
    echo "stored expression leak: RETAINED $((queries_after - queries_before)) query states"
fi
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_count_matches_stored_$i SYNC"
done

# 10. Results are unchanged by the charge. The zero-byte and NUL-heavy shapes are the ones the charge could
#     have disturbed, because the amount charged is derived from how far each match advanced `pos`, and both
#     values of `count_matches_stop_at_empty_match` are covered so both branches of the empty-match handling
#     are exercised.
${CLICKHOUSE_CLIENT} --query "
    SELECT countMatches(repeat('\0\0\0\0\0\0\0\0\0\0a', 100), 'a'),
           countMatches('aaa', ''), countMatches('aaa', 'x*'), countMatches('', 'x*'),
           countMatches('hello 123 world 456 test', '[0-9]+'),
           countMatches('', 'a'), countMatches('a', 'a'), countMatches('aa', 'a'),
           countMatches('ab ab', '\\\\bab'), countMatches('aaa', '^a'), countMatches('aaa', 'a\$'),
           countMatches(toFixedString('abcabc', 6), 'abc'), countMatches(toFixedString('a\0b', 3), '\0'),
           countMatches('\0\0\0', ''), countMatches('\0\0\0', '\0*'), countMatches('\0a\0', '\0'),
           countMatchesCaseInsensitive('Hello HELLO world', 'hello'),
           countMatchesCaseInsensitive('AAA', 'a')"
${CLICKHOUSE_CLIENT} --query "
    SELECT countMatches('aaa', ''), countMatches('aaa', 'x*'), countMatches('', 'x*'),
           countMatches('\0\0\0', ''), countMatches('\0\0\0', '\0*')
    SETTINGS count_matches_stop_at_empty_match = 1"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(countMatches(materialize(concat('x', toString(number), 'y')), '[0-9]')) FROM numbers(100)"
