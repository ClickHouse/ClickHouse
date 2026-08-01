#!/usr/bin/env bash
# Tags: long, no-fasttest, no-msan, no-parallel, no-coverage, no-flaky-check
# no-msan: that build has no embedded compiler, so case 22 would assert nothing.
# no-parallel: case 21 samples the process-wide `CurrentMetrics::QueryNonInternal`.
# no-coverage: per-test coverage instrumentation makes the fixed side of cases 8 and 16 unstable.
# no-flaky-check: The test verifies a timeout-based behavior and is not suitable for rerun-based flakiness detection.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every case bounds the elapsed time the server itself reports in its `TIMEOUT_EXCEEDED` message: the fix
# bounds it, the unfixed code does not. Each one is sized to keep its prologue - materializing an
# argument, reading rows, building a block - small and put the cost inside the function, because the
# prologue is not interruptible by this fix and scales the other way on an optimized build.
# A sanitizer or coverage build stretches both sides, so the bound scales; coverage never reaches
# CXX_FLAGS and has to be read from its own `system.build_options` row.
DEADLINE=1
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
BOUND=$((SCALE * 2000))

# $1 = label, $2 = query, $3 = overflow mode (default "throw"), $4 = query id (default none),
# $5 = "fold" if the call is constant-folded (no pipeline), empty for a pipeline case
#
# Regexp compilation is pinned off for every case but the one that is about the compiled matcher: the
# compiled-regexp cache is server-global with a compile threshold of 3, and this test runs up to 5 times,
# so an earlier run's pattern would already be compiled and the query would finish inside the deadline.
#
# Two messages report the same enforced deadline and only one carries a number: `CancellationChecker`
# can win the race against this function's own check, and its error then has no elapsed part to read.
# For a folded call that is still this function's stop to make, since the fold runs during analysis and
# there is no pipeline to cancel, so the wall clock is bounded instead - as cases 19 and 20 do. For a
# pipeline case it is not: `addPipelineExecutor` throws on the killed flag before the executor is
# registered, so the query can end without ever entering the function, and accepting it on wall time
# would assert query-level cancellation instead of the in-function checkpoint. That is reported as
# inconclusive rather than passed.
run() {
    local label="$1" query="$2" mode="${3:-throw}" query_id="${4:-}" shape="${5:-}"
    local output start_ms elapsed_ms wall_ms
    start_ms=$(date +%s%N)
    # shellcheck disable=SC2086
    output=$(timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode "$mode" \
        --compile_regular_expressions 0 \
        ${query_id:+--query_id "$query_id"} \
        --query "$query" 2>&1)
    wall_ms=$(( ($(date +%s%N) - start_ms) / 1000000 ))
    elapsed_ms=$(printf '%s' "$output" | grep -oP 'elapsed \K[0-9]+(?=\.)' | head -1)
    # A verdict rather than the number itself keeps the reference stable across machine speeds.
    if [ -n "$elapsed_ms" ]; then
        if [ "$elapsed_ms" -lt "$BOUND" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${elapsed_ms} ms"
        fi
    elif ! printf '%s' "$output" | grep -q TIMEOUT_EXCEEDED; then
        echo "$label: no timeout"
    elif [ "$shape" = fold ]; then
        # Wall clock covers the whole client call, not server time alone, hence case 19's allowance.
        if [ "$wall_ms" -lt "$((BOUND * 2))" ]; then
            echo "$label: stopped within bound"
        else
            echo "$label: OVERSHOT ${wall_ms} ms"
        fi
    else
        echo "$label: cancelled before the pipeline started"
    fi
}

# 1. Constant folding: no pipeline exists while this runs, which is what the original hung-check reports
#    showed.
run "fold regexp" \
    "SELECT length(replaceRegexpAll(repeat(repeat('1', 1000000), 200), '[0-9]{1,3}', 'x')) FORMAT Null" throw "" fold

# 2. The same work in the pipeline, which makes case 1 non-vacuous: the defect is the missing in-function
#    checkpoint, not something specific to constant folding. All three arguments must be materialized -
#    materializing only the haystack selects the JIT-accelerated implementation (case 22).
run "pipe regexp" \
    "SELECT length(replaceRegexpAll(materialize(repeat(repeat('1', 1000000), 60)), materialize('[0-9]((a|b)(c|d)|(e|f)(g|h))?'), materialize('x'))) FROM numbers(1) FORMAT Null"

# 3. Many small rows: the per-row loops, which a checkpoint scoped to a single value would never reach.
#    max_block_size is pinned like the other per-row cases: the runner randomizes it down to 8000, and a
#    split block would let the unconditional end-of-call check bound the query on its own.
run "many rows" \
    "SELECT sum(length(replaceRegexpAll(materialize(repeat('1', 20000)), materialize('[0-9]{1,3}'), materialize('x')))) FROM numbers(10000) SETTINGS max_block_size = 10000"

# 4. Many folds, each provably too small to reach a throttled checkpoint, so the three unconditional
#    per-call checks are the only thing that can stop this query - and the only cover for the bulk-copy
#    fast paths, which return without entering any loop. Each fold's cost is building the matcher, charged
#    as the pattern's byte count: 32500 bytes is about 2000 units against a 65536-unit budget.
#    Sizing the folds by their MATCH count instead lets the in-loop check fire, which is what an earlier
#    version got wrong. The folds go in one array rather than a '+' chain: a chain nests one node per
#    fold and formatting it recurses, which overruns the stack allowance a TSan build gives a query thread.
#    One fold is the smallest amount of work this can interrupt, because the budget is only consulted
#    between folds, so the reported time comes out a multiple of a single fold's cost. Many cheap folds
#    rather than few expensive ones keep that step well inside the deadline: at a pattern of 10000 one
#    fold cost about a whole deadline on a thread-sanitizer build, leaving the case no room under the
#    bound. Total pattern bytes are unchanged, so the unfixed side is bounded by nothing as before.
FOLDS=$(python3 -c "print('arraySum([' + ', '.join(\"length(replaceRegexpAll('zzz%d', repeat('[a-z0-9]{1,2}', 2500), 'x'))\" % i for i in range(240)) + '])')")
run "many folds" "SELECT $FOLDS FORMAT Null" throw "" fold

# 5. One match per iteration with a replacement much larger than the match: the whole cost is in the
#    generated output, which accounting that looked only at the searched input would price at zero.
#    Split across independent folds because a single fold large enough to matter races the 5GiB test
#    profile; each fold's output is released before the next starts.
EXPAND=$(python3 -c "print(' + '.join(\"length(replaceAll(repeat('%s', 500), '%s', repeat('Y', 1000000)))\" % (c, c) for c in 'qwertyuiopas'))")
run "expanding replacement" "SELECT $EXPAND FORMAT Null" throw "" fold

# 6. The literal-search implementation reached through the regexp function's trivial-pattern shortcut,
#    whose delegated call re-traverses the whole value. The needle must be a plain literal, or the
#    shortcut is not taken and the case duplicates case 2.
#    Materializing the argument happens before the call and is not interruptible by this fix, while the
#    deadline runs from when the query was registered. That part therefore has to stay well inside the
#    deadline on the slowest build, or the query is stopped before the function is entered and the case
#    passes whatever the function does. The value is kept small and the cost carried by the replacement
#    length instead, which is charged inside the loop.
run "regexp fallback to literal" \
    "SELECT length(replaceRegexpAll(materialize(repeat(repeat('ab', 1000000), 60)), 'ab', 'YZYZYZYZYZYZYZYZYZYZYZYZYZYZYZYZ')) FROM numbers(1) FORMAT Null"

# 7. The same work reached directly through replaceAll rather than through that shortcut.
#    Split across independent folds like case 5, and for the same two reasons: building one 600MB constant
#    is not interruptible by this fix and cost about a whole deadline on a thread-sanitizer build, which
#    left the case no room under the bound, and one value that large also doubled what the query needs
#    against the test memory profile. Each fold gets its own value so none of them is shared.
SPLIT_ALL=$(python3 -c "print('arraySum([' + ', '.join(\"length(replaceAll(repeat(repeat('%s', 1000000), 30), '%s', 'YZ'))\" % (p, p) for p in ['ab','cd','ef','gh','ij','kl','mn','op','qr','st']) + '])')")
run "replaceAll" "SELECT $SPLIT_ALL FORMAT Null" throw "" fold

# 8. Empty haystacks with a different pattern per row: nothing is scanned or written, so all the work is
#    building one matcher per row. The needle must vary, otherwise the matcher is built once outside the
#    loop and the case measures nothing. max_block_size is pinned so the whole row set arrives in one call.
run "per-row matcher setup" \
    "SELECT sum(length(replaceRegexpAll(materialize(''), toString(number)||'[0-9]{1,3}(a|b|c)+x?', 'y'))) FROM numbers(1000000) SETTINGS max_block_size = 1000000"

# 9. A one-byte needle and replacement on the per-row entry point. It has to be a fold: in the pipeline
#    the executor's own between-block check bounds the query whatever the function does. Split for the
#    same reasons as case 7, with a needle that is one byte of each fold's own value.
SPLIT_ONE=$(python3 -c "print('arraySum([' + ', '.join(\"length(replaceAll(repeat(repeat('%s', 1000000), 30), '%s', '%s'))\" % (h, h[1], r) for (h, r) in [('ab','c'),('cd','e'),('ef','g'),('gh','i'),('ij','k'),('kl','m'),('mn','o'),('op','q'),('qr','s'),('st','u')]) + '])')")
run "one-byte in place" "SELECT $SPLIT_ONE FORMAT Null" throw "" fold

# 10. Many capture references against an empty capture group: every match executes the whole list while
#     producing no output, so one checkpoint per match is not enough. The group must be empty and the
#     references must be substitutions rather than literal bytes.
run "instruction list" \
    "SELECT length(replaceRegexpAll(repeat('a', 200000), '()', repeat('\\\\1', 20000))) FORMAT Null" throw "" fold

# 11. A pattern that matches the empty string at every position: each iteration advances one byte, runs no
#     instructions and writes nothing, which is why the budget counts iterations rather than bytes.
run "empty matches" \
    "SELECT length(replaceRegexpAll(repeat(repeat('a', 1000000), 20), '()', '')) FORMAT Null" throw "" fold

# 12. A replacement of 200000 capture references parsed per row against a needle that never matches: the
#     whole list is built before any processing loop runs. The haystack must not match, or the case
#     degenerates into case 10; only the needle varies per row, so nothing large has to be materialized.
run "replacement parsing" \
    "SELECT sum(length(replaceRegexpAll(materialize(''), toString(number)||'(q)', repeat('\\\\1', 200000)))) FROM numbers(1500) SETTINGS max_block_size = 1500"

# 13. A FixedString haystack, which reaches the regexp loop through its own entry point.
run "fixed string haystack" \
    "SELECT sum(length(replaceRegexpAll(toFixedString(materialize(repeat('1', 20000)), 20000), '[0-9]((a|b)(c|d)|(e|f)(g|h))?', 'x'))) FROM numbers(2000) SETTINGS max_block_size = 2000"

# 14. A different replacement per row, which rebuilds the instruction list per row.
run "per-row replacement" \
    "SELECT sum(length(replaceRegexpAll(materialize(repeat('1', 20000)), '[0-9]{1,3}', toString(number)))) FROM numbers(10000) SETTINGS max_block_size = 10000"

# 15. A SINGLE match whose instruction list is itself over budget, so charging the list only after the loop
#     finished would leave one match uninterruptible. Case 10's list is short enough that the per-match
#     charge alone fires.
run "one match, long instruction list" \
    "SELECT length(replaceRegexpAll(repeat('a', 1000000), '(.*)', repeat('\\\\1', 2000))) FORMAT Null" throw "" fold

# 16. The "replace first" specialization, which leaves the per-row loop at the first match - a different
#     loop exit from "replace all". Empty haystacks with a per-row pattern make the matcher setup the whole
#     cost, which is the part the early exit does not skip.
run "replaceRegexpOne" \
    "SELECT sum(length(replaceRegexpOne(materialize(''), toString(number)||'[0-9]{1,3}(a|b|c)+x?', 'y'))) FROM numbers(1000000) SETTINGS max_block_size = 1000000"

# NOTE. Four charged sites have no case of their own, because under the 5GiB test memory profile none of
#     them can be made to run past the deadline: the unconditional suffix and remainder copies, the two
#     FixedString offset loops that follow a bulk copy, and the JIT implementation's output-byte charge and
#     one-byte in-place fast path. Their charges are verified by inspection. This is why the case numbering
#     below has gaps.

# 19. The "break" overflow mode, where checkTimeLimit() returns false instead of throwing, so a path that
#     never calls it ignores the deadline entirely. What is asserted is the wall time, not the presence of
#     an error: in the pipeline the executor's soft check and this function's check race, and both are
#     correct stops.
break_start=$(date +%s%N)
timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode break \
    --compile_regular_expressions 0 \
    --query "SELECT length(replaceRegexpAll(materialize(repeat(repeat('1', 1000000), 60)), materialize('[0-9]((a|b)(c|d)|(e|f)(g|h))?'), materialize('x'))) FROM numbers(1) FORMAT Null" \
    > /dev/null 2>&1
break_ms=$(( ($(date +%s%N) - break_start) / 1000000 ))
if [ "$break_ms" -lt "$((BOUND * 2))" ]; then
    echo "break pipeline: stopped within bound"
else
    echo "break pipeline: OVERSHOT ${break_ms} ms"
fi

#     A folded call has no way to report a partial result, so there the timeout always surfaces as an
#     error, as master already does for base58Decode.
timeout 600 ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --timeout_overflow_mode break \
    --compile_regular_expressions 0 \
    --query "SELECT length(replaceRegexpAll(repeat(repeat('1', 1000000), 200), '[0-9]{1,3}', 'x')) FORMAT Null" 2>&1 \
    | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "break fold: no timeout"

# 20. KILL QUERY, a different channel from the elapsed-time limit: it sets the killed flag and surfaces
#     through a separate branch of the same check. No time limit is set, so only the kill can stop the
#     query, and what is asserted is how long the synchronous kill waits. Both halves are needed - that
#     the target was really running, and that KILL reported killing it - otherwise a query that never
#     started would be "killed" instantly.
kill_id="${CLICKHOUSE_DATABASE}_replace_kill"
${CLICKHOUSE_CLIENT} --query_id "$kill_id" --compile_regular_expressions 0 \
    --query "SELECT length(replaceRegexpAll(repeat(repeat('1', 1000000), 400), '[0-9]{1,3}', 'x')) FORMAT Null" \
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

# 21. A `replace*` call inside a stored expression (a key, a skip index or a TTL). The instance built while
#     that expression is analysed is kept for the table's lifetime and executed by unrelated later queries,
#     so the query to check is the one running the call, not the one that defined it: a definition-time
#     query would be the wrong deadline and, held alive, a permanent `CurrentMetrics::QueryNonInternal`
#     leak. Asserted from both sides. The rows must arrive in one block, or the pipeline's own check bounds
#     the INSERT whatever the function does.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_replace_stored SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_replace_stored (k String) ENGINE = MergeTree
    PARTITION BY substring(replaceRegexpAll(k, '[0-9]((a|b)(c|d)|(e|f)(g|h))?', 'x'), 1, 1) ORDER BY tuple()"
run "stored expression" \
    "INSERT INTO t_replace_stored SELECT repeat('1', 1000000) FROM numbers(40) SETTINGS max_insert_block_size = 40, min_insert_block_size_rows = 40"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_replace_stored SYNC"

#     The leak side: a retained QueryStatus keeps the `CurrentMetrics::QueryNonInternal` increment it owns,
#     so the count never returns to where it was. The assertion is a zero delta rather than a tolerance,
#     which a drop in either direction would mask: the metric excludes internal queries, so background work
#     cannot move it, and the test is no-parallel, so every non-internal query in the window is one of this
#     test's own and cancels in the difference.
sample_queries() {
    local m=999999 v
    for _ in 1 2 3 4 5; do
        v=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.metrics WHERE metric = 'QueryNonInternal'")
        [ "$v" -lt "$m" ] && m=$v
    done
    echo "$m"
}

for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_replace_stored_$i SYNC"
done
queries_before=$(sample_queries)
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_replace_stored_$i (k String, v String) ENGINE = MergeTree ORDER BY k"
    # ALTER, not CREATE: a CREATE analyses the expression on a context carrying no query state, so nothing
    # could be captured there and the case would be vacuous.
    ${CLICKHOUSE_CLIENT} --max_execution_time "$DEADLINE" --query "
        ALTER TABLE t_replace_stored_$i ADD INDEX ix replaceRegexpAll(v, '[0-9]', 'x') TYPE set(10) GRANULARITY 1"
done
queries_after=$(sample_queries)
if [ "$((queries_after - queries_before))" = 0 ]; then
    echo "stored expression leak: no query state retained"
else
    echo "stored expression leak: RETAINED $((queries_after - queries_before)) query states"
fi
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_replace_stored_$i SYNC"
done

# 22. The JIT-compiled regexp matcher, which has its own copy of the substitution loop. It is reached only
#     with a non-constant haystack, a constant non-trivial pattern and a constant replacement, and only
#     once the pattern has been compiled - hence the two settings and the many short rows.
#     That the pattern really was compiled is asserted rather than argued, by the `CompileRegexpFunction`
#     event this PR adds: `getRegexpJITMatcher` charges it once the matcher has survived both the holder
#     construction and the cache insertion, so it cannot claim a compile the query then fell back from, and
#     it is read back for this query's own `query_id`. `argMax` over `event_time_microseconds` rather than
#     `sum`, because the stress runner gives some threads one fixed database for every test, so across
#     repeats the same id accumulates rows and a sum would answer with an earlier run's compile. The cache
#     must be dropped first for the same reason:
#     `CacheBase::getOrSet` returns early on a hit without invoking the load lambda, so nothing is compiled
#     and the event is not charged when the pattern is already cached.
#     A failed compile is silent by design, and a build with no embedded compiler can never obtain the
#     matcher, so that is asserted directly: `USE_EMBEDDED_COMPILER` is substituted unconditionally, so on
#     such a build the row is empty and anything that is not `ON` takes the fail-loud branch.
JIT_PINS="--compile_expressions 0 --compile_aggregate_expressions 0 --compile_sort_description 0"
jit_id="${CLICKHOUSE_DATABASE}_replace_jit"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP COMPILED EXPRESSION CACHE"
run "jit matcher" \
    "SELECT sum(length(replaceRegexpAll(materialize(repeat('1', 10000)), '[0-9]{1,3}', repeat('y', 20)))) FROM numbers(40000) SETTINGS max_block_size = 40000, compile_regular_expressions = 1, min_count_to_compile_regular_expression = 0, compile_expressions = 0, compile_aggregate_expressions = 0, compile_sort_description = 0" \
    throw "$jit_id"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# shellcheck disable=SC2086
jit_regexp_compiles=$(${CLICKHOUSE_CLIENT} $JIT_PINS --query "SELECT argMax(ProfileEvents['CompileRegexpFunction'], event_time_microseconds) FROM system.query_log WHERE query_id = '$jit_id' AND current_database = currentDatabase() AND type IN ('QueryFinish', 'ExceptionWhileProcessing')")
# shellcheck disable=SC2086
jit_embedded=$(${CLICKHOUSE_CLIENT} $JIT_PINS --query "SELECT value FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER'")
case "$jit_embedded" in
    ON|1) if [ "$jit_regexp_compiles" -ge 1 ]; then
              echo "jit matcher: compiled"
          else
              echo "jit matcher: NOT COMPILED, the case ran the interpreted loop"
          fi ;;
    *)    echo "jit matcher: NO EMBEDDED COMPILER, the case ran the interpreted loop" ;;
esac

# 25. A FixedString haystack on the literal implementation, which has its own entry point with its own
#     per-row offset loop and its own search loop over the whole block. The haystack has to be
#     materialized: constant arguments are unwrapped to their nested data column before the dispatcher
#     runs, which leaves the needle no longer constant so no branch matches. A prologue is therefore
#     unavoidable, so the work per byte is large rather than the data - every byte matches - while the
#     replacement stays bounded so the output cannot outgrow the test's memory profile.
#     Sizing this case by its written bytes instead does not work, which is what two earlier versions got
#     wrong: the output is then what bounds the call, and no single size satisfies a fast and a slow build.
#     The row count bounds that unavoidable prologue, which the deadline covers but this fix cannot
#     interrupt: at 400000 rows it alone outlasted the deadline on a thread-sanitizer build, so the query
#     was stopped before the function was entered and the case reported the same time either way.
run "fixed string literal" \
    "SELECT sum(length(replaceAll(toFixedString(materialize(repeat('b', 1600)), 1600), 'b', 'YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'))) FROM numbers(50000) SETTINGS max_block_size = 50000"
