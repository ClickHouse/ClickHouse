#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# When the `-ForEach` array grows, `ensureAggregateData` creates fresh nested states and merges the
# old ones into them, and the old states used to be abandoned without being destroyed. A
# `quantilesExact` state keeps 5 values inline and allocates past that, so an element must take at
# least 6 values before a later, longer row regrows the array. A leak reports only at process exit,
# so the oracle needs a short-lived process: run the queries in `clickhouse-local` and count the
# leak reports each one leaves behind. Both counts are 0 on a build without a leak sanitizer, so
# one reference matches every build type.
#
# Each invocation gets its own log so the two counts stay independent: the regrow and the
# exception path are separate guards, and a mutant that reverts one must redden only its own count.
# Routing also keeps the report out of the runner's fatal log, which would fail the test as stderr
# before the counts below are ever compared (last `log_path` wins). A leak makes the sanitizer
# abort, and for that the shell writes a job status line to stderr, which would fail the test the
# same way, so each invocation is a pipeline - the shell reports no status for those.
regrow_log="${CLICKHOUSE_TMP}/04812_regrow_${CLICKHOUSE_TEST_UNIQUE_NAME}"
throw_log="${CLICKHOUSE_TMP}/04812_throw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$regrow_log"* "$throw_log"*

o="log_path=$regrow_log"
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "
        -- add() allocation site: 8 rows of length 1 spill element 0 to the heap, then a length-2
        -- row regrows.
        SELECT quantilesExactForEach(0.5)(arr) FROM (SELECT arrayMap(x -> number + x, range(if(number < 8, 1, 2))) AS arr FROM numbers(16));

        -- Repeated regrows also abandon states allocated by the migration merge itself.
        SELECT quantilesExactForEach(0.5)(arr) FROM (SELECT arrayMap(x -> number + x, range(intDiv(number, 8) + 2)) AS arr FROM numbers(64));

        -- Same through GROUP BY, so every key's state regrows independently.
        SELECT k, quantilesExactForEach(0.5)(arr)
        FROM (SELECT number % 2 AS k, arrayMap(x -> number + x, range(intDiv(number, 8) + 1)) AS arr FROM numbers(32))
        GROUP BY k ORDER BY k;

        -- State-state merge path: arrayReduceInRanges pre-aggregates one -ForEach state per 64
        -- rows and merges those states, so ensureAggregateData regrows from mergeImpl rather than
        -- from add.
        SELECT arrayReduceInRanges('quantilesExactForEach(0.5)', [(1, 200)], arrayMap(x -> arrayMap(y -> y, range(intDiv(x, 20) + 1)), range(200)));

        -- Results must be unchanged: the migrated values are still all present after the fix.
        SELECT arrayReduce('quantilesExactForEach(0.5)', [[1], [1, 2, 3], [5, 5]]);
        SELECT arrayReduce('sumForEach', [[1, 2], [3, 4, 5], [6, 7]]);
    " | cat

# The migration merge allocates, so a throw part-way through it used to abandon every state the
# regrow had just created. Each element holds 4 MiB of UInt256 values, so the migration doubles a
# 32 MiB state element by element and this limit lands inside that loop - which needs
# `max_block_size` small enough that no input block is the largest allocation. The error text goes
# to stdout, because any stderr fails the test.
o="log_path=$throw_log"
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "
        SELECT length(quantilesExactForEach(0.5)(arr))
        FROM (SELECT arrayMap(x -> toUInt256(number) + x, range(if(number < 131072, 8, 9))) AS arr FROM numbers(131073))
        SETTINGS max_threads = 1, max_untracked_memory = 1, max_block_size = 8192, max_memory_usage = 50000000
    " 2>&1 | grep -c -m1 'MEMORY_LIMIT_EXCEEDED'

cat "$regrow_log"* 2>/dev/null | grep -c 'detected memory leaks' || true
cat "$throw_log"* 2>/dev/null | grep -c 'detected memory leaks' || true
rm -f "$regrow_log"* "$throw_log"*
