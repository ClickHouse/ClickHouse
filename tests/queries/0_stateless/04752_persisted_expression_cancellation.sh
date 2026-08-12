#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-flaky-check
# no-parallel: the leak case samples the process-wide `CurrentMetrics::QueryNonInternal`.
# no-flaky-check: every assertion is a cancellation latency, and the flaky check runs many copies of
# one test on a single runner; that contention overlaps the unfixed distribution, so no bound both
# passes fixed and fails unfixed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A function that resolves its `QueryStatusPtr` when the object is built captures the status of
# whichever query analysed the expression. For an expression stored in table metadata that is a global
# context (sorting key, skip index: empty status) or the `CREATE TABLE` query (`PARTITION BY`: an
# already-finished deadline), so the in-loop cancellation check the function already contains reads a
# query that is not the one running and never fires.
#
# Each case below therefore runs a persisted expression from a LATER query. Three fixture properties
# are load-bearing, and getting any of them wrong makes the case pass unfixed:
#   * no `%` in a `PARTITION BY` key. `MergeTreePartition::adjustPartitionKey` rebuilds the whole key
#     description with the INSERT's own context when the key mentions `modulo`, which hands the
#     expression a correct status and hides the defect.
#   * the INSERT's SELECT side must be cheap. If it burns the deadline itself the timeout fires there,
#     on a direct call with a correct status, and never reaches the persisted expression.
#   * arguments must be column-dependent. A constant argument is folded once, so the function runs a
#     single time whatever the row count.
#
# `max_insert_block_size`/`min_insert_block_size_rows`/`max_threads` are pinned statement-level because
# the runner randomizes them and the effect size is a function of rows-per-block: split into enough
# blocks, the pipeline's own between-blocks check bounds the query however the function behaves.
# `max_execution_time` and `timeout_overflow_mode` are the oracle and are not randomized.
BLOCK="max_insert_block_size = 100000, min_insert_block_size_rows = 100000, max_threads = 1"

# 4x the 1s limit absorbs clock granularity, a busy runner and the work in flight when the check fires;
# a sanitizer or coverage build stretches both sides, so the bound scales. The unfixed path lands far
# above either.
SCALE=1
[ -n "$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS' AND value LIKE '%sanitize=%'")" ] && SCALE=2
case "$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.build_options WHERE name = 'WITH_COVERAGE'")" in ON|1) SCALE=2 ;; esac
BOUND=$((SCALE * 4000))

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE src_f (a Float64, k UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE src_i (a UInt64, k UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE src_s (a String, k UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE src_g (a Array(Tuple(Float64, Float64)), k UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO src_f SELECT number * 1e-9, number FROM numbers(60000);
    INSERT INTO src_i SELECT number, number FROM numbers(60000);
    -- 'Q' is a base58 digit, so this needs no base58Encode to be decodable, and the length varies per
    -- row so the argument stays column-dependent.
    INSERT INTO src_s SELECT repeat('Q', 4000 + (number % 7)), number FROM numbers(3000);
    INSERT INTO src_g SELECT [(number * 1e-7, 0.), (number * 1e-7 + 2., 0.), (number * 1e-7 + 2., 2.), (number * 1e-7, 2.), (number * 1e-7, 0.)], number FROM numbers(2000);"

# $1 = label, $2 = the table DDL carrying the persisted expression, $3 = the source table
deadline_case() {
    local label="$1" ddl="$2" src="$3"
    local query_id="04752_${label}_${CLICKHOUSE_DATABASE}"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_$label SYNC"
    ${CLICKHOUSE_CLIENT} -q "$ddl"
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time 1 --timeout_overflow_mode throw \
        -q "INSERT INTO t_$label SELECT * FROM $src SETTINGS $BLOCK" 2>&1 \
        | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "$label: no timeout"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT if(max(query_duration_ms) < $BOUND, '$label: stopped promptly',
                  '$label: OVERSHOT ' || toString(max(query_duration_ms)) || 'ms past a 1000ms limit')
        FROM system.query_log
        WHERE query_id = '$query_id' AND current_database = currentDatabase() AND type != 'QueryStart'"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_$label SYNC"
}

# CONTROL. `replaceRegexpAll` already resolves its status per call, so it is bounded with and without
# this change. It is what makes the carrier cases an attribution rather than "these functions are
# slow": execution-time resolution demonstrably works on a persisted expression.
deadline_case "control_partition" \
    "CREATE TABLE t_control_partition (a String, k UInt64) ENGINE = MergeTree
     PARTITION BY intDiv(length(replaceRegexpAll(a, '(a)', 'xyxyxyxyxy')), 1000000) ORDER BY tuple()" \
    "(SELECT repeat('ab', 2000) || toString(number) AS a, number AS k FROM numbers(60000))"

# The `PARTITION BY` route: `KeyDescription::getKeyFromAST` builds the expression once at DDL time and
# the metadata keeps it, so every later write re-executes that instance with the `CREATE`'s status.
deadline_case "geohash_partition" \
    "CREATE TABLE t_geohash_partition (a Float64, k UInt64) ENGINE = MergeTree
     PARTITION BY intDiv(length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7)), 1000000) ORDER BY tuple()" \
    "src_f"
deadline_case "arrayfold_partition" \
    "CREATE TABLE t_arrayfold_partition (a UInt64, k UInt64) ENGINE = MergeTree
     PARTITION BY intDiv(arrayFold((acc, x) -> acc + x + a, range(3000), toUInt64(0)), 100000000000) ORDER BY tuple()" \
    "src_i"
# Three `sleep` calls, not one: a single call cannot overshoot by more than
# `function_sleep_max_microseconds_per_block` (3s by default) whatever the deadline, which is under the
# bound on its own, so a one-call case passes unfixed. The arguments must differ - identical calls are
# one common subexpression and run once (measured: `sleep(3) + sleep(3)` takes 3s, `sleep(3) +
# sleep(2.9)` takes 6s).
deadline_case "sleep_partition" \
    "CREATE TABLE t_sleep_partition (a UInt64, k UInt64) ENGINE = MergeTree
     PARTITION BY intDiv(a, 1000000) + sleep(3) + sleep(2.9) + sleep(2.8) ORDER BY tuple()" \
    "src_i"
deadline_case "base58_partition" \
    "CREATE TABLE t_base58_partition (a String, k UInt64) ENGINE = MergeTree
     PARTITION BY intDiv(length(base58Decode(a)), 1000000) ORDER BY tuple()" \
    "src_s"

# The sorting-key route fails differently and so needs its own case per carrier: `MergeTreeData`'s
# context is its global one, so the captured status is empty rather than merely wrong.
deadline_case "geohash_orderby" \
    "CREATE TABLE t_geohash_orderby (a Float64, k UInt64) ENGINE = MergeTree
     ORDER BY (k, length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7)))" \
    "src_f"
deadline_case "arrayfold_orderby" \
    "CREATE TABLE t_arrayfold_orderby (a UInt64, k UInt64) ENGINE = MergeTree
     ORDER BY (k, arrayFold((acc, x) -> acc + x + a, range(3000), toUInt64(0)))" \
    "src_i"
deadline_case "sleep_orderby" \
    "CREATE TABLE t_sleep_orderby (a UInt64, k UInt64) ENGINE = MergeTree
     ORDER BY (k, sleep(3) + sleep(2.9) + sleep(2.8))" \
    "src_i"
deadline_case "base58_orderby" \
    "CREATE TABLE t_base58_orderby (a String, k UInt64) ENGINE = MergeTree
     ORDER BY (k, length(base58Decode(a)))" \
    "src_s"

# A skip index is built through the same global-context call as the sorting key but by a separate
# function, so it is asserted rather than assumed to follow.
deadline_case "geohash_skipindex" \
    "CREATE TABLE t_geohash_skipindex (a Float64, k UInt64,
        INDEX ix length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7)) TYPE minmax GRANULARITY 1)
     ENGINE = MergeTree ORDER BY tuple()" \
    "src_f"

# The h3 pair checks `isKilled()` rather than `checkTimeLimit()`, which still observes a deadline in
# the default `throw` mode: `CancellationChecker` calls `cancelQuery(TIMEOUT)` there, which sets
# `is_killed`. Under `timeout_overflow_mode = 'break'` nothing sets it and these two run to completion,
# so only the default mode is asserted here.
deadline_case "h3cells_orderby_deadline" \
    "CREATE TABLE t_h3cells_orderby_deadline (a Array(Tuple(Float64, Float64)), k UInt64)
     ENGINE = MergeTree ORDER BY (k, length(h3PolygonToCells(a, 7)))" \
    "src_g"
# Resolution 8 rather than 7 for the same reason as the kill case below: at 7 this fixture finishes
# inside the bound unfixed, so the case would pass either way.
deadline_case "h3containment_orderby_deadline" \
    "CREATE TABLE t_h3containment_orderby_deadline (a Array(Tuple(Float64, Float64)), k UInt64)
     ENGINE = MergeTree ORDER BY (k, length(h3PolygonToCellsWithContainment(a, 8, 0)))" \
    "src_g"

# BASELINE. The same functions on a non-persisted path are bounded with and without this change, which
# is what confines the defect to the persisted routes rather than to the functions themselves.
for fn in "sum(length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7))) FROM src_f" \
          "sum(arrayFold((acc, x) -> acc + x + a, range(3000), toUInt64(0))) FROM src_i" \
          "sum(length(base58Decode(a))) FROM src_s"; do
    ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode throw \
        -q "SELECT $fn SETTINGS max_block_size = 100000, max_threads = 1" 2>&1 \
        | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "direct baseline: no timeout"
done

# `KILL QUERY` needs its own channel for the h3 pair: it is the one cancellation their loop observes in
# every overflow mode.
kill_case() {
    local label="$1" ddl="$2"
    local query_id="04752_kill_${label}_${CLICKHOUSE_DATABASE}"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_$label SYNC"
    ${CLICKHOUSE_CLIENT} -q "$ddl"
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" \
        -q "INSERT INTO t_$label SELECT * FROM src_g SETTINGS $BLOCK" > /dev/null 2>&1 &
    local query_pid=$!

    # Readiness waits for the query to have been RUNNING, not merely visible: `ProcessList` makes it
    # visible before the executor is attached, and `addPipelineExecutor` raises a pending cancellation
    # itself, so a kill winning that race would pass even unfixed. Killing an id that never started
    # also returns promptly, so the poll reports its own failure rather than looking like a pass.
    # The poll runs over HTTP: a fresh client process costs ~70ms per iteration against ~10ms for a
    # request, and that overhead would land inside the measured kill latency.
    local waited=0 ready_ok=0
    while [ "$waited" -lt 500 ]; do
        if [ "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT max(elapsed) > 1 FROM system.processes WHERE query_id = '$query_id'")" = "1" ]; then
            ready_ok=1
            break
        fi
        waited=$((waited + 1))
        sleep 0.02
    done
    [ "$ready_ok" = "0" ] && echo "$label: query never reached the row loop"

    ${CLICKHOUSE_CLIENT} --query_id "${query_id}_sync" -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
    wait $query_pid 2>/dev/null

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT if(max(query_duration_ms) < $BOUND, '$label: killed promptly',
                  '$label: KILL BLOCKED ' || toString(max(query_duration_ms)) || 'ms')
        FROM system.query_log
        WHERE query_id = '${query_id}_sync' AND current_database = currentDatabase() AND type != 'QueryStart'"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_$label SYNC"
}

kill_case "h3cells_orderby" \
    "CREATE TABLE t_h3cells_orderby (a Array(Tuple(Float64, Float64)), k UInt64) ENGINE = MergeTree
     ORDER BY (k, length(h3PolygonToCells(a, 7)))"
# Resolution 8 rather than 7: at 7 this fixture runs for about three seconds, so a kill landing after
# one second finishes inside the bound however the function behaves. Every kill case needs the target to
# outlast the bound by enough that only a prompt cancellation can satisfy it.
kill_case "h3containment_orderby" \
    "CREATE TABLE t_h3containment_orderby (a Array(Tuple(Float64, Float64)), k UInt64) ENGINE = MergeTree
     ORDER BY (k, length(h3PolygonToCellsWithContainment(a, 8, 0)))"

# A `geohashesInBox` case on the same channel, so the `KILL` half is not asserted for the h3 pair only.
kill_geohash_id="04752_kill_geohash_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_kill_geohash (a Float64, k UInt64) ENGINE = MergeTree
    ORDER BY (k, length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7)))"
${CLICKHOUSE_CLIENT} --query_id "$kill_geohash_id" \
    -q "INSERT INTO t_kill_geohash SELECT * FROM src_f SETTINGS $BLOCK" > /dev/null 2>&1 &
kill_geohash_pid=$!
waited=0 ready_ok=0
while [ "$waited" -lt 500 ]; do
    if [ "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT max(elapsed) > 1 FROM system.processes WHERE query_id = '$kill_geohash_id'")" = "1" ]; then
        ready_ok=1
        break
    fi
    waited=$((waited + 1))
    sleep 0.02
done
[ "$ready_ok" = "0" ] && echo "kill_geohash: query never reached the row loop"
${CLICKHOUSE_CLIENT} --query_id "${kill_geohash_id}_sync" -q "KILL QUERY WHERE query_id = '$kill_geohash_id' SYNC" > /dev/null
wait $kill_geohash_pid 2>/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT if(max(query_duration_ms) < $BOUND, 'kill_geohash: killed promptly',
              'kill_geohash: KILL BLOCKED ' || toString(max(query_duration_ms)) || 'ms')
    FROM system.query_log
    WHERE query_id = '${kill_geohash_id}_sync' AND current_database = currentDatabase() AND type != 'QueryStart'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_kill_geohash SYNC"

# The leak side of the same line. A `QueryStatusPtr` member kept the `CurrentMetrics::QueryNonInternal`
# increment its `QueryStatus` owns, so on a `PARTITION BY` instance retained for the table's lifetime
# the count never returned to where it started. The assertion is a zero delta rather than a tolerance,
# which a drop in either direction would mask: the metric excludes internal queries, so background work
# cannot move it, and this test is no-parallel, so every non-internal query in the window is one of its
# own and cancels in the difference.
sample_queries() {
    local m=999999 v
    for _ in 1 2 3 4 5; do
        v=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.metrics WHERE metric = 'QueryNonInternal'")
        [ "$v" -lt "$m" ] && m=$v
    done
    echo "$m"
}

for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_leak_$i SYNC"
done
queries_before=$(sample_queries)
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_leak_$i (a Float64, k UInt64) ENGINE = MergeTree ORDER BY k"
    # ALTER, not CREATE: a CREATE analyses the expression on a context carrying no query state, so
    # nothing could be captured there and the case would be vacuous.
    ${CLICKHOUSE_CLIENT} -q "
        ALTER TABLE t_leak_$i ADD INDEX ix length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 7))
        TYPE minmax GRANULARITY 1"
done
queries_after=$(sample_queries)
if [ "$((queries_after - queries_before))" = 0 ]; then
    echo "stored expression leak: no query state retained"
else
    echo "stored expression leak: RETAINED $((queries_after - queries_before)) query states"
fi
for i in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_leak_$i SYNC"
done

# Liveness. With no time limit each function must still return its documented result, so a "fix" that
# simply always threw, or that lost the checkpoint entirely, fails here instead of passing.
${CLICKHOUSE_CLIENT} -q "
    SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4);
    SELECT arrayFold((acc, x) -> acc + x, range(11), toUInt64(0));
    SELECT base58Decode(base58Encode('hello'));
    SELECT sleep(0.01);
    SELECT length(h3PolygonToCells([(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)], 4));
    SELECT length(h3PolygonToCellsWithContainment([(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)], 4, 0));"

# The persisted expressions must also still produce the documented values when nothing is cancelled,
# which the timing cases above cannot show because every one of them ends in a timeout.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_live (a Float64, k UInt64) ENGINE = MergeTree
    PARTITION BY intDiv(length(geohashesInBox(a, a, a + toFloat64(1), a + toFloat64(1), 3)), 100)
    ORDER BY (k, arrayFold((acc, x) -> acc + x, range(3), toUInt64(0)));
    INSERT INTO t_live SELECT number, number FROM numbers(4);
    SELECT count(), sum(k) FROM t_live;
    DROP TABLE t_live SYNC;"
