#!/usr/bin/env bash
# Tags: long, no-sanitizers, no-flaky-check
# Test for https://github.com/ClickHouse/ClickHouse/issues/32465
# Enormously large query is slow when run from a Merge table with many underlying tables.
# Pre-fix, the query tree was cloned per underlying table, so planning was O(N * query_complexity).
# Post-fix, planning on the Merge table is close to single-underlying-table planning,
# so this test compares the two latencies and asserts a bounded ratio.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_TABLES=200

# Create underlying tables (batched in a single connection to avoid 200 client startups).
CREATE_QUERIES=""
for i in $(seq 0 $((NUM_TABLES - 1))); do
    CREATE_QUERIES+="CREATE TABLE ${CLICKHOUSE_DATABASE}.t_merge_perf_${i} (date Date, category String, value Int64, customer_id String) ENGINE = MergeTree ORDER BY (date, category);"
done
$CLICKHOUSE_CLIENT -nm -q "$CREATE_QUERIES"

# Create Merge table
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_merge_perf_all (date Date, category String, value Int64, customer_id String) ENGINE = Merge('${CLICKHOUSE_DATABASE}', '^t_merge_perf_\\\\d+\$')"

# Insert a small amount of data into one table (we care about planning time, not data processing)
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}.t_merge_perf_0 SELECT
    toDate('2021-10-04') + number % 70,
    ['auto', 'appliances', 'garden', 'children', 'home', 'hobbies', 'electronics', 'books'][number % 8 + 1],
    number,
    toString(number % 1000)
FROM numbers(1000)"

# Build a large multiIf expression block (simulating the original issue with ~16 categories)
CATEGORIES=("auto" "appliances" "garden" "children" "home" "hobbies" "electronics" "books"
            "computers" "beauty" "equipment" "clothing" "food" "sports" "construction" "health")

build_multiif() {
    local field=$1
    local result="multiIf("
    for cat in "${CATEGORIES[@]}"; do
        result+="position(${field}, '${cat}') > 0, '${cat}_mapped', "
    done
    result+="'other')"
    echo "$result"
}

MULTIIF_CAT=$(build_multiif "category")

# Build the large query with repeated multiIf expressions in both SELECT and WHERE
# This simulates the original ~1260-line machine-generated query
QUERY="SELECT
    count(),
    sum(value),
    uniqExact(customer_id),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 0, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 1, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 2, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 3, customer_id,
        ''
    )),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 10, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 11, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 12, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 13, customer_id,
        ''
    )),
    sum(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 0, value,
        ${MULTIIF_CAT} >= 'b' AND value > 1, value,
        ${MULTIIF_CAT} >= 'c' AND value > 2, value,
        ${MULTIIF_CAT} >= 'd' AND value > 3, value,
        0
    )),
    sum(multiIf(
        ${MULTIIF_CAT} >= 'e' AND value > 0, value,
        ${MULTIIF_CAT} >= 'f' AND value > 1, value,
        ${MULTIIF_CAT} >= 'g' AND value > 2, value,
        ${MULTIIF_CAT} >= 'h' AND value > 3, value,
        0
    )),
    uniqExact(multiIf(
        ${MULTIIF_CAT} >= 'a' AND value > 100, customer_id,
        ${MULTIIF_CAT} >= 'b' AND value > 101, customer_id,
        ${MULTIIF_CAT} >= 'c' AND value > 102, customer_id,
        ${MULTIIF_CAT} >= 'd' AND value > 103, customer_id,
        ''
    )),
    sum(toInt64(multiIf(
        ${MULTIIF_CAT} >= 'a', value * 2,
        ${MULTIIF_CAT} >= 'b', value * 3,
        ${MULTIIF_CAT} >= 'c', value * 4,
        ${MULTIIF_CAT} >= 'd', value * 5,
        0
    )))
FROM ${CLICKHOUSE_DATABASE}.t_merge_perf_all
WHERE
    date >= '2021-10-04' AND date <= '2021-12-13'
    AND ${MULTIIF_CAT} >= 'a'
    AND ${MULTIIF_CAT} <= 'z'
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        position(category, 'home') > 0, 5,
        position(category, 'hobbies') > 0, 6,
        position(category, 'electronics') > 0, 7,
        position(category, 'books') > 0, 8,
        position(category, 'computers') > 0, 9,
        position(category, 'beauty') > 0, 10,
        position(category, 'equipment') > 0, 11,
        position(category, 'clothing') > 0, 12,
        position(category, 'food') > 0, 13,
        position(category, 'sports') > 0, 14,
        position(category, 'construction') > 0, 15,
        position(category, 'health') > 0, 16,
        0
    ) >= 1
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        position(category, 'home') > 0, 5,
        position(category, 'hobbies') > 0, 6,
        position(category, 'electronics') > 0, 7,
        position(category, 'books') > 0, 8,
        position(category, 'computers') > 0, 9,
        position(category, 'beauty') > 0, 10,
        position(category, 'equipment') > 0, 11,
        position(category, 'clothing') > 0, 12,
        position(category, 'food') > 0, 13,
        position(category, 'sports') > 0, 14,
        position(category, 'construction') > 0, 15,
        position(category, 'health') > 0, 16,
        0
    ) <= 16
GROUP BY
    addDays(CAST(date, 'Date'), -1 * (((7 + if(toDayOfWeek(date) = 7, 1, toDayOfWeek(date) + 1)) - 2) % 7))
FORMAT Null"

# Compare planning time on the Merge table against a single underlying table.
# Pre-fix, planning was O(N * query_tree_size), so merge-table latency scaled with N.
# With N=200 underlying tables this would be ~100x-200x slower than a single-table query.
# Post-fix, the Merge planner shares the cloned query tree across tables of identical
# structure, so merge-table latency stays close to single-table latency.
# A loose 20x ratio is enough to catch the regression while tolerating CI noise.
# The single-table timed run is fast (~250-400ms) and small absolute fluctuations cause
# large ratio swings under parallel CI load, so the threshold needs comfortable margin.
QUERY_SINGLE_TIMING="${QUERY/t_merge_perf_all/t_merge_perf_0}"

# Warm up the data-side cache with a small query so the timed runs measure planning.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_merge_perf_0 FORMAT Null"

T_START=$(date +%s%N)
$CLICKHOUSE_CLIENT --max_query_size 1048576 --max_execution_time 30 -q "$QUERY" >/dev/null || { echo "FAIL: query on merge table timed out" >&2; exit 1; }
T_END=$(date +%s%N)
TIME_MERGE_NS=$((T_END - T_START))

T_START=$(date +%s%N)
$CLICKHOUSE_CLIENT --max_query_size 1048576 --max_execution_time 30 -q "$QUERY_SINGLE_TIMING" >/dev/null || { echo "FAIL: query on single table timed out" >&2; exit 1; }
T_END=$(date +%s%N)
TIME_SINGLE_NS=$((T_END - T_START))

if [ "$TIME_MERGE_NS" -gt $((TIME_SINGLE_NS * 20)) ]; then
    echo "FAIL: merge-table query (${TIME_MERGE_NS}ns) is more than 20x slower than single-table query (${TIME_SINGLE_NS}ns)" >&2
    exit 1
fi
echo "OK"

# Also verify correctness: same query on single table vs merge table should produce equal results.
QUERY_RESULT="SELECT
    count(),
    sum(value),
    uniqExact(customer_id)
FROM ${CLICKHOUSE_DATABASE}.t_merge_perf_all
WHERE
    date >= '2021-10-04' AND date <= '2021-12-13'
    AND multiIf(
        position(category, 'auto') > 0, 1,
        position(category, 'appliances') > 0, 2,
        position(category, 'garden') > 0, 3,
        position(category, 'children') > 0, 4,
        0
    ) >= 1"

QUERY_SINGLE="${QUERY_RESULT/t_merge_perf_all/t_merge_perf_0}"

RESULT_MERGE=$($CLICKHOUSE_CLIENT -q "$QUERY_RESULT")
RESULT_SINGLE=$($CLICKHOUSE_CLIENT -q "$QUERY_SINGLE")

if [ "$RESULT_MERGE" = "$RESULT_SINGLE" ]; then
    echo "OK"
else
    echo "FAIL: merge table result differs from single table" >&2
    echo "Merge: $RESULT_MERGE" >&2
    echo "Single: $RESULT_SINGLE" >&2
    exit 1
fi

# Cleanup (batched in a single connection).
DROP_QUERIES="DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_merge_perf_all;"
for i in $(seq 0 $((NUM_TABLES - 1))); do
    DROP_QUERIES+="DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_merge_perf_${i};"
done
$CLICKHOUSE_CLIENT -nm -q "$DROP_QUERIES"
