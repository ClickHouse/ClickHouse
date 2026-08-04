#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [ -z ${ENABLE_ANALYZER+x} ]; then
    ENABLE_ANALYZER=0
fi

DISABLE_OPTIMIZATION="SET enable_analyzer=$ENABLE_ANALYZER;SET query_plan_remove_redundant_sorting=0;SET optimize_duplicate_order_by_and_distinct=0"
ENABLE_OPTIMIZATION="SET enable_analyzer=$ENABLE_ANALYZER;SET query_plan_remove_redundant_sorting=1;SET optimize_duplicate_order_by_and_distinct=0"

echo "-- Disabled query_plan_remove_redundant_sorting"
echo "-- ORDER BY clauses in subqueries are untouched"
query="SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
ORDER BY number ASC"
$CLICKHOUSE_CLIENT -q "$DISABLE_OPTIMIZATION;EXPLAIN $query"

function run_query {
    echo "-- query"
    echo "$1"
    echo "-- explain"
    $CLICKHOUSE_CLIENT -q "$ENABLE_OPTIMIZATION;EXPLAIN $1"
    echo "-- execute"
    $CLICKHOUSE_CLIENT -q "$ENABLE_OPTIMIZATION;$1"
}

echo "-- Enabled query_plan_remove_redundant_sorting"
echo "-- ORDER BY removes ORDER BY clauses in subqueries"
run_query "$query"

echo "-- ORDER BY cannot remove ORDER BY in subquery WITH FILL"
query="SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number DESC
    )
    ORDER BY number ASC WITH FILL STEP 1
)
ORDER BY number ASC"
run_query "$query"

echo "-- ORDER BY cannot remove ORDER BY in subquery with LIMIT BY"
query="SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number DESC
    )
    ORDER BY number ASC
    LIMIT 1 BY number
)
ORDER BY number ASC"
run_query "$query"

echo "-- CROSS JOIN with subqueries, nor ORDER BY nor GROUP BY in main query -> only ORDER BY clauses in most inner subqueries will be removed"
query="SELECT *
FROM
(
    SELECT number
    FROM
    (
        SELECT number
        FROM numbers(3)
        ORDER BY number DESC
    )
    ORDER BY number ASC
) AS t1,
(
    SELECT number
    FROM
    (
        SELECT number
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
) AS t2
ORDER BY t1.number, t2.number"
run_query "$query"

echo "-- CROSS JOIN with subqueries, ORDER BY in main query -> all ORDER BY clauses will be removed in subqueries"
query="SELECT *
FROM
(
    SELECT number
    FROM
    (
        SELECT number
        FROM numbers(3)
        ORDER BY number DESC
    )
    ORDER BY number ASC
) AS t1,
(
    SELECT number
    FROM
    (
        SELECT number
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
) AS t2
ORDER BY t1.number, t2.number"
run_query "$query"

echo "-- GROUP BY with aggregation function which does NOT depend on order -> eliminate ORDER BY(s) in _all_ subqueries"
query="SELECT sum(number)
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
GROUP BY number
ORDER BY number"
run_query "$query"

echo "-- GROUP BY with aggregation function which depends on order -> keep ORDER BY in first subquery, and eliminate in second subquery"
query="SELECT any(number)
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
GROUP BY number
ORDER BY number
SETTINGS optimize_aggregators_of_group_by_keys=0 -- avoid removing any() as it depends on order and we need it for the test"
run_query "$query"

echo "-- query with aggregation function but w/o GROUP BY -> remove sorting"
query="SELECT sum(number)
FROM
(
    SELECT *
    FROM numbers(10)
    ORDER BY number DESC
)"
run_query "$query"

echo "-- check that optimization is applied recursively to subqueries as well"
echo "-- GROUP BY with aggregation function which does NOT depend on order -> eliminate ORDER BY in most inner subquery here"
query="SELECT a
FROM
(
    SELECT sum(number) AS a
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    GROUP BY number
)
ORDER BY a ASC"
run_query "$query"

echo "-- GROUP BY with aggregation function which depends on order -> ORDER BY in subquery is kept due to the aggregation function"
query="SELECT a
FROM
(
    SELECT any(number) AS a
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    GROUP BY number
)
ORDER BY a ASC
SETTINGS optimize_aggregators_of_group_by_keys=0 -- avoid removing any() as it depends on order and we need it for the test"
run_query "$query"

echo "-- Check that optimization works for subqueries as well, - main query have neither ORDER BY nor GROUP BY"
query="SELECT a
FROM
(
    SELECT any(number) AS a
    FROM
    (
        SELECT *
        FROM
        (
            SELECT *
            FROM numbers(3)
            ORDER BY number DESC
        )
        ORDER BY number ASC
    )
    GROUP BY number
)
WHERE a > 0
ORDER BY a
SETTINGS optimize_aggregators_of_group_by_keys=0 -- avoid removing any() as it depends on order and we need it for the test"
run_query "$query"

echo "-- GROUP BY in most inner query makes execution parallelized, and removing inner sorting steps will keep it that way. But need to correctly update data streams sorting properties after removing sorting steps"
query="SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        GROUP BY number
        ORDER BY number ASC
    )
    ORDER BY number ASC
)
ORDER BY number ASC"
run_query "$query"

echo "-- sum() with Floats depends on order, -> sorting is not removed here"
query="SELECT
    toTypeName(sum(v)),
    sum(v)
FROM
(
    SELECT v
    FROM
    (
        SELECT CAST('9007199254740992', 'Float64') AS v
        UNION ALL
        SELECT CAST('1', 'Float64') AS v
        UNION ALL
        SELECT CAST('1', 'Float64') AS v
    )
    ORDER BY v ASC
)"
run_query "$query"

echo "-- sum() with Nullable(Floats) depends on order, -> sorting is not removed here"
query="SELECT
    toTypeName(sum(v)),
    sum(v)
FROM
(
    SELECT v
    FROM
    (
        SELECT '9007199254740992'::Nullable(Float64) AS v
        UNION ALL
        SELECT '1'::Nullable(Float64) AS v
        UNION ALL
        SELECT '1'::Nullable(Float64) AS v
    )
    ORDER BY v ASC
)"
run_query "$query"

echo "-- sumIf() with Floats depends on order, -> sorting is not removed here"
query="SELECT
    toTypeName(sumIf(v, v > 0)),
    sumIf(v, v > 0)
FROM
(
    SELECT v
    FROM
    (
        SELECT CAST('9007199254740992', 'Float64') AS v
        UNION ALL
        SELECT CAST('1', 'Float64') AS v
        UNION ALL
        SELECT CAST('1', 'Float64') AS v
    )
    ORDER BY v ASC
)"
run_query "$query"

echo "-- presence of an inner OFFSET retains the ORDER BY"
query="WITH
  t1 AS (
    SELECT a, b
    FROM
      VALUES (
        'b UInt32, a Int32',
        (1, 1),
        (2, 0)
      )
  )
SELECT
  SUM(a)
FROM (
  SELECT a, b
  FROM t1
  ORDER BY 1 DESC, 2
  OFFSET 1
) t2"
run_query "$query"

echo "-- disable common optimization to avoid functions to be lifted up (liftUpFunctions optimization), needed for testing with stateful function"
ENABLE_OPTIMIZATION="SET query_plan_enable_optimizations=0;$ENABLE_OPTIMIZATION"
echo "-- neighbor() as stateful function prevents removing inner ORDER BY since its result depends on order"
query="SELECT
    number,
    neighbor(number, 2)
FROM
(
    SELECT *
    FROM numbers(10)
    ORDER BY number DESC
)
ORDER BY number ASC
SETTINGS allow_deprecated_error_prone_window_functions = 1"
run_query "$query"

echo "-- non-stateful function does _not_ prevent removing inner ORDER BY"
query="SELECT
    number,
    plus(number, 2)
FROM
(
    SELECT *
    FROM numbers(10)
    ORDER BY number DESC
)"
run_query "$query"
