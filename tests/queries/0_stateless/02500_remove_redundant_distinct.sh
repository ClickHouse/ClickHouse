#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

OPTIMIZATION_SETTING="query_plan_remove_redundant_distinct"
DISABLE_OPTIMIZATION="SET allow_experimental_analyzer=1;SET $OPTIMIZATION_SETTING=0;SET optimize_duplicate_order_by_and_distinct=0"
ENABLE_OPTIMIZATION="SET allow_experimental_analyzer=1;SET $OPTIMIZATION_SETTING=1;SET optimize_duplicate_order_by_and_distinct=0"

echo "-- Disabled $OPTIMIZATION_SETTING"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM
    (
        SELECT DISTINCT *
        FROM numbers(3)
    )
)"

$CLICKHOUSE_CLIENT -nq "$DISABLE_OPTIMIZATION;EXPLAIN $query"

function run_query {
    echo "-- query"
    echo "$1"
    echo "-- explain"
    $CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;EXPLAIN $1"
    echo "-- execute"
    $CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;$1"
}

echo "-- Enabled $OPTIMIZATION_SETTING"
echo "-- DISTINCT is only in most inner subquery"
run_query "$query"

echo "-- do _not_ remove DISTINCT after UNION"
query="SELECT DISTINCT number FROM
(
    (SELECT DISTINCT number FROM numbers(1))
    UNION ALL
    (SELECT DISTINCT number FROM numbers(2))
)
ORDER BY number"
run_query "$query"

echo "-- do _not_ remove DISTINCT after JOIN"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT number AS n
    FROM numbers(2)
),
(
    SELECT DISTINCT number AS n
    FROM numbers(2)
) SETTINGS joined_subquery_requires_alias=0"
run_query "$query"

echo "-- DISTINCT duplicates with several columns"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM
    (
        SELECT DISTINCT number as a, 2*number as b
        FROM numbers(3)
    )
)"
run_query "$query"
