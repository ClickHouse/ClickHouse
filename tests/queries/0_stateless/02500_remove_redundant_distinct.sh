#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [ -z ${ENABLE_ANALYZER+x} ]; then
    ENABLE_ANALYZER=0
fi

OPTIMIZATION_SETTING="query_plan_remove_redundant_distinct"
DISABLE_OPTIMIZATION="set enable_analyzer=$ENABLE_ANALYZER;SET $OPTIMIZATION_SETTING=0;SET optimize_duplicate_order_by_and_distinct=0"
ENABLE_OPTIMIZATION="set enable_analyzer=$ENABLE_ANALYZER;SET $OPTIMIZATION_SETTING=1;SET optimize_duplicate_order_by_and_distinct=0"

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
) as x,
(
    SELECT DISTINCT number AS n
    FROM numbers(2)
) as y
ORDER BY x.n, y.n"
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
)
ORDER BY a, b"
run_query "$query"

echo "-- DISTINCT duplicates with constant columns"
query="SELECT DISTINCT 2, a, b
FROM
(
    SELECT DISTINCT a, b
    FROM
    (
        SELECT DISTINCT 1, number as a, 2*number as b
        FROM numbers(3)
    )
)
ORDER BY a, b"
run_query "$query"

echo "-- ARRAY JOIN: do _not_ remove outer DISTINCT because new rows are generated between inner and outer DISTINCTs"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM VALUES('Hello', 'World', 'Goodbye')
) AS words
ARRAY JOIN [0, 1] AS arr
ORDER BY c1, arr"
run_query "$query"

echo "-- WITH FILL: do _not_ remove outer DISTINCT because new rows are generated between inner and outer DISTINCTs"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM values('id UInt8', 0, 2)
    ORDER BY id ASC WITH FILL
)"
run_query "$query"

echo "-- WHERE with arrayJoin(): do _not_ remove outer DISTINCT because new rows are generated between inner and outer DISTINCTs"
query="SELECT DISTINCT *
FROM
(
    SELECT DISTINCT ['Istanbul', 'Berlin', 'Bensheim'] AS cities
)
WHERE arrayJoin(cities) IN ['Berlin', 'Bensheim']
ORDER BY cities"
run_query "$query"

echo "-- GROUP BY before DISTINCT with on the same columns => remove DISTINCT"
query="SELECT DISTINCT a
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY before DISTINCT with on different columns => do _not_ remove DISTINCT"
query="SELECT DISTINCT c
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH ROLLUP before DISTINCT with on different columns => do _not_ remove DISTINCT"
query="SELECT DISTINCT c
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH ROLLUP
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH ROLLUP before DISTINCT with on the same columns => remove DISTINCT"
query="SELECT DISTINCT a
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH ROLLUP
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH CUBE before DISTINCT with on different columns => do _not_ remove DISTINCT"
query="SELECT DISTINCT c
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH CUBE
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH CUBE before DISTINCT with on the same columns => remove DISTINCT"
query="SELECT DISTINCT a
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH CUBE
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH TOTALS before DISTINCT with on different columns => do _not_ remove DISTINCT"
query="SELECT DISTINCT c
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH TOTALS
    ORDER BY a
)"
run_query "$query"

echo "-- GROUP BY WITH TOTALS before DISTINCT with on the same columns => remove DISTINCT"
query="SELECT DISTINCT a
FROM
(
    SELECT
        a,
        sum(b) AS c
    FROM
    (
        SELECT
            x.number AS a,
            y.number AS b
        FROM numbers(3) AS x, numbers(3, 3) AS y
    )
    GROUP BY a WITH TOTALS
    ORDER BY a
)"
run_query "$query"

echo "-- DISTINCT COUNT() with GROUP BY => do _not_ remove DISTINCT"
query="select distinct count() from numbers(10) group by number"
run_query "$query"

echo "-- UNION ALL with DISTINCT => do _not_ remove DISTINCT"
query="SELECT DISTINCT number
FROM
(
    SELECT DISTINCT number
    FROM numbers(1)
    UNION ALL
    SELECT DISTINCT number
    FROM numbers(2)
)
ORDER BY number"
run_query "$query"
