#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail


echo "
    DROP TABLE if exists tableA;
    DROP TABLE if exists tableB;

    create table tableA (id UInt64, col1 UInt64, colDate Date) engine = ReplacingMergeTree(colDate, id, 8192);
    create table tableB (id UInt64, Aid UInt64, colDate Date) engine = ReplacingMergeTree(colDate, id, 8192);

    insert into tableA select number, number % 10, addDays(toDate('2020-01-01'), - number % 1000) from numbers(100000);
    insert into tableB select number, number % 100000, addDays(toDate('2020-01-01'), number % 90) from numbers(50000000);
" | $CLICKHOUSE_CLIENT -n

echo "
SELECT tableName
FROM
    (
        SELECT
            col1,
            'T1_notJoin1' AS tableName,
            count(*) AS c
        FROM tableA
        GROUP BY col1
        UNION ALL
        SELECT
            a.col1,
            'T2_filteredAfterJoin1' AS tableName,
            count(*) AS c
        FROM tableB AS b
        INNER JOIN tableA AS a ON a.id = b.Aid
        WHERE b.colDate = '2020-01-01'
        GROUP BY a.col1
        UNION ALL
        SELECT
            a.col1,
            'T3_filteredAfterJoin2' AS tableName,
            count(*) AS c
        FROM tableB AS b
            INNER JOIN
            tableA AS a
            ON a.id = b.Aid
        WHERE b.colDate = '2020-01-02'
        GROUP BY a.col1
        UNION ALL
        SELECT
            a.col1,
            'T4_filteredBeforeJoin1' AS tableName,
            count(*) AS c
        FROM tableA AS a
        INNER JOIN
        (
            SELECT
                Aid
            FROM tableB
            WHERE colDate = '2020-01-01'
        ) AS b ON a.id = b.Aid
        GROUP BY a.col1
        UNION ALL
        SELECT
            a.col1,
            'T5_filteredBeforeJoin2' AS tableName,
            count(*) AS c
        FROM tableA AS a
        INNER JOIN
        (
            SELECT
                Aid
            FROM tableB
            WHERE colDate = '2020-01-02'
        ) AS b ON a.id = b.Aid
        GROUP BY a.col1
        UNION ALL
        SELECT
            a.col1,
            'T6_filteredAfterJoin3' AS tableName,
            count(*) AS c
        FROM tableB AS b
        INNER JOIN tableA AS a ON a.id = b.Aid
        WHERE b.colDate = '2020-01-03'
        GROUP BY a.col1
        UNION ALL
        SELECT
            col1,
            'T7_notJoin2' AS tableName,
            count(*) AS c
        FROM tableA
        GROUP BY col1
        UNION ALL
        SELECT
            a.col1,
            'T8_filteredBeforeJoin3' AS tableName,
            count(*) AS c
        FROM tableA AS a
        INNER JOIN
        (
            SELECT
                Aid
            FROM tableB
            WHERE colDate = '2020-01-03'
        ) AS b ON a.id = b.Aid
        GROUP BY a.col1
    ) AS a
GROUP BY tableName
ORDER BY tableName ASC;
" | $CLICKHOUSE_CLIENT -n | wc -l

echo "
    DROP TABLE tableA;
    DROP TABLE tableB;
" | $CLICKHOUSE_CLIENT -n
