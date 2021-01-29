#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

function drop_database()
{
    # redirect stderr since it is racy with DROP TABLE
    # and tries to remove db_01516.data too.
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS db_01516" 2>/dev/null
}

function drop_table()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS db_01516.data3;" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS db_01516.data1;" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS db_01516.data2;" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
}

function create()
{
    ${CLICKHOUSE_CLIENT} -q "CREATE DATABASE IF NOT EXISTS db_01516;"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE IF NOT EXISTS db_01516.data1 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE IF NOT EXISTS db_01516.data2 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE IF NOT EXISTS db_01516.data3 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed"
}

for _ in {1..100}; do
    create
    drop_table &
    drop_database &
    wait
done
