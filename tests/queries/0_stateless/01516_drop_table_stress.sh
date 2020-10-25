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
    ${CLICKHOUSE_CLIENT} -nm <<EOL
DROP TABLE IF EXISTS db_01516.data3;
DROP TABLE IF EXISTS db_01516.data1;
DROP TABLE IF EXISTS db_01516.data2;
EOL
}

function create()
{
    ${CLICKHOUSE_CLIENT} -nm <<EOL
CREATE DATABASE IF NOT EXISTS db_01516;
CREATE TABLE IF NOT EXISTS db_01516.data1 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);
CREATE TABLE IF NOT EXISTS db_01516.data2 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);
CREATE TABLE IF NOT EXISTS db_01516.data3 Engine=MergeTree() ORDER BY number AS SELECT * FROM numbers(1);
EOL
}

for _ in {1..100}; do
    create
    drop_table &
    drop_database &
    wait
done
