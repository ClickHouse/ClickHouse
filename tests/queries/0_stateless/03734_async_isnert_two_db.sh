#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


CH_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--database=${CLICKHOUSE_DATABASE}"'//g')
CH_CLIENT="${CH_CLIENT} --async_insert_use_adaptive_busy_timeout=0 --async_insert_busy_timeout_min_ms=1000 --async_insert_busy_timeout_max_ms=5000"

db1="test_async_insert_two_db_1_$CLICKHOUSE_DATABASE"
db3="test_async_insert_two_db_2_$CLICKHOUSE_DATABASE"

create_query="""
CREATE TABLE IF NOT EXISTS test_async_insert_two_db_table
(
    id UInt32,
    val String
)
ENGINE = MergeTree()
ORDER BY id;
"""

for db in $db1 $db3; do
    echo "Creating database ${db%$CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $db"
    $CLICKHOUSE_CLIENT --query "CREATE DATABASE IF NOT EXISTS $db"
    echo "Creating table in database ${db%$CLICKHOUSE_DATABASE}"
    $CH_CLIENT --database=$db --query "$create_query"
done

for db in $db1 $db3; do
    echo "Inserting data into database ${db%$CLICKHOUSE_DATABASE}"
    $CH_CLIENT --database=$db --async_insert=1 --wait_for_async_insert=0 --query "INSERT INTO test_async_insert_two_db_table VALUES (1, 'one'), (2, 'two'), (3, 'three');"
    $CH_CLIENT --database=$db --async_insert=1 --wait_for_async_insert=0 --query "INSERT INTO test_async_insert_two_db_table VALUES (1, 'one'), (2, 'two'), (3, 'three');"
    $CH_CLIENT --database=$db --async_insert=1 --wait_for_async_insert=0 --query "INSERT INTO test_async_insert_two_db_table VALUES (3, 'three'), (4, 'four'), (5, 'five');"
done

for db in $db1 $db3; do
    echo "Flushing inserts in ${db%$CLICKHOUSE_DATABASE}"
    $CH_CLIENT --database=$db "system flush async insert queue test_async_insert_two_db_table;"
done

for db in $db1 $db3; do
    echo "select table in ${db%$CLICKHOUSE_DATABASE}"
    $CH_CLIENT --database=$db "select count() from test_async_insert_two_db_table;"
done

for db in $db1 $db3; do
    echo "Dropping database ${db%$CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $db;"
done
