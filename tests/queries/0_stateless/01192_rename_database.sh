#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192_renamed"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192_atomic"

$CLICKHOUSE_CLIENT --default_database_engine=Ordinary -q "CREATE DATABASE test_01192 UUID '00001192-0000-4000-8000-000000000001'" 2>&1| grep -F "does not support" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT --allow_experimental_database_atomic=1 --default_database_engine=Atomic -q "CREATE DATABASE test_01192 UUID '00001192-0000-4000-8000-000000000001'"

$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "SELECT engine, splitByChar('/', data_path)[-2], uuid, splitByChar('/', metadata_path)[-2] FROM system.databases WHERE name='test_01192'"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.mt (n UInt64) ENGINE=MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192.mt SELECT number + sleepEachRow(1.5) FROM numbers(10)" && echo "inserted" &
sleep 1

$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO default" 2>&1| grep -F "already exists" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192_notexisting TO test_01192_renamed" 2>&1| grep -F "doesn't exist" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO test_01192_renamed" && echo "renamed"
wait

$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE DATABASE test_01192_renamed"
$CLICKHOUSE_CLIENT -q "SELECT engine, splitByChar('/', data_path)[-2], uuid, splitByChar('/', metadata_path)[-2] FROM system.databases WHERE name='test_01192_renamed'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_01192_renamed"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_renamed.mt"

$CLICKHOUSE_CLIENT --default_database_engine=Ordinary -q "CREATE DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.mt AS test_01192_renamed.mt ENGINE=MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.rmt AS test_01192_renamed.mt ENGINE=ReplicatedMergeTree('/test/01192/', '1') ORDER BY n"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW test_01192.mv TO test_01192.rmt AS SELECT * FROM test_01192.mt"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192.mt SELECT number FROM numbers(10)" && echo "inserted"

$CLICKHOUSE_CLIENT --allow_experimental_database_atomic=1 --default_database_engine=Atomic -q "CREATE DATABASE test_01192_atomic"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192_renamed"
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01192.mt TO test_01192_atomic.mt, test_01192.rmt TO test_01192_atomic.rmt, test_01192.mv TO test_01192_atomic.mv" && echo "renamed"

$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.rmt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.mv" 2>&1| grep -F "doesn't exist" > /dev/null && echo "ok"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192_atomic.mt SELECT number + sleepEachRow(1) + 10 FROM numbers(10)" && echo "inserted" &
sleep 1

$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO test_01192_renamed" 2>&1| grep -F "not supported" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192_atomic TO test_01192" && echo "renamed"
wait

$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.rmt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.mv"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192"
