#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# 1. init
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192_renamed"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01192_atomic"

$CLICKHOUSE_CLIENT --default_database_engine=Ordinary -q "CREATE DATABASE test_01192 UUID '00001192-0000-4000-8000-000000000001'" 2>&1| grep -F "does not support" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT --default_database_engine=Atomic -q "CREATE DATABASE test_01192 UUID '00001192-0000-4000-8000-000000000001'"

# 2. check metadata
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "SELECT engine, splitByChar('/', data_path)[-2], uuid, splitByChar('/', metadata_path)[-2] FROM system.databases WHERE name='test_01192'"

# 3. check RENAME don't wait for INSERT
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.mt (n UInt64) ENGINE=MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192.mt SELECT number + sleepEachRow(1.5) FROM numbers(10)" && echo "inserted" &
sleep 1

$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO default" 2>&1| grep -F "already exists" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192_notexisting TO test_01192_renamed" 2>&1| grep -F "doesn't exist" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO test_01192_renamed" && echo "renamed"
wait

# 4. check metadata after RENAME
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE DATABASE test_01192_renamed"
$CLICKHOUSE_CLIENT -q "SELECT engine, splitByChar('/', data_path)[-2], uuid, splitByChar('/', metadata_path)[-2] FROM system.databases WHERE name='test_01192_renamed'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_01192_renamed"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_renamed.mt"

# 5. check moving tables from Ordinary to Atomic (can be used to "alter" database engine)
$CLICKHOUSE_CLIENT --default_database_engine=Ordinary -q "CREATE DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.mt AS test_01192_renamed.mt ENGINE=MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.rmt AS test_01192_renamed.mt ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/', '1') ORDER BY n"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW test_01192.mv TO test_01192.rmt AS SELECT * FROM test_01192.mt"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192.mt SELECT number FROM numbers(10)" && echo "inserted"

$CLICKHOUSE_CLIENT --default_database_engine=Atomic -q "CREATE DATABASE test_01192_atomic"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192_renamed"
# it's blocking
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01192.mt TO test_01192_atomic.mt, test_01192.rmt TO test_01192_atomic.rmt, test_01192.mv TO test_01192_atomic.mv" && echo "renamed"

# 6. check data after RENAME
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.rmt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192_atomic.mv" 2>&1| grep -F "doesn't exist" > /dev/null && echo "ok"

# 7. create dictionary and check it
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01192.mt (n UInt64, _part String) ENGINE=Memory" # mock
$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY test_01192_atomic.dict UUID '00001192-0000-4000-8000-000000000002' (n UInt64, _part String DEFAULT 'no') PRIMARY KEY n LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'mt' DB 'test_01192'))"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE DICTIONARY test_01192_atomic.dict"
$CLICKHOUSE_CLIENT -q "SELECT database, name, status, origin FROM system.dictionaries WHERE uuid='00001192-0000-4000-8000-000000000002'"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('test_01192_atomic.dict', '_part', toUInt64(1))"

# 8. check RENAME don't wait for INSERT
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01192_atomic.mt SELECT number + sleepEachRow(1) + 10 FROM numbers(10)" && echo "inserted" &
sleep 1

$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192 TO test_01192_renamed" 2>&1| grep -F "not supported" > /dev/null && echo "ok"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE test_01192_atomic TO test_01192" && echo "renamed"
wait

# 9. check data after RENAME
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.rmt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01192.mv"
$CLICKHOUSE_CLIENT -q "SELECT database, name, status, origin FROM system.dictionaries WHERE uuid='00001192-0000-4000-8000-000000000002'"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('test_01192.dict', '_part', toUInt64(1))"
$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD DICTIONARY test_01192.dict"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('test_01192.dict', '_part', toUInt64(10))"
$CLICKHOUSE_CLIENT -q "SELECT database, name, status, origin FROM system.dictionaries WHERE uuid='00001192-0000-4000-8000-000000000002'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01192"

$CLICKHOUSE_CLIENT -q "SELECT database, name, status, origin FROM system.dictionaries WHERE uuid='00001192-0000-4000-8000-000000000002'" # 0 rows
