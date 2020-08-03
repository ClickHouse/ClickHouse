#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01114_1"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01114_2"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_01114_3"


$CLICKHOUSE_CLIENT --allow_experimental_database_atomic=1 -q "CREATE DATABASE test_01114_1 ENGINE=Atomic"
$CLICKHOUSE_CLIENT --default_database_engine=Atomic --allow_experimental_database_atomic=1 -q "CREATE DATABASE test_01114_2"
$CLICKHOUSE_CLIENT --default_database_engine=Ordinary -q "CREATE DATABASE test_01114_3"

$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_01114_1"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_01114_2"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_01114_3"
$CLICKHOUSE_CLIENT -q "SELECT name, engine, splitByChar('/', data_path)[-2], splitByChar('/', metadata_path)[-3], splitByChar('/', metadata_path)[-2] FROM system.databases WHERE name LIKE 'test_01114_%'"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01114_1.mt_tmp (n UInt64) ENGINE=MergeTree() ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01114_1.mt_tmp SELECT * FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01114_3.mt (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5)"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01114_3.mt SELECT * FROM numbers(20)"

$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01114_1.mt_tmp TO test_01114_3.mt_tmp"   # move from Atomic to Ordinary
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01114_3.mt TO test_01114_1.mt"           # move from Ordinary to Atomic
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01114_1.mt"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01114_3.mt_tmp"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01114_3"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01114_2.mt UUID '00001114-0000-4000-8000-000000000002' (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5)"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE test_01114_2.mt"
$CLICKHOUSE_CLIENT -q "SELECT name, uuid, create_table_query FROM system.tables WHERE database='test_01114_2'"


$CLICKHOUSE_CLIENT -q "SELECT count(col), sum(col) FROM (SELECT n + sleepEachRow(1.5) AS col FROM test_01114_1.mt)" &     # 30s, result: 20, 190
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01114_2.mt SELECT number + sleepEachRow(1.5) FROM numbers(30)" &                  # 45s
sleep 1   # SELECT and INSERT should start before the following RENAMEs

$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01114_1.mt TO test_01114_1.mt_tmp"
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01114_1.mt_tmp TO test_01114_2.mt_tmp"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES test_01114_2.mt AND test_01114_2.mt_tmp"
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_01114_2.mt_tmp TO test_01114_1.mt"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES test_01114_1.mt AND test_01114_2.mt"

# Check that nothing changed
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01114_1.mt"
uuid_mt1=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database='test_01114_1' AND name='mt'")
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE test_01114_1.mt" | sed "s/$uuid_mt1/00001114-0000-4000-8000-000000000001/g"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE test_01114_2.mt"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_01114_1.mt"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01114_1.mt (s String) ENGINE=Log()"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01114_1.mt SELECT 's' || toString(number) FROM numbers(5)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01114_1.mt"   # result: 5

$CLICKHOUSE_CLIENT -q "SELECT tuple(s, sleepEachRow(3)) FROM test_01114_1.mt" > /dev/null &    # 15s
sleep 1
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01114_1" && echo "dropped"

wait # for INSERT

$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM test_01114_2.mt"    # result: 30, 435
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01114_2"
