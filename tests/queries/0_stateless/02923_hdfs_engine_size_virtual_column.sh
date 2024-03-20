#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data1.tsv') select 1 settings hdfs_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data2.tsv') select 11 settings hdfs_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data3.tsv') select 111 settings hdfs_truncate_on_insert=1;"


$CLICKHOUSE_CLIENT -q "select _size from hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data*.tsv', auto, 'x UInt64') order by _size"
$CLICKHOUSE_CLIENT -q "select _size from hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data*.tsv', auto, 'x UInt64') order by _size"

