#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data1.tsv') select 1 settings hdfs_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data2.tsv') select 2 settings hdfs_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "insert into table function hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data3.tsv') select 3 settings hdfs_truncate_on_insert=1;"


$CLICKHOUSE_CLIENT --print-profile-events -q "select * from hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data*.tsv', auto, 'x UInt64') where _file like '%data1%' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"

$CLICKHOUSE_CLIENT --print-profile-events -q "select * from hdfs('hdfs://localhost:12222/$CLICKHOUSE_TEST_UNIQUE_NAME.data*.tsv', auto, 'x UInt64') where _path like '%data1%' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"
