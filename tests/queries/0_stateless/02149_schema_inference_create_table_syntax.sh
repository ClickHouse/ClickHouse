#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
mkdir $USER_FILES_PATH/test_02149
FILE_NAME=test_02149/data.Parquet
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

$CLICKHOUSE_CLIENT -q "select number as num, concat('Str: ', toString(number)) as str, [number, number + 1] as arr from numbers(10) format Parquet" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "drop table if exists test_02149"
$CLICKHOUSE_CLIENT -q "create table test_02149 engine=File('Parquet', '$FILE_NAME')"
$CLICKHOUSE_CLIENT -q "select * from test_02149"
$CLICKHOUSE_CLIENT -q "drop table test_02149"

$CLICKHOUSE_CLIENT -q "create table test_02149 (x UInt32, s String, a Array(UInt32)) engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into test_02149 select number, toString(number), [number, number + 1] from numbers(10)"

$CLICKHOUSE_CLIENT -q "drop table if exists test_merge"
$CLICKHOUSE_CLIENT -q "create table test_merge engine=Merge(currentDatabase(), 'test_02149')"
$CLICKHOUSE_CLIENT -q "select * from test_merge"
$CLICKHOUSE_CLIENT -q "drop table test_merge"

$CLICKHOUSE_CLIENT -q "drop table if exists test_distributed"
$CLICKHOUSE_CLIENT -q "create table test_distributed engine=Distributed(test_shard_localhost, currentDatabase(), 'test_02149')"
$CLICKHOUSE_CLIENT -q "select * from test_distributed"
$CLICKHOUSE_CLIENT -q "drop table test_distributed"

$CLICKHOUSE_CLIENT -q "drop table if exists test_buffer"
$CLICKHOUSE_CLIENT -q "create table test_buffer engine=Buffer(currentDatabase(), 'test_02149', 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
$CLICKHOUSE_CLIENT -q "select * from test_buffer"
$CLICKHOUSE_CLIENT -q "drop table test_buffer"

rm -rf ${USER_FILES_PATH:?}/test_02149

