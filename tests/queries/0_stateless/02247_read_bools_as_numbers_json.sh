#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02247.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

touch $DATA_FILE

echo -e '{"x" : true}
{"x" : false}'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"

echo -e '{"x" : 42.42}
{"x" : false}'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"

echo -e '{"x" : true}
{"x" : 0.42}'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"


echo -e '[true]
[false]'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRow')"

echo -e '[42.42]
[false]'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRow')"

echo -e '[true]
[0.42]'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRow')"


rm $DATA_FILE
