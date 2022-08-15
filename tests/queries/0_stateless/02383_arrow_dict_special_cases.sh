#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p $USER_FILES_PATH/test_02383
cp $CURDIR/data_arrow/dictionary*.arrow $USER_FILES_PATH/test_02383/
cp $CURDIR/data_arrow/corrupted.arrow $USER_FILES_PATH/test_02383/

$CLICKHOUSE_CLIENT -q "desc file('test_02383/dictionary1.arrow')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02383/dictionary1.arrow')"
$CLICKHOUSE_CLIENT -q "desc file('test_02383/dictionary2.arrow')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02383/dictionary2.arrow')"
$CLICKHOUSE_CLIENT -q "desc file('test_02383/dictionary3.arrow')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02383/dictionary3.arrow')"

$CLICKHOUSE_CLIENT -q "desc file('test_02383/corrupted.arrow')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02383/corrupted.arrow')" 2>&1 | grep -F -q "INCORRECT_DATA" && echo OK || echo FAIL


rm -rf $USER_FILES_PATH/test_02383
