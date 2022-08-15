#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

cp $CURDIR/data_avro/corrupted.avro $USER_FILES_PATH/

$CLICKHOUSE_CLIENT -q "select * from file(corrupted.avro, Avro, 'x String')" 2>&1 | grep -F -q "Cannot read compressed data" && echo "OK" || echo "FAIL"

