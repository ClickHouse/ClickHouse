#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
DATA_FILE=$USER_FILES_PATH/test_02104_null.data

echo -e '{"s" : "NULLSome string"}' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02104_null.data', 'JSONStringsEachRow', 's Nullable(String)')"

echo -e '["NULLSome string"]' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02104_null.data', 'JSONCompactStringsEachRow', 's Nullable(String)')"

rm $DATA_FILE

