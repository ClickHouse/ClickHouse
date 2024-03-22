#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

DATA_FILE=$USER_FILES_PATH/test_02103.data

FORMATS=('TSVWithNames' 'TSVWithNamesAndTypes' 'TSVRawWithNames' 'TSVRawWithNamesAndTypes' 'CSVWithNames' 'CSVWithNamesAndTypes' 'JSONCompactEachRowWithNames' 'JSONCompactEachRowWithNamesAndTypes')

for format in "${FORMATS[@]}"
do
    $CLICKHOUSE_CLIENT -q "SELECT number, range(number + 10) AS array, toString(number) AS string FROM numbers(10) FORMAT $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02103.data', '$format', 'number UInt64, array Array(UInt64), string String') SETTINGS input_format_parallel_parsing=1, min_chunk_bytes_for_parallel_parsing=40"
done

rm $DATA_FILE

