#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$USER_FILES_PATH/${CLICKHOUSE_DATABASE}.data

FORMATS=('TSVWithNames' 'TSVWithNamesAndTypes' 'TSVRawWithNames' 'TSVRawWithNamesAndTypes' 'CSVWithNames' 'CSVWithNamesAndTypes' 'JSONCompactEachRowWithNames' 'JSONCompactEachRowWithNamesAndTypes')

for format in "${FORMATS[@]}"
do
    $CLICKHOUSE_CLIENT -q "SELECT number, range(number + 10) AS array, toString(number) AS string FROM numbers(10) FORMAT $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_DATABASE}.data', '$format', 'number UInt64, array Array(UInt64), string String') ORDER BY number SETTINGS input_format_parallel_parsing=1, min_chunk_bytes_for_parallel_parsing=40"
done

rm $DATA_FILE

