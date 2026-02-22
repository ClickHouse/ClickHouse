#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME=test_02242.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

for format in Arrow ArrowStream Parquet ORC
do
    echo $format
    $CLICKHOUSE_CLIENT -q "select number % 2 ? NULL : number as x, [number % 2 ? NULL : number, number + 1] as arr1, [[NULL, 'String'], [NULL], []] as arr2, [(NULL, NULL), ('String', NULL), (NULL, number)] as arr3 from numbers(5) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"
done

rm $DATA_FILE
