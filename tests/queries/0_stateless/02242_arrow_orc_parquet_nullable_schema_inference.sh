#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME=test_02242.data
DATA_FILE=$CLICKHOUSE_USER_FILES_UNIQUE/$FILE_NAME

for format in Arrow ArrowStream Parquet ORC
do
    echo $format
    $CLICKHOUSE_CLIENT -q "select number % 2 ? NULL : number as x, [number % 2 ? NULL : number, number + 1] as arr1, [[NULL, 'String'], [NULL], []] as arr2, [(NULL, NULL), ('String', NULL), (NULL, number)] as arr3 from numbers(5) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "
        desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', '$format');
        select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', '$format');"
done

rm $DATA_FILE
