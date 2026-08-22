#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


DATA_FILE=$USER_FILES_PATH/${CLICKHOUSE_DATABASE}.data
SELECT_QUERY="select * from file('${CLICKHOUSE_DATABASE}.data', 'CustomSeparated', 'x Nullable(Float64), y Nullable(UInt64)') settings input_format_parallel_parsing=0, format_custom_escaping_rule='Quoted'"


$CLICKHOUSE_CLIENT -q "drop table if exists test_02130"
$CLICKHOUSE_CLIENT -q "create table test_02130 (x Nullable(Float64), y Nullable(UInt64)) engine=Memory()"

echo -e "null\t1" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "nan\t2" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "42.42\t3" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "null\t4" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=1 --storage_file_read_method=pread

echo -e "null\t5" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=2 --storage_file_read_method=pread

echo -e "null\t6" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=3 --storage_file_read_method=pread

echo -e "null\t7" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread

echo -e "nan\t8" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=1 --storage_file_read_method=pread

echo -e "nan\t9" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=2 --storage_file_read_method=pread

echo -e "nan\t10" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=3 --storage_file_read_method=pread

echo -e "nan\t11" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread

echo -e "42\tnan" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "select * from test_02130 order by y"
$CLICKHOUSE_CLIENT -q "drop table test_02130"

rm $DATA_FILE
