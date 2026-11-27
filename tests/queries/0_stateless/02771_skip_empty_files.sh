#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PREFIX=$CLICKHOUSE_TEST_UNIQUE_NAME
touch $FILE_PREFIX-1.parquet
$CLICKHOUSE_LOCAL -q "select * from numbers(1) format Parquet" > $FILE_PREFIX-2.parquet
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-1.parquet') settings engine_file_skip_empty_files=0" 2>&1 | grep -c "CANNOT_EXTRACT_TABLE_STRUCTURE"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-1.parquet', auto, 'number UInt64') settings engine_file_skip_empty_files=0" 2>&1 | grep -c "Exception"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-1.parquet') settings engine_file_skip_empty_files=1" 2>&1 | grep -c "Exception"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-1.parquet', auto, 'number UInt64') settings engine_file_skip_empty_files=1"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-*.parquet') settings engine_file_skip_empty_files=0" 2>&1 | grep -c "Exception"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-*.parquet', auto, 'number UInt64') settings engine_file_skip_empty_files=0" 2>&1 | grep -c "Exception"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-*.parquet') settings engine_file_skip_empty_files=1"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE_PREFIX-*.parquet', auto, 'number UInt64') settings engine_file_skip_empty_files=1"

rm $FILE_PREFIX-*
