#!/usr/bin/env bash
#Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

filename="${CLICKHOUSE_TEST_UNIQUE_NAME}"_02511_data
filename1="${filename}1"
filename2="${filename}2}"
$CLICKHOUSE_LOCAL -q "select number as x from numbers(3) format Parquet" > $filename1.parquet
$CLICKHOUSE_LOCAL -q "select y from file($filename1.parquet, auto, 'x UInt64, y String default \'Hello\'') settings input_format_parquet_allow_missing_columns=1"
$CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format Parquet" > $filename2.parquet
$CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('$filename*.parquet', auto, 'x UInt64, y String') settings input_format_parquet_allow_missing_columns=1"

$CLICKHOUSE_LOCAL -q "select number as x from numbers(3) format ORC" > $filename1.orc
$CLICKHOUSE_LOCAL -q "select y from file($filename1.orc, auto, 'x UInt64, y String default \'Hello\'') settings input_format_orc_allow_missing_columns=1"
$CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format ORC" > $filename2.orc
$CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('$filename*.orc', auto, 'x UInt64, y String') settings input_format_orc_allow_missing_columns=1"

rm "${filename}"*

