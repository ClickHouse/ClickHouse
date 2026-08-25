#!/usr/bin/env bash
#Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select number as x from numbers(3) format Parquet" > 02511_data1.parquet
$CLICKHOUSE_LOCAL -q "select y from file(02511_data1.parquet, auto, 'x UInt64, y String default \'Hello\'') settings input_format_parquet_allow_missing_columns=1"
$CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format Parquet" > 02511_data2.parquet
$CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('02511_data*.parquet', auto, 'x UInt64, y String') settings input_format_parquet_allow_missing_columns=1"

$CLICKHOUSE_LOCAL -q "select number as x from numbers(3) format ORC" > 02511_data1.orc
$CLICKHOUSE_LOCAL -q "select y from file(02511_data1.orc, auto, 'x UInt64, y String default \'Hello\'') settings input_format_orc_allow_missing_columns=1"
$CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format ORC" > 02511_data2.orc
$CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('02511_data*.orc', auto, 'x UInt64, y String') settings input_format_orc_allow_missing_columns=1"

rm 02511_data*

