#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME

formats="TSV JSONEachRow Parquet ORC"
settings="input_format_orc_allow_missing_columns = 1, input_format_parquet_allow_missing_columns = 1"
for format in ${formats}; do
    echo $format

    rm $DATA_FILE
    $CLICKHOUSE_LOCAL -q "select ((1, 2), 3)::Tuple(b Tuple(c UInt32, d UInt32), e UInt32) as a format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "select a.b.d, a.b, a.e from file('$DATA_FILE', $format, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') settings $settings"
    $CLICKHOUSE_LOCAL -q "select x.b.d, x.b, x.e from file('$DATA_FILE', $format, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') settings $settings"

    # Currently ORC and Parquet do not support deducing default values for subcolumn from type definition.
    $CLICKHOUSE_LOCAL -q "select x.b.d, x.b, x.e from file('$DATA_FILE', $format, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)') settings $settings"
done

