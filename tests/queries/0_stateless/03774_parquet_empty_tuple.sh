#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Files for this test were produced by commenting out the check in the parquet writer
# (`if (num_elements == 0)` in PrepareForWrite.cpp), then:
#   insert into function file('empty_tuple.parquet') select tuple() as x, 42 + number as y from numbers(2);
#   insert into function file('empty_tuple2.parquet') select tuple() as x from numbers(2) settings engine_file_truncate_on_insert=1;

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/empty_tuple.parquet')"
# File where the only column is an empty tuple.
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/empty_tuple2.parquet')"
