#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Test that no formats crash or return LOGICAL_ERROR on empty tuple
# This test is on the same spirit as 03277_empty_tuple_formats.sh except it has additional column in addition to empty tuple.
# For example this logical error: Invalid number of rows in Chunk  Int32(size = 5) Tuple(size = 0) column Tuple() at position 1: expected 5, got 0
# was triggered when there was another column in addition to the empty tuple. This test covers that case.

FILE=03277_$CLICKHOUSE_DATABASE

# With schema inference.
for format in Native TSV CSV TSKV JSON JSONCompact JSONEachRow JSONObjectEachRow JSONCompactEachRow JSONColumns JSONCompactColumns JSONColumnsWithMetadata ORC Arrow
do
  echo $format
  $CLICKHOUSE_LOCAL -q "
    insert into function file('$FILE', '$format') select number, () from numbers(5) settings engine_file_truncate_on_insert=1;
    select * from file('$FILE', '$format');"
done

# Picky about column names.
echo Avro
$CLICKHOUSE_LOCAL -q "
  insert into function file('$FILE', 'Avro') select number as x, () as y from numbers(5) settings engine_file_truncate_on_insert=1;
  select * from file('$FILE', 'Avro');"

# Without schema inference.
for format in RowBinary Values BSONEachRow MsgPack Native TSV CSV TSKV JSON JSONCompact JSONEachRow JSONObjectEachRow JSONCompactEachRow JSONColumns JSONCompactColumns JSONColumnsWithMetadata ORC Arrow
do
  echo $format
  $CLICKHOUSE_LOCAL -q "
    insert into function file('$FILE', '$format', 'x UInt64, y Tuple()') select number as x, () as y from numbers(5) settings engine_file_truncate_on_insert=1;
    select * from file('$FILE', '$format', 'x UInt64, y Tuple()');"
done

# Formats that don't support empty tuples/multiple columns.
$CLICKHOUSE_LOCAL -q "
  insert into function file('$FILE', 'Parquet') select number, () from numbers(5) settings engine_file_truncate_on_insert=1; -- {serverError BAD_ARGUMENTS}
  insert into function file('$FILE', 'Npy') select number, () from numbers(5) settings engine_file_truncate_on_insert=1; -- {serverError TOO_MANY_COLUMNS}
  insert into function file('$FILE', 'CapnProto', 'x UInt64, y Tuple()') select number as x, () as y from numbers(5) settings engine_file_truncate_on_insert=1; -- {serverError CAPN_PROTO_BAD_CAST}
  insert into function file('$FILE', 'RawBLOB') select number, () from numbers(5) settings engine_file_truncate_on_insert=1; -- {serverError NOT_IMPLEMENTED}"

rm $FILE
