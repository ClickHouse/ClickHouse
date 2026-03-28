#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Test that no formats crash or return LOGICAL_ERROR on empty tuple.
# Some formats read zero rows; this test currently allows that.

FILE=03277_$CLICKHOUSE_DATABASE

# With schema inference.
for format in Native TSV CSV TSKV JSON JSONCompact JSONEachRow JSONObjectEachRow JSONCompactEachRow JSONColumns JSONCompactColumns JSONColumnsWithMetadata ORC Arrow
do
  echo $format
  $CLICKHOUSE_LOCAL -q "
    insert into function file('$FILE', '$format') select () settings engine_file_truncate_on_insert=1;
    select * from file('$FILE', '$format');"
done

# Picky about column names.
echo Avro
$CLICKHOUSE_LOCAL -q "
  insert into function file('$FILE', 'Avro') select () as x settings engine_file_truncate_on_insert=1;
  select * from file('$FILE', 'Avro');"

# Without schema inference.
for format in RowBinary Values BSONEachRow MsgPack Native TSV CSV TSKV JSON JSONCompact JSONEachRow JSONObjectEachRow JSONCompactEachRow JSONColumns JSONCompactColumns JSONColumnsWithMetadata ORC Arrow
do
  echo $format
  $CLICKHOUSE_LOCAL -q "
    insert into function file('$FILE', '$format', 'x Tuple()') select () as x settings engine_file_truncate_on_insert=1;
    select * from file('$FILE', '$format', 'x Tuple()');"
done

# Formats that don't support empty tuples.
$CLICKHOUSE_LOCAL -q "
  insert into function file('$FILE', 'Parquet') select () settings engine_file_truncate_on_insert=1; -- {serverError BAD_ARGUMENTS}
  insert into function file('$FILE', 'Npy') select () settings engine_file_truncate_on_insert=1; -- {serverError BAD_ARGUMENTS}
  insert into function file('$FILE', 'CapnProto', 'x Tuple()') select () as x settings engine_file_truncate_on_insert=1; -- {serverError CAPN_PROTO_BAD_CAST}
  insert into function file('$FILE', 'RawBLOB') select () settings engine_file_truncate_on_insert=1; -- {serverError NOT_IMPLEMENTED}"

rm $FILE
