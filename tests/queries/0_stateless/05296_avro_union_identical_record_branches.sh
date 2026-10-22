#!/usr/bin/env bash
# Tags: no-fasttest
# Avro unions whose named branches (records, enums) have identical structure map to the same
# ClickHouse type, so the Variant has fewer types than the union has branches.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

file_name="$CLICKHOUSE_DATABASE"_union_identical_record_branches.avro
cp "$DATA_DIR"/union_identical_record_branches.avro "$CLICKHOUSE_USER_FILES/$file_name"

echo "== DESCRIBE =="
$CLICKHOUSE_CLIENT -q "DESC file('$file_name')"

echo "== SELECT =="
$CLICKHOUSE_CLIENT -q "SELECT id, payload, variantType(payload), status, variantType(status) FROM file('$file_name') ORDER BY id"

echo "== SELECT with explicit structure =="
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$file_name', 'Avro', '
  id Int32,
  payload Variant(Tuple(x Int32, s String), Tuple(y Float64)),
  status Variant(Array(Int32), Enum8(\'a\' = 0, \'b\' = 1))
') ORDER BY id"

echo "== SELECT with more types than the union has =="
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$file_name', 'Avro', '
  id Int32,
  payload Variant(Tuple(x Int32, s String), Tuple(y Float64), String),
  status Variant(Array(Int32), Enum8(\'a\' = 0, \'b\' = 1))
')" 2>&1 | grep -m1 -c 'The number of distinct (non-null) union types in Avro record (2) does not match the number of types in destination Variant type (3)'

rm -f "$CLICKHOUSE_USER_FILES/$file_name"
