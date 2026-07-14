#!/usr/bin/env bash
# Tags: no-fasttest

# Iceberg / Spark encode a map with a non-string key as an Avro array of
# two-field {key, value} records (Avro native maps only allow string keys).
# ClickHouse must accept such a schema when the destination type is a Map.
# See https://github.com/ClickHouse/ClickHouse/issues/109282

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

file_name="$CLICKHOUSE_DATABASE"_map_as_array_of_records.avro
cp "$DATA_DIR"/map_as_array_of_records.avro "$CLICKHOUSE_USER_FILES/$file_name"

echo "== inferred schema (array of records, no Map inference) =="
$CLICKHOUSE_CLIENT -q "desc file('$file_name')"
echo

echo "== read as Array(Tuple(...)) (already worked) =="
$CLICKHOUSE_CLIENT -q "select m_int from file('$file_name', 'Avro', 'm_int Array(Tuple(key Int32, value Int32))')"
echo

echo "== read as Map(Int32, Int32) (the fix) =="
$CLICKHOUSE_CLIENT -q "select m_int from file('$file_name', 'Avro', 'm_int Map(Int32, Int32)')"
echo

echo "== map lookup by key works =="
$CLICKHOUSE_CLIENT -q "select m_int[3] from file('$file_name', 'Avro', 'm_int Map(Int32, Int32)')"
echo

echo "== value type coercion Map(Int32, Int64) =="
$CLICKHOUSE_CLIENT -q "select m_int from file('$file_name', 'Avro', 'm_int Map(Int32, Int64)')"
echo

echo "== fields routed by name: value declared before key, no logicalType, Map(String, Int64) =="
$CLICKHOUSE_CLIENT -q "select m_swapped from file('$file_name', 'Avro', 'm_swapped Map(String, Int64)')"
echo

echo "== nullable value Map(Int32, Nullable(Int64)) =="
$CLICKHOUSE_CLIENT -q "select m_nullable_val from file('$file_name', 'Avro', 'm_nullable_val Map(Int32, Nullable(Int64))')"
echo

echo "== only one canonical name (value, field 0): stays positional, key from field 0 =="
$CLICKHOUSE_CLIENT -q "select m_one_name_value from file('$file_name', 'Avro', 'm_one_name_value Map(Int32, Int32)')"
echo

echo "== only one canonical name (key, field 1): stays positional, key from field 0 =="
$CLICKHOUSE_CLIENT -q "select m_one_name_key from file('$file_name', 'Avro', 'm_one_name_key Map(Int32, Int32)')"
echo

echo "== all three maps together =="
$CLICKHOUSE_CLIENT -q "select m_int, m_swapped, m_nullable_val from file('$file_name', 'Avro', 'm_int Map(Int32, Int32), m_swapped Map(String, Int64), m_nullable_val Map(Int32, Nullable(Int64))')"
echo

rm -f "$CLICKHOUSE_USER_FILES/$file_name"
