#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1"

cp $DATA_DIR/union_in_complex_types.avro $CLICKHOUSE_USER_FILES/union_in_complex_types.avro

echo "== DESCRIBE =="
$CH_CLIENT -q "desc file('union_in_complex_types.avro')"
echo

echo "== SELECT variantType =="
$CH_CLIENT -q "
  SELECT
      toTypeName(string_only),
      string_or_null IS NULL,
      null_or_string IS NULL,
      * EXCEPT (string_only, string_or_null, null_or_string, double_or_long_or_string_in_array, double_or_string_or_long_or_null_in_map) APPLY (x -> variantType(x)),
      arrayMap(x -> variantType(x), double_or_long_or_string_in_array),
      arrayMap(x -> variantType(x), mapValues(double_or_string_or_long_or_null_in_map))
  FROM file('union_in_complex_types.avro')"
echo

echo "== SELECT * =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro')"
echo

echo "== SELECT * WITH CustomSchema =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro', 'Avro', '
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(Float64, String),
  string_or_double Variant(Float64, String),
  null_or_string_or_double Variant(Float64, String),
  string_or_double_or_null Variant(Float64, String),
  string_or_float_or_long Variant(Float32, Int64, String),
  long_or_string_or_float Variant(Float32, Int64, String),
  double_or_null_or_string_or_long Variant(Float64, String, Int64),
  double_or_long_or_string_in_array Array(Variant(Float64, String, Int64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(Float64, Int64, String))
');"
echo

echo "== SELECT * WITH CustomSchema SwappedFirstLastVariant =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro', 'Avro', '
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(String, Float64),
  string_or_double Variant(String, Float64),
  null_or_string_or_double Variant(String, Float64),
  string_or_double_or_null Variant(String, Float64),
  string_or_float_or_long Variant(String, Int64, Float32),
  long_or_string_or_float Variant(String, Int64, Float32),
  double_or_null_or_string_or_long Variant(Int64, String, Float64),
  double_or_long_or_string_in_array Array(Variant(Int64, String, Float64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(String, Int64, Float64))
');"
echo

echo "== SELECT * WITH CustomSchema Float32 instead of Float64 =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro', 'Avro', '
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(Float32, String),
  string_or_double Variant(Float32, String),
  null_or_string_or_double Variant(Float32, String),
  string_or_double_or_null Variant(Float32, String),
  string_or_float_or_long Variant(Float32, Int64, String),
  long_or_string_or_float Variant(Float32, Int64, String),
  double_or_null_or_string_or_long Variant(Float32, String, Int64),
  double_or_long_or_string_in_array Array(Variant(Float32, String, Int64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(Float32, Int64, String))
');" 2>&1 | grep -c 'DB::Exception: Destination Variant(Float32, String) and Avro Union containing Float64 are not compatible.'
echo

echo "== SELECT * WITH CustomSchema more types than expected =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro', 'Avro', '
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(Float64, String, Int64),
  string_or_double Variant(Float64, String, Int64),
  null_or_string_or_double Variant(Float64, String, Int64),
  string_or_double_or_null Variant(Float64, String, Int64),
  string_or_float_or_long Variant(Float32, Int64, String, Int64),
  long_or_string_or_float Variant(Float32, Int64, String, Int64),
  double_or_null_or_string_or_long Variant(Float64, String, Int64, Int64),
  double_or_long_or_string_in_array Array(Variant(Float64, String, Int64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(Float64, Int64, String))
');" 2>&1 | grep -c 'DB::Exception: The number of (non-null) union types in Avro record (2) does not match the number of types in destination Variant type (3).'
echo

echo "== SELECT * WITH CustomSchema less types than expected =="
$CH_CLIENT -q "select * from file('union_in_complex_types.avro', 'Avro', '
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(Float64, String),
  string_or_double Variant(Float64, String),
  null_or_string_or_double Variant(Float64, String),
  string_or_double_or_null Variant(Float64, String),
  string_or_float_or_long Variant(Float32, Int64, String),
  long_or_string_or_float Variant(Float32, Int64, String),
  double_or_null_or_string_or_long Variant(Float64, String, Int64),
  double_or_long_or_string_in_array Array(Variant(Float64, String, Int64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(Float64, Int64))
');" 2>&1 | grep -c 'DB::Exception: Destination Variant(Float64, Int64) and Avro Union containing String are not compatible.'
echo

echo "== CREATE TABLE avro_union_test_03237 =="
$CH_CLIENT -q "CREATE TABLE avro_union_test_03237 (
  string_only String,
  string_or_null Nullable(String),
  null_or_string Nullable(String),
  double_or_string Variant(Float64, String),
  string_or_double Variant(Float64, String),
  null_or_string_or_double Variant(Float64, String),
  string_or_double_or_null Variant(Float64, String),
  string_or_float_or_long Variant(Float32, Int64, String),
  long_or_string_or_float Variant(Float32, Int64, String),
  double_or_null_or_string_or_long Variant(Float64, String, Int64),
  double_or_long_or_string_in_array Array(Variant(Float64, String, Int64)),
  double_or_string_or_long_or_null_in_map Map(String, Variant(Float64, Int64, String))
) ENGINE = MergeTree ORDER BY tuple()"
echo

echo "== SELECT * FORMAT Avro | INSERT INTO avro_union_test_03237 FORMAT Avro =="
$CH_CLIENT -q "SELECT * FROM file('union_in_complex_types.avro') FORMAT Avro" | tee /tmp/out.avro | $CH_CLIENT -q "INSERT INTO avro_union_test_03237 FORMAT Avro"
echo


echo "== SELECT * FROM avro_union_test_03237 =="
$CH_CLIENT -q "SELECT * FROM avro_union_test_03237"
echo

echo "== TRUNCATE TABLE avro_union_test_03237 =="
$CH_CLIENT -q "TRUNCATE TABLE avro_union_test_03237"
echo

echo "== insert into table avro_union_test_03237 select * from file('union_in_complex_types.avro') =="
$CH_CLIENT -q "insert into table avro_union_test_03237 select * from file('union_in_complex_types.avro')"
echo

echo "== SELECT * FROM avro_union_test_03237 =="
$CH_CLIENT -q "SELECT * FROM avro_union_test_03237"
echo

rm -f $CLICKHOUSE_USER_FILES/union_in_complex_types_2.avro

echo "== insert into table function file('union_in_complex_types_2.avro') select * from file('union_in_complex_types.avro') =="
$CH_CLIENT -q "insert into table function file('union_in_complex_types_2.avro') select * from file('union_in_complex_types.avro') format Avro"
echo

echo "== SELECT * FROM file('union_in_complex_types_2.avro') =="
$CH_CLIENT -q "SELECT * FROM file('union_in_complex_types_2.avro')"