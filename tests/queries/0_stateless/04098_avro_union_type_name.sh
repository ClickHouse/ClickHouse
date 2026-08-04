#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro
# enable_nullable_tuple_type is required so an Avro [null, record] union is inferred as
# Nullable(Tuple(...)); it defaults to off, in which case such a union becomes a plain Tuple.
CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --enable_nullable_tuple_type=1"

file_name="$CLICKHOUSE_DATABASE"_union_type_name.avro
cp "$DATA_DIR/union_type_name.avro" "$CLICKHOUSE_USER_FILES/$file_name"

nested_file_name="$CLICKHOUSE_DATABASE"_union_nested.avro
cp "$DATA_DIR/union_nested.avro" "$CLICKHOUSE_USER_FILES/$nested_file_name"

single_file_name="$CLICKHOUSE_DATABASE"_union_nested_single.avro
cp "$DATA_DIR/union_nested_single.avro" "$CLICKHOUSE_USER_FILES/$single_file_name"

echo "== DESCRIBE with union_type_name enabled =="
$CH_CLIENT -q "DESCRIBE file('$file_name') SETTINGS input_format_avro_union_type_name=1"
echo

echo "== SELECT id, nullable_payload.\$name, variant_payload.\$name =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`nullable_payload.\$name\`, \`variant_payload.\$name\`
  FROM file('$file_name')
  ORDER BY id
"
echo

echo "== Filter WHERE nullable_payload.\$name = 'TypeA' =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`nullable_payload.\$name\`
  FROM file('$file_name')
  WHERE \`nullable_payload.\$name\` = 'TypeA'
  ORDER BY id
"
echo

echo "== Filter WHERE nullable_payload.\$name IS NULL =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`nullable_payload.\$name\`
  FROM file('$file_name')
  WHERE \`nullable_payload.\$name\` IS NULL
  ORDER BY id
"
echo

echo "== Both value and \$name for Nullable union =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, nullable_payload, \`nullable_payload.\$name\`
  FROM file('$file_name')
  ORDER BY id
"
echo

echo "== Both value and \$name for Variant union =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, variant_payload, \`variant_payload.\$name\`
  FROM file('$file_name')
  ORDER BY id
"
echo

echo "== Explicit schema: only \$name columns =="
$CH_CLIENT -q "
  SELECT id, \`nullable_payload.\$name\`, \`variant_payload.\$name\`
  FROM file('$file_name', 'Avro', '
    id Int32,
    \`nullable_payload.\$name\` Nullable(String),
    \`variant_payload.\$name\` Nullable(String)
  ')
  ORDER BY id
"
echo

echo "== DESCRIBE: branch-value columns exposed (Variant only, not Nullable) =="
$CH_CLIENT -q "DESCRIBE file('$file_name') SETTINGS input_format_avro_union_type_name=1" \
  | grep -E 'payload\.' || true
echo

echo "== Project a Variant branch value by the name \$name reports =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`variant_payload.\$name\`, \`variant_payload.TypeB\`, \`variant_payload.TypeC\`
  FROM file('$file_name')
  ORDER BY id
"
echo

echo "== Filter by \$name + project that branch value =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`variant_payload.TypeB\`
  FROM file('$file_name')
  WHERE \`variant_payload.\$name\` = 'TypeB'
  ORDER BY id
"
echo

echo "== Value + branch column together: active branch filled, others NULL =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, variant_payload, \`variant_payload.TypeB\`, \`variant_payload.TypeC\`
  FROM file('$file_name')
  ORDER BY id
"
echo

echo "== Nullable union has NO branch-value column (only \$name) =="
$CH_CLIENT -q "DESCRIBE file('$file_name') SETTINGS input_format_avro_union_type_name=1" \
  | grep -E 'nullable_payload' || true
echo

echo "== Non-nullable branch sub-column is rejected (value + branch selected) =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, variant_payload, \`variant_payload.TypeB\`
  FROM file('$file_name', 'Avro', 'id Int32, variant_payload Variant(Tuple(y String), Tuple(z Float64)), \`variant_payload.TypeB\` Tuple(y String)')
" 2>&1 | grep -q 'must be Nullable' && echo "rejected: must be Nullable" || echo "NOT rejected"
echo

echo "== Nested union: DESCRIBE exposes the inner \$name one level deep =="
$CH_CLIENT -q "DESCRIBE file('$nested_file_name') SETTINGS input_format_avro_union_type_name=1" \
  | grep -E '^payload' || true
echo

echo "== Nested union: inner \$name alone =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`payload.TypeA.inner.\$name\`
  FROM file('$nested_file_name')
  ORDER BY id
"
echo

echo "== Nested union: inner \$name together with the union value =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, payload, \`payload.TypeA.inner.\$name\`
  FROM file('$nested_file_name')
  ORDER BY id
"
echo

echo "== Nested union: filter by the inner branch name =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id
  FROM file('$nested_file_name')
  WHERE \`payload.TypeA.inner.\$name\` = 'InnerX'
  ORDER BY id
"
echo

echo "== Nested union: outer branch value still carries the whole inner union =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`payload.TypeA\`, \`payload.TypeB\`
  FROM file('$nested_file_name')
  ORDER BY id
"
echo

echo "== Nested union with a single non-null inner branch has no inner \$name =="
$CH_CLIENT -q "DESCRIBE file('$single_file_name') SETTINGS input_format_avro_union_type_name=1" \
  | grep -cE '^payload\.TypeA\.inner' || true
echo

echo "== Nested union: inner \$name alongside the outer \$name is not supported =="
$CH_CLIENT --input_format_avro_union_type_name=1 -q "
  SELECT id, \`payload.\$name\`, \`payload.TypeA.inner.\$name\`
  FROM file('$nested_file_name')
  ORDER BY id
" 2>&1 | grep -q 'THERE_IS_NO_COLUMN' && echo "rejected: THERE_IS_NO_COLUMN" || echo "NOT rejected"
echo
