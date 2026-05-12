#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro
CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1"

file_name="$CLICKHOUSE_DATABASE"_union_type_name.avro
cp "$DATA_DIR/union_type_name.avro" "$CLICKHOUSE_USER_FILES/$file_name"

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
