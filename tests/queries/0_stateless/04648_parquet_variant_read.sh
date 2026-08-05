#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading Parquet VARIANT columns (unshredded and shredded), decoded to Dynamic.
# The test files were hand-crafted per the parquet-format VariantEncoding/VariantShredding specs.

echo "-- unshredded variant: schema inference gives Dynamic --"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet')" | cut -f1,2

echo "-- unshredded variant: values --"
$CLICKHOUSE_LOCAL -q "SELECT v, dynamicType(v) FROM file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet')"

echo "-- unshredded variant: JSON text via explicit String type --"
$CLICKHOUSE_LOCAL -q "SELECT v FROM file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet', 'Parquet', 'v String')"

echo "-- unshredded variant: raw group via explicit Tuple type --"
$CLICKHOUSE_LOCAL -q "SELECT v.metadata, length(v.value) FROM file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet', 'Parquet', 'v Tuple(metadata String, value String)')"

echo "-- unshredded variant: legacy tuple reading with enable_json_parsing = 0 --"
$CLICKHOUSE_LOCAL -q "SELECT v.metadata, length(v.value) FROM file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet') SETTINGS input_format_parquet_enable_json_parsing = 0"

echo "-- shredded primitive: values --"
$CLICKHOUSE_LOCAL -q "SELECT measurement, dynamicType(measurement) FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_primitive.parquet')"

echo "-- shredded object: values (spec shredding examples) --"
$CLICKHOUSE_LOCAL -q "SELECT event FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_object.parquet', 'Parquet', 'event String')"

echo "-- shredded object: values as Dynamic --"
$CLICKHOUSE_LOCAL -q "SELECT event, dynamicType(event) FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_object.parquet')"

echo "-- shredded object: JSON subcolumns read shredded fields --"
$CLICKHOUSE_LOCAL -q "SELECT event, event.event_type, event.event_ts FROM file('$CUR_DIR/data_parquet/04648_variant_objects_only.parquet', 'Parquet', 'event JSON')"

echo "-- shredded object: JSON type rejects non-object top-level values --"
$CLICKHOUSE_LOCAL -q "SELECT event FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_object.parquet', 'Parquet', 'event JSON')" 2>&1 | grep -c "Cannot read JSON object"

echo "-- shredded array: values --"
$CLICKHOUSE_LOCAL -q "SELECT tags FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_array.parquet', 'Parquet', 'tags String')"

echo "-- shredded array: values as Dynamic --"
$CLICKHOUSE_LOCAL -q "SELECT tags, dynamicType(tags) FROM file('$CUR_DIR/data_parquet/04648_variant_shredded_array.parquet')"

echo "-- duckdb-written variant (typed_value String holds JSON text) --"
$CLICKHOUSE_LOCAL -q "SELECT x, dynamicType(x) FROM file('$CUR_DIR/data_parquet/04648_variant_duckdb.parquet')"

echo "-- fully-shredded variant: subcolumn requests read only the shredded leaf --"
$CLICKHOUSE_LOCAL -q "SELECT \`payload.event_type\`, \`payload.ts\` FROM file('$CUR_DIR/data_parquet/04648_variant_fully_shredded.parquet', 'Parquet', '\`payload.event_type\` String, \`payload.ts\` Nullable(DateTime64(6, ''UTC''))')"

echo "-- fully-shredded variant: full column read assembles from typed_value only --"
$CLICKHOUSE_LOCAL -q "SELECT payload, dynamicType(payload) FROM file('$CUR_DIR/data_parquet/04648_variant_fully_shredded.parquet')"

echo "-- variant write path: Dynamic written as VARIANT group, read back losslessly --"
$CLICKHOUSE_LOCAL -q "
SELECT v FROM file('$CUR_DIR/data_parquet/04648_variant_unshredded.parquet') FORMAT Parquet" > "$CUR_DIR/04648_roundtrip.parquet"
$CLICKHOUSE_LOCAL -q "SELECT v, dynamicType(v) FROM file('$CUR_DIR/04648_roundtrip.parquet')"
rm -f "$CUR_DIR/04648_roundtrip.parquet"

echo "-- engine-level subcolumn pushdown: prunable shredded subcolumns read only the shredded leaf --"
$CLICKHOUSE_LOCAL -q "
SELECT payload.event_type.:String AS et, count() FROM file('$CUR_DIR/data_parquet/04648_variant_fully_shredded.parquet', 'Parquet', 'payload JSON') GROUP BY et ORDER BY et"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$CUR_DIR/data_parquet/04648_variant_fully_shredded.parquet', 'Parquet', 'payload JSON') WHERE payload.event_type.:String = 'login'"

echo "-- engine-level subcolumn fallback: partially-shredded subcolumns assemble the whole variant --"
$CLICKHOUSE_LOCAL -q "
SELECT event.event_type.:String AS et, count() FROM file('$CUR_DIR/data_parquet/04648_variant_objects_only.parquet', 'Parquet', 'event JSON') GROUP BY et ORDER BY et"

echo "-- selective subcolumns: unshredded fields extracted from the value binary --"
$CLICKHOUSE_LOCAL -q "
SELECT event.email.:String, event.click.:String FROM file('$CUR_DIR/data_parquet/04648_variant_objects_only.parquet', 'Parquet', 'event JSON')"

echo "-- selective subcolumns: shredded field, timestamp leaf and value-resident field together --"
$CLICKHOUSE_LOCAL -q "
SELECT event.event_type.:String, event.event_ts.:\`DateTime64(6, 'UTC')\`, event.email.:String FROM file('$CUR_DIR/data_parquet/04648_variant_objects_only.parquet', 'Parquet', 'event JSON')"

echo "-- selective subcolumns: missing path --"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$CUR_DIR/data_parquet/04648_variant_objects_only.parquet', 'Parquet', 'event JSON') WHERE event.nosuch.:String != ''"

echo "-- selective subcolumns: DuckDB JSON-document variant, top-level and nested paths --"
$CLICKHOUSE_LOCAL -q "
SELECT x.name.:String, x.age.:Int64, x.nested.y.:String FROM file('$CUR_DIR/data_parquet/04648_variant_duckdb.parquet', 'Parquet', 'x JSON')"

echo "-- selective subcolumns: nested shredded groups (path descent) --"
$CLICKHOUSE_LOCAL -q "
SELECT v.nested.b.:String, v.nested.a.:Int64, v.other.:String FROM file('$CUR_DIR/data_parquet/04648_variant_nested_shredded.parquet', 'Parquet', 'v JSON')"

echo "-- selective subcolumns: whole nested subtree as Dynamic --"
$CLICKHOUSE_LOCAL -q "
SELECT v.nested, dynamicType(v.nested) FROM file('$CUR_DIR/data_parquet/04648_variant_nested_shredded.parquet', 'Parquet', 'v JSON')"

echo "-- selective subcolumns: whole nested group and a deeper path in one query --"
$CLICKHOUSE_LOCAL -q "
SELECT v.nested, v.nested.b.:String FROM file('$CUR_DIR/data_parquet/04648_variant_nested_shredded.parquet', 'Parquet', 'v JSON')"
