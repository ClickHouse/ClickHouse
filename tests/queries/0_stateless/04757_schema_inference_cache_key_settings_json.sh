#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Parquet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each carrier below is probed in BOTH orders, because an order-insensitive probe cannot
# detect a missing cache-key field: whichever query runs first decides the cached type.
# Every order gets its OWN file so the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: SchemaCache::tryGetImpl drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

printf 'x=${c1:CSV}\n' > "${T}_row.tpl"
for suffix in a b; do printf 'x=1e5\n' > "${T}_tpl_${suffix}.txt"; done
touch -d "$AGE" "${T}"_*

# --- schema_inference_make_json_columns_nullable -------------------------------------------
# Decides whether a required JSON column stays JSON or becomes Nullable(JSON), so each pair must
# report the type its own query asked for. The column must be required: an optional one is nullable
# at both values.
for suffix in a b; do cp "$CUR_DIR"/data_parquet/parquet_required_json_column.parquet "${T}_json_${suffix}.parquet"; done
touch -d "$AGE" "${T}"_json_*.parquet
JSON_OPTS="schema_inference_make_columns_nullable = 1, input_format_parquet_enable_json_parsing = 1"
echo "-- Parquet make_json_columns_nullable, nullable=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_json_a.parquet', 'Parquet') SETTINGS $JSON_OPTS, schema_inference_make_json_columns_nullable = 1;
    DESC file('${T}_json_a.parquet', 'Parquet') SETTINGS $JSON_OPTS, schema_inference_make_json_columns_nullable = 0;" | awk -F'\t' '$1 == "j" {print $2}'
echo "-- Parquet make_json_columns_nullable, nullable=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_json_b.parquet', 'Parquet') SETTINGS $JSON_OPTS, schema_inference_make_json_columns_nullable = 0;
    DESC file('${T}_json_b.parquet', 'Parquet') SETTINGS $JSON_OPTS, schema_inference_make_json_columns_nullable = 1;" | awk -F'\t' '$1 == "j" {print $2}'

# --- input_format_json_infer_array_of_dynamic_from_array_of_different_types ----------------
# Decides whether a heterogeneous JSON array becomes Array(Dynamic) or stays an unnamed Tuple,
# so each pair must report the type its own query asked for. The array must mix types: a
# single-type array infers Array(Nullable(Int64)) at both values.
for suffix in a b; do printf '{"a":[42,"hello",[1,2,3]]}\n' > "${T}_dyn_${suffix}.json"; done
touch -d "$AGE" "${T}"_dyn_*.json
echo "-- JSONEachRow array_of_dynamic, dynamic=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1;
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;" | awk -F'\t' '{print $2}'
echo "-- JSONEachRow array_of_dynamic, dynamic=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_dyn_b.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;
    DESC file('${T}_dyn_b.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1;" | awk -F'\t' '{print $2}'
# The two orders above re-infer either way, so they alone do not prove the JSON getter's entries are
# read back. Repeating one query at unchanged settings must hit.
echo "-- JSONEachRow array_of_dynamic, a repeated query hits the cache"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1 FORMAT Null;
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1 FORMAT Null;
    SELECT value > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

# --- Template ----------------------------------------------------------------------------
# The row format's own field rule (CSV here) must key the entry, not format_regexp_escaping_rule.
# The field must be rule-specific: an Escaped field infers String at both exponent values.
echo "-- Template exponent, exp=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_tpl_a.txt', 'Template') SETTINGS format_template_row = '${T}_row.tpl', format_regexp_escaping_rule = 'JSON', input_format_try_infer_exponent_floats = 1;
    DESC file('${T}_tpl_a.txt', 'Template') SETTINGS format_template_row = '${T}_row.tpl', format_regexp_escaping_rule = 'JSON', input_format_try_infer_exponent_floats = 0;" | awk '{print $2}'
echo "-- Template exponent, exp=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_tpl_b.txt', 'Template') SETTINGS format_template_row = '${T}_row.tpl', format_regexp_escaping_rule = 'JSON', input_format_try_infer_exponent_floats = 0;
    DESC file('${T}_tpl_b.txt', 'Template') SETTINGS format_template_row = '${T}_row.tpl', format_regexp_escaping_rule = 'JSON', input_format_try_infer_exponent_floats = 1;" | awk '{print $2}'

rm -f "${T}"_*
