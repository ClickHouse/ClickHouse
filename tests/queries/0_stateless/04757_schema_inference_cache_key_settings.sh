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

for suffix in a b; do
    printf '1e5\n'  > "${T}_exp_${suffix}.tsv"
    printf '1e5\n'  > "${T}_exp_${suffix}.csv"
    printf 'x=1e5'  > "${T}_exp_${suffix}.form"
    printf '{"x":1e5}\n' > "${T}_exp_${suffix}.json"
    printf '{"x":[[[[[[[[1]]]]]]]]}\n' > "${T}_deep_${suffix}.json"
    printf '[[[[[[1]]]]]]\n' > "${T}_deep_${suffix}.tsv"
done
printf 'x=${c1:CSV}\n' > "${T}_row.tpl"
for suffix in a b; do printf 'x=1e5\n' > "${T}_tpl_${suffix}.txt"; done

$CLICKHOUSE_LOCAL -q "SELECT [[[[[[toUInt32(1)]]]]]] AS x INTO OUTFILE '${T}_deep_a.parquet' TRUNCATE FORMAT Parquet"
cp "${T}_deep_a.parquet" "${T}_deep_b.parquet"
touch -d "$AGE" "${T}"_*

# --- input_format_try_infer_exponent_floats -----------------------------------------------
# Each pair must report Float64 for the exp=1 query and String for the exp=0 query,
# whichever ran first.
for fmt_file in "TSV tsv" "CSV csv" "Form form"; do
    set -- $fmt_file
    echo "-- $1 exponent, exp=1 first"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_exp_a.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 1;
        DESC file('${T}_exp_a.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 0;" | awk '{print $2}'
    echo "-- $1 exponent, exp=0 first"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_exp_b.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 0;
        DESC file('${T}_exp_b.$2', '$1') SETTINGS input_format_try_infer_exponent_floats = 1;" | awk '{print $2}'
done

# The value channel, not just the type: a poisoned Float64 prints 100000 instead of 1e5.
echo "-- TSV exponent value, exp=1 first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT c1 FROM file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 1;
    SELECT c1 FROM file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 0;"

# Values uses the Quoted escaping rule. Only the permissive direction is observable: at
# exp=0 inference throws, and a throwing inference caches nothing.
echo "-- Values (Quoted) exponent, exp=1 first, second query must fail"
printf '(1e5)\n' > "${T}_exp_q.values"
touch -d "$AGE" "${T}_exp_q.values"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_q.values', 'Values') SETTINGS input_format_try_infer_exponent_floats = 1;
    DESC file('${T}_exp_q.values', 'Values') SETTINGS input_format_try_infer_exponent_floats = 0;" \
    2>&1 | grep -oE 'Nullable\(Float64\)|ONLY_NULLS_WHILE_READING_SCHEMA'

# JSON must NOT be keyed on this setting: tryReadFloat short-circuits it for JSON, so the
# verdict is identical at both values. This row keeps a later change from adding it there.
# The type alone cannot detect that: it is identical whether or not the field is in the key.
# Asserting that JSON produces ONE cache entry for both values is what pins the omission.
echo "-- JSON exponent control, type must be identical at both values"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_a.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 1;
    DESC file('${T}_exp_a.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 0;" | awk '{print $2}'
echo "-- JSON exponent control, both values must share one cache entry"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_b.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 1 FORMAT Null;
    DESC file('${T}_exp_b.json', 'JSONEachRow') SETTINGS input_format_try_infer_exponent_floats = 0 FORMAT Null;
    SELECT count(), countDistinct(additional_format_info) FROM system.schema_inference_cache;"

# --- max_parser_depth --------------------------------------------------------------------
# A low limit must keep throwing after a high-limit query warmed the cache.
for fmt_file in "JSONEachRow json" "TSV tsv"; do
    set -- $fmt_file
    echo "-- $1 depth, high limit then low limit must throw"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_deep_a.$2', '$1') SETTINGS max_parser_depth = 1000 FORMAT Null;
        DESC file('${T}_deep_a.$2', '$1') SETTINGS max_parser_depth = 3 FORMAT Null;" \
        2>&1 | grep -c TOO_DEEP_RECURSION
    echo "-- $1 depth, low limit alone throws (control)"
    $CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.$2', '$1') SETTINGS max_parser_depth = 3 FORMAT Null" \
        2>&1 | grep -c TOO_DEEP_RECURSION
done

echo "-- Parquet depth, high limit then low limit must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 1000 FORMAT Null;
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null;" \
    2>&1 | grep -c TOO_DEEP_RECURSION
echo "-- Parquet depth, low limit alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null" \
    2>&1 | grep -c TOO_DEEP_RECURSION

# --- input_format_parquet_local_time_as_utc -----------------------------------------------
# Selects the timezone of the inferred DateTime64 for a non-UTC-adjusted timestamp column,
# so each pair must report DateTime64(3, 'UTC') for the =1 query and DateTime64(3) for the =0
# query, whichever ran first. A stale entry also changes the value read back, not just the name.
for suffix in a b; do cp "$CUR_DIR"/data_parquet/not_utc.parquet "${T}_lt_${suffix}.parquet"; done
touch -d "$AGE" "${T}"_lt_*.parquet
echo "-- Parquet local_time_as_utc, utc=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_lt_a.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 1;
    DESC file('${T}_lt_a.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 0;" | cut -f2
echo "-- Parquet local_time_as_utc, utc=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_lt_b.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 0;
    DESC file('${T}_lt_b.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 1;" | cut -f2

# --- input_format_parquet_allow_geoparquet_parser -----------------------------------------
# Decides whether a GeoParquet geometry column is inferred as a geo type or as its raw String
# representation, so each pair must report LineString for the =1 query and Nullable(String)
# for the =0 query, whichever ran first.
for suffix in a b; do cp "$CUR_DIR"/data_parquet/03445_geoparquet_null_linestring.parquet "${T}_geo_${suffix}.parquet"; done
touch -d "$AGE" "${T}"_geo_*.parquet
echo "-- Parquet allow_geoparquet_parser, geo=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_geo_a.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 1;
    DESC file('${T}_geo_a.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 0;" | awk -F'\t' '$1 == "geometry" {print $2}'
echo "-- Parquet allow_geoparquet_parser, geo=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_geo_b.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 0;
    DESC file('${T}_geo_b.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 1;" | awk -F'\t' '$1 == "geometry" {print $2}'

# --- input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference ---------
# Decides whether a column of an unsupported type is dropped or the file is rejected, so the
# permissive query must not let a later strict query skip the exception. Only this direction is a
# carrier: the strict query throws, and a throwing inference caches nothing.
# The data file has one VARIANT-typed column `u`, which is a valid Parquet logical type that is not
# implemented here, and one supported Int32 column `id`.
cp "$CUR_DIR"/data_parquet/parquet_variant_logical_type.parquet "${T}_unsup_a.parquet"
touch -d "$AGE" "${T}"_unsup_*.parquet
echo "-- Parquet skip_columns_with_unsupported_types, skip=1 first then strict must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    DESC file('${T}_unsup_a.parquet', 'Parquet') FORMAT Null;" \
    2>&1 | grep -c INCORRECT_DATA
echo "-- Parquet skip_columns_with_unsupported_types, strict alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_unsup_a.parquet', 'Parquet') FORMAT Null" \
    2>&1 | grep -c INCORRECT_DATA
# Without this the arm above would also pass if nothing had been cached at all: the strict query
# throws either way. This shows the permissive query really did leave an entry, keyed on its value.
echo "-- Parquet skip_columns_with_unsupported_types, the permissive entry exists and is keyed"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    SELECT count(), extract(additional_format_info, 'skip_columns_with_unsupported_types=\w+')
    FROM system.schema_inference_cache WHERE format = 'Parquet' GROUP BY 2 ORDER BY 2;"
# An entry being present still does not prove a later query read it: with cache reads bypassed every
# query re-infers and rewrites the same entry. Repeating one query at unchanged settings must hit.
echo "-- Parquet skip_columns_with_unsupported_types, a repeated query hits the cache"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    SELECT value > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

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

# --- the key strings themselves ------------------------------------------------------------
# These pin the cache key directly and fail loudly if a field is dropped again.
echo "-- TSV key carries both new fields, once per setting value"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 1 FORMAT Null;
    DESC file('${T}_exp_a.tsv', 'TSV') SETTINGS input_format_try_infer_exponent_floats = 0 FORMAT Null;
    SELECT extract(additional_format_info, 'input_format_try_infer_exponent_floats=\w+'), extract(additional_format_info, 'max_parser_depth=\d+')
    FROM system.schema_inference_cache ORDER BY ALL;"

echo "-- Values (Quoted) key carries the exponent field"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_q.values', 'Values') SETTINGS input_format_try_infer_exponent_floats = 1 FORMAT Null;
    SELECT extract(additional_format_info, 'input_format_try_infer_exponent_floats=\w+') FROM system.schema_inference_cache;"

echo "-- Form key is no longer empty"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_exp_a.form', 'Form') FORMAT Null;
    SELECT additional_format_info != '' FROM system.schema_inference_cache;"

echo "-- Parquet key carries max_parser_depth and all four new Parquet fields"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 1000 FORMAT Null;
    SELECT extract(additional_format_info, 'max_parser_depth=\d+'), extract(additional_format_info, 'local_time_as_utc=\w+'), extract(additional_format_info, 'allow_geoparquet_parser=\w+'), extract(additional_format_info, 'skip_columns_with_unsupported_types=\w+'), extract(additional_format_info, 'schema_inference_make_json_columns_nullable=\w+')
    FROM system.schema_inference_cache;"

echo "-- JSON key carries the array_of_dynamic field, once per setting value"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1 FORMAT Null;
    DESC file('${T}_dyn_a.json', 'JSONEachRow') SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0 FORMAT Null;
    SELECT extract(additional_format_info, 'infer_array_of_dynamic_from_array_of_different_values=\w+')
    FROM system.schema_inference_cache ORDER BY ALL;"

echo "-- Template key follows the row format's rule, not format_regexp_escaping_rule"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_tpl_a.txt', 'Template') SETTINGS format_template_row = '${T}_row.tpl', format_regexp_escaping_rule = 'JSON' FORMAT Null;
    SELECT additional_format_info LIKE '%tuple_delimiter%', additional_format_info LIKE '%read_bools_as_numbers%'
    FROM system.schema_inference_cache;"

rm -f "${T}"_*
