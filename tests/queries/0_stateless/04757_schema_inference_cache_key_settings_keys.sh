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

printf '1e5\n'  > "${T}_exp_a.tsv"
printf 'x=1e5'  > "${T}_exp_a.form"
printf '(1e5)\n' > "${T}_exp_q.values"
printf '{"a":[42,"hello",[1,2,3]]}\n' > "${T}_dyn_a.json"
printf 'x=${c1:CSV}\n' > "${T}_row.tpl"
printf 'x=1e5\n' > "${T}_tpl_a.txt"
$CLICKHOUSE_LOCAL -q "SELECT [[[[[[toUInt32(1)]]]]]] AS x INTO OUTFILE '${T}_deep_a.parquet' TRUNCATE FORMAT Parquet"
touch -d "$AGE" "${T}"_*

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

echo "-- Parquet key carries max_parser_depth and all five new Parquet fields"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 1000 FORMAT Null;
    SELECT extract(additional_format_info, 'max_parser_depth=\d+'), extract(additional_format_info, 'local_time_as_utc=\w+'), extract(additional_format_info, 'allow_geoparquet_parser=\w+'), extract(additional_format_info, 'skip_columns_with_unsupported_types=\w+'), extract(additional_format_info, 'schema_inference_make_json_columns_nullable=\w+'), extract(additional_format_info, 'schema_inference_allow_nullable_tuple_type=\w+')
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
