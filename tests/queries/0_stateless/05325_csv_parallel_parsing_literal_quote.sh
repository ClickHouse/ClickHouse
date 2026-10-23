#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `"` inside an unquoted CSV field is a literal character, so the parallel parsing segmenter
# must not treat it as the start of a quoted field and split a later multi-line quoted field.

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

printf '12" pizza\n"x\ny\nz\nw"\n' > "${tmp_dir}/literal.csv"
printf 'a,"x\ny\nz\nw"\nb,"p\nq\nr\ns"\n' > "${tmp_dir}/quoted_after_delimiter.csv"
printf 'a, "x\ny\nz\nw"\n' > "${tmp_dir}/quoted_after_space.csv"

SETTINGS="input_format_csv_detect_header = 0, use_cache_for_count_from_files = 0, min_chunk_bytes_for_parallel_parsing = 1, max_threads = 2, max_parsing_threads = 2"

for parallel in 0 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'literal', ${parallel}, s FROM file('${unique_name}/literal.csv', 'CSV', 's String') ORDER BY s SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'after_delimiter', ${parallel}, a, b FROM file('${unique_name}/quoted_after_delimiter.csv', 'CSV', 'a String, b String') ORDER BY a SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'after_space', ${parallel}, a, b FROM file('${unique_name}/quoted_after_space.csv', 'CSV', 'a String, b String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
done

# `count()` skips rows without parsing them (CSVFormatReader::skipRow), which has its own quote tracking.
for parallel in 0 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_literal', ${parallel}, count() FROM file('${unique_name}/literal.csv', 'CSV', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_after_delimiter', ${parallel}, count() FROM file('${unique_name}/quoted_after_delimiter.csv', 'CSV', 'a String, b String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_after_space', ${parallel}, count() FROM file('${unique_name}/quoted_after_space.csv', 'CSV', 'a String, b String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
done


# Spaces before a quoted field are skipped by the parser for non-string columns even with trimming disabled,
# and a field may start with a single quote when `format_csv_allow_single_quotes` is enabled.
printf ' "[1,\n2,\n3,\n4]"\n' > "${tmp_dir}/array_after_space.csv"
printf "'\"x\ny\nz\nw\"'\n" > "${tmp_dir}/single_quoted.csv"
ARR_SETTINGS="input_format_csv_trim_whitespaces = 0, use_cache_for_count_from_files = 0, ${SETTINGS}"
SQ_SETTINGS="format_csv_allow_single_quotes = 1, use_cache_for_count_from_files = 0, ${SETTINGS}"

for parallel in 0 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'array_after_space', ${parallel}, s FROM file('${unique_name}/array_after_space.csv', 'CSV', 's Array(UInt8)') SETTINGS input_format_parallel_parsing = ${parallel}, ${ARR_SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'single_quoted', ${parallel}, s FROM file('${unique_name}/single_quoted.csv', 'CSV', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SQ_SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_array_after_space', ${parallel}, count() FROM file('${unique_name}/array_after_space.csv', 'CSV', 's Array(UInt8)') SETTINGS input_format_parallel_parsing = ${parallel}, ${ARR_SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_single_quoted', ${parallel}, count() FROM file('${unique_name}/single_quoted.csv', 'CSV', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SQ_SETTINGS}"
done

# A BOM before the first field, a double quote inside a single-quoted field preceded by text,
# and tuple elements separated by vertical tab / form feed whitespace.
printf '\xEF\xBB\xBF"[1,\n2,\n3,\n4]"\n' > "${tmp_dir}/bom_array.csv"
printf "'a\"x\ny\nz\nw\"'\n" > "${tmp_dir}/single_quoted_text.csv"
printf '1,\v"x\ny\nz\nw"\n' > "${tmp_dir}/tuple_vt.csv"
printf '1,\f"x\ny\nz\nw"\n' > "${tmp_dir}/tuple_ff.csv"

for parallel in 0 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'bom_array', ${parallel}, s FROM file('${unique_name}/bom_array.csv', 'CSV', 's Array(UInt8)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_bom_array', ${parallel}, count() FROM file('${unique_name}/bom_array.csv', 'CSV', 's Array(UInt8)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'single_quoted_text', ${parallel}, s FROM file('${unique_name}/single_quoted_text.csv', 'CSV', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SQ_SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_single_quoted_text', ${parallel}, count() FROM file('${unique_name}/single_quoted_text.csv', 'CSV', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SQ_SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'tuple_vt', ${parallel}, s FROM file('${unique_name}/tuple_vt.csv', 'CSV', 's Tuple(Int32, String)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_tuple_vt', ${parallel}, count() FROM file('${unique_name}/tuple_vt.csv', 'CSV', 's Tuple(Int32, String)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'tuple_ff', ${parallel}, s FROM file('${unique_name}/tuple_ff.csv', 'CSV', 's Tuple(Int32, String)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_tuple_ff', ${parallel}, count() FROM file('${unique_name}/tuple_ff.csv', 'CSV', 's Tuple(Int32, String)') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
done

rm -rf "${tmp_dir:?}"
