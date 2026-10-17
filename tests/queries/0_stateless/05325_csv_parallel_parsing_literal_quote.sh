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

SETTINGS="input_format_csv_detect_header = 0, min_chunk_bytes_for_parallel_parsing = 1, max_threads = 2, max_parsing_threads = 2"

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

rm -rf "${tmp_dir:?}"
