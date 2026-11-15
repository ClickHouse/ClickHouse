#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CSVFormatReader::skipRow` (the `optimize_count_from_files` fast path) must treat `\r\n` as a single
# line ending with `input_format_csv_allow_cr_end_of_line = 1`, like the regular parser does.
# Otherwise the `\n` left after `\r` was counted as an extra empty row for every CRLF record.

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

printf 'a\r\nb\r\nc\r\n' > "${tmp_dir}/crlf.csv"
# Mixed line endings: bare `\r`, `\r\n`, `\n`, and a bare `\r` at the end of the file.
printf 'a\rb\r\nc\nd\r' > "${tmp_dir}/mixed.csv"
printf 'x\r\na\r\nb\r\n' > "${tmp_dir}/crlf_with_names.csv"

SETTINGS="input_format_csv_allow_cr_end_of_line = 1, use_cache_for_count_from_files = 0, input_format_csv_detect_header = 0"

for optimize in 1 0
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'crlf', ${optimize}, count() FROM file('${unique_name}/crlf.csv', 'CSV', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'mixed', ${optimize}, count() FROM file('${unique_name}/mixed.csv', 'CSV', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'crlf_with_names', ${optimize}, count() FROM file('${unique_name}/crlf_with_names.csv', 'CSVWithNames', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
done

rm -rf "${tmp_dir:?}"
