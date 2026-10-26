#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CSVFormatReader::skipRow` must consume a trailing `\r` after a `\r\n` line ending
# when bare CR line endings are disabled, matching the regular CSV parser.

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

printf 'a\r\nb\r\n\r' > "${tmp_dir}/data.csv"

SETTINGS="input_format_csv_allow_cr_end_of_line = 0, use_cache_for_count_from_files = 0, input_format_csv_detect_header = 0"

for optimize in 1 0
do
    ${CLICKHOUSE_CLIENT} -q "SELECT ${optimize}, count() FROM file('${unique_name}/data.csv', 'CSV', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
done

rm -rf "${tmp_dir:?}"
