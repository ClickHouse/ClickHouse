#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for `CSVFormatReader::skipRow` (the `optimize_count_from_files` fast path)
# dereferencing a stale pointer after a read buffer refill.
#
# The read buffer is 16 bytes. The first row is laid out so that its `\r` is the very last byte
# of the first chunk: checking for the following `\n` refills the buffer, and the byte that
# now sits at the same offset of the reused buffer memory (the last byte of the second chunk)
# is the `\n` that terminates the second row. Reading the stale pointer instead of the real
# cursor made `skipRow` return without consuming the real `\n`, which was then counted as an
# extra empty row.
#
# Layout (32 bytes, two 16-byte chunks):
#   chunk 1: 'a' x 15, '\r'
#   chunk 2: '\n', 'b' x 14, '\n'

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

printf 'aaaaaaaaaaaaaaa\r\nbbbbbbbbbbbbbb\n' > "${tmp_dir}/data.csv"
# The same layout with a two-byte `CSVWithNames` header (`x\n`) before the rows.
printf 'x\naaaaaaaaaaaaa\r\nbbbbbbbbbbbbbb\n' > "${tmp_dir}/data_with_names.csv"

# Header detection peeks at the first row through a checkpoint, which changes the buffer layout
# and hides the problem for the plain `CSV` case, so it is turned off.
SETTINGS="max_read_buffer_size = 16, use_cache_for_count_from_files = 0, input_format_csv_detect_header = 0"

for optimize in 1 0
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'CSV', ${optimize}, count() FROM file('${unique_name}/data.csv', 'CSV', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'CSVWithNames', ${optimize}, count() FROM file('${unique_name}/data_with_names.csv', 'CSVWithNames', 'x String') SETTINGS optimize_count_from_files = ${optimize}, ${SETTINGS}"
done

rm -rf "${tmp_dir:?}"
