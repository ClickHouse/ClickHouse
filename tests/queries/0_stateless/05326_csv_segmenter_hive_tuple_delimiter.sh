#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: HiveText is not available in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# HiveText keeps the default csv.tuple_delimiter (',') while its field delimiter is \x01, so a comma inside
# a String field is ordinary data. The CSV segmenter and count() must not treat it as a field start
# and let a following literal apostrophe open a quote.

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

printf "foo,'x\n\"a'b\nc\nd\ne\"\n" > "${tmp_dir}/hive_comma_apostrophe.txt"

SETTINGS="format_csv_allow_single_quotes = 1, use_cache_for_count_from_files = 0, min_chunk_bytes_for_parallel_parsing = 1, max_threads = 2, max_parsing_threads = 2"

for parallel in 0 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT 'hive_comma_apostrophe', ${parallel}, s FROM file('${unique_name}/hive_comma_apostrophe.txt', 'HiveText', 's String') ORDER BY s SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'count_hive_comma_apostrophe', ${parallel}, count() FROM file('${unique_name}/hive_comma_apostrophe.txt', 'HiveText', 's String') SETTINGS input_format_parallel_parsing = ${parallel}, ${SETTINGS}"
done

rm -rf "${tmp_dir:?}"
