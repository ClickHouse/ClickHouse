#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/multi_column_bf.gz.parquet"
DATA_FILE_USER_PATH="${WORKING_DIR}/multi_column_bf.gz.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

# Parquet bloom filter with an empty IN set should not cause a logical error.
# Previously, an empty hash vector from parquetTryHashColumn would set
# use_bloom_filter = true without adding any hashes, leading to
# "Logical error: '!subranges.empty()'" in Prefetcher::splitRange.

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM file('${DATA_FILE_USER_PATH}', Parquet) WHERE string IN (SELECT toString(number) FROM numbers(0)) SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false, input_format_parquet_page_filter_push_down=false, input_format_parquet_enable_row_group_prefetch=false;"

rm -rf "${WORKING_DIR}"
