#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/integers_1_5_no_3_bf_minmax.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/integers_1to5_no_3_bf_minmax.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

# This test isolates bloom-filter / min-max pruning and asserts exact rows_read, so disable the
# dictionary-based row group filter (which the test harness randomizes) to keep the counts stable.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --input_format_parquet_dictionary_filter_push_down=0"

# Prior to this PR, bloom filter and minmax were evaluated separately.
# This was sub-optimal for conditions like `x = 3 or x > 5` where data is [1, 2, 4, 5].
# Bloom filter is not able to handle greater than operations. Therefore, it can't evaluate x > 5. Even though it can tell
# `3` is not in the set by evaluating `x = 3`, it can't discard the row group because of the `or` condition.
# On the other hand, min max can handle both. It'll evaluate x = 3 to true (because it is within the range) and the latter to false
# Therefore, bloom filter would determine `false or true` and minmax would determine `true or false`. Resulting in true.

# Without bf to prove nothing is returned, but rows had to be read
${CLICKHOUSE_CLIENT} --query="select * from file('${DATA_FILE_USER_PATH}', Parquet) WHERE int8 = 3 or int8 > 5 FORMAT Json SETTINGS input_format_parquet_filter_push_down=true, input_format_parquet_bloom_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed,.statistics.bytes_read)'

# Since both structures are now evaluated together, the row group should be skipped
${CLICKHOUSE_CLIENT} --query="select * from file('${DATA_FILE_USER_PATH}', Parquet) WHERE int8 = 3 or int8 > 5 FORMAT Json SETTINGS input_format_parquet_filter_push_down=true, input_format_parquet_bloom_filter_push_down=true;" | jq 'del(.meta,.statistics.elapsed,.statistics.bytes_read)'
