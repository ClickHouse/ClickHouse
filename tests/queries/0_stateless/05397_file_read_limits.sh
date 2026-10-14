#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
trap 'rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"' EXIT
rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${FILE_NAME}', CSV, 'n UInt64') SELECT number FROM numbers(100000)"

SOURCE="file('${FILE_NAME}', CSV, 'n UInt64')"
SETTINGS="max_threads = 1, max_parsing_threads = 1, max_block_size = 1000, input_format_parallel_parsing = 0"

${CLICKHOUSE_CLIENT} --query "SELECT sum(n) FROM ${SOURCE} SETTINGS ${SETTINGS}"

for limit in max_rows_to_read max_rows_to_read_leaf; do
    ${CLICKHOUSE_CLIENT} --query "SELECT sum(n) FROM ${SOURCE} SETTINGS ${SETTINGS}, ${limit} = 10" \
        2>&1 | grep -m1 -o 'TOO_MANY_ROWS'
done

for limit in max_bytes_to_read max_bytes_to_read_leaf; do
    ${CLICKHOUSE_CLIENT} --query "SELECT sum(n) FROM ${SOURCE} SETTINGS ${SETTINGS}, ${limit} = 1000" \
        2>&1 | grep -m1 -o 'TOO_MANY_BYTES'
done

# Read limits are checked at block boundaries, so do not assert an exact partial sum.
for limit in max_rows_to_read max_bytes_to_read; do
    ${CLICKHOUSE_CLIENT} --query "SELECT sum(n) < 4999950000 FROM ${SOURCE} SETTINGS ${SETTINGS}, ${limit} = 1000, read_overflow_mode = 'break'"
done

for limit in max_rows_to_read_leaf max_bytes_to_read_leaf; do
    ${CLICKHOUSE_CLIENT} --query "SELECT sum(n) < 4999950000 FROM ${SOURCE} SETTINGS ${SETTINGS}, ${limit} = 1000, read_overflow_mode_leaf = 'break'"
done
