#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Parquet is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One block of 40000 rows is split into 40000 one-row row groups, each compressed by its own zstd stream at
# level 15, so writing them takes more than 90 s on a release build, with and without the encoder threads.
# The oracle is the outer `timeout`: a `TIMEOUT_EXCEEDED` that does not arrive within 30 s fails the test.

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

for threads in 1 2; do
    if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode throw --query "
            INSERT INTO FUNCTION file('${FILE}', 'Parquet') SELECT number FROM numbers(40000)
            SETTINGS output_format_parquet_row_group_size = 1, max_block_size = 40000, max_threads = ${threads},
                max_insert_threads = 1, output_format_parquet_parallel_encoding = 1,
                output_format_parquet_compression_method = 'zstd', output_format_compression_level = 15,
                engine_file_truncate_on_insert = 1
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
    then
        echo "max_threads = ${threads}: stopped at the deadline"
    else
        echo "max_threads = ${threads}: still running after 30 seconds"
    fi
done

rm -f "${CLICKHOUSE_USER_FILES}/${FILE}"
