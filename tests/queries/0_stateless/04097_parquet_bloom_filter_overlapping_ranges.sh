#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for a segfault/assertion failure in Parquet bloom filter
# prefetch range coalescing. When two overlapping prefetch ranges (bloom filter
# header and bloom filter data) share the same start offset and happen to be
# sorted with the larger range before the smaller one, the leftward coalescing
# scan included the larger range but did not extend end_offset. The created task
# was too small, causing out-of-bounds access in findAnyHash/getRangeData.
# https://github.com/ClickHouse/ClickHouse/issues/102257

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
FILE_PATH="${WORKING_DIR}/test.parquet"

# Create a parquet file with Nullable(String) columns containing a mix of
# empty strings, NULLs, and number strings. The bloom filter for col1 will
# have a small number of blocks (the key ingredient for the bug).
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${FILE_PATH}', 'Parquet')
    SELECT
        multiIf(
            cityHash64(number) % 113, ''::Nullable(String),
            cityHash64(number) % 5, NULL::Nullable(String),
            toString(number)
        ) AS col1,
        toString(number)::Nullable(String) AS col2,
        toString(number)::Nullable(String) AS col3,
        toString(number)::Nullable(String) AS col4,
        toString(number)::Nullable(String) AS col5,
        NULL::Nullable(String) AS col23
    FROM numbers(500000)
"

# Reference result without bloom filter
EXPECTED=$(${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM file('${FILE_PATH}', Parquet,
         'col1 String, col2 String, col3 String, col4 String, col5 String')
    WHERE col1 = '' AND col2 != ''
    SETTINGS input_format_parquet_bloom_filter_push_down = 0
")

# Run with bloom filter multiple times. Before the fix, this would either
# crash (segfault / LOGICAL_ERROR assertion) or return inconsistent results.
for _ in $(seq 1 5); do
    RESULT=$(${CLICKHOUSE_CLIENT} --query "
        SET max_threads = 1;
        SELECT count()
        FROM file('${FILE_PATH}', Parquet,
             'col1 String, col2 String, col3 String, col4 String, col5 String')
        WHERE col1 = '' AND col2 != ''
    ")
    if [ "$RESULT" != "$EXPECTED" ]; then
        echo "MISMATCH: got $RESULT, expected $EXPECTED"
        rm -rf "${WORKING_DIR}"
        exit 1
    fi
done

# Also test den-crane's simpler reproducer (https://github.com/ClickHouse/ClickHouse/issues/102231)
FILE_PATH2="${WORKING_DIR}/bloom_simple.parquet"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${FILE_PATH2}', 'Parquet')
    SELECT
        IF(number % 113 = 0, toString(number), '') AS col1,
        toString(number) AS col2
    FROM numbers(50000)
"

BLOOM_ON=$(${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM file('${FILE_PATH2}', Parquet, 'col1 String, col2 String')
    WHERE col1 = ''
    SETTINGS input_format_parquet_bloom_filter_push_down = 1
")
BLOOM_OFF=$(${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM file('${FILE_PATH2}', Parquet, 'col1 String, col2 String')
    WHERE col1 = ''
    SETTINGS input_format_parquet_bloom_filter_push_down = 0
")
if [ "$BLOOM_ON" != "$BLOOM_OFF" ]; then
    echo "SIMPLE MISMATCH: bloom_on=$BLOOM_ON bloom_off=$BLOOM_OFF"
    rm -rf "${WORKING_DIR}"
    exit 1
fi

echo "OK"

rm -rf "${WORKING_DIR}"
