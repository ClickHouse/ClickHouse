#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101544
# When reading a tar archive from S3 containing parquet files with different
# schemas using schema_inference_mode=union, columns present only in some
# files were silently returned as all-null due to Parquet metadata cache
# key collision: all files in the same archive shared the same cache key.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_04092"

# Create two parquet files with different schemas on S3
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url='http://localhost:11111/${S3_PATH}/test_1.parquet', format=Parquet)
    SELECT number + 1 AS timestamp, number + 1.0 AS a
    FROM numbers(3)
    SETTINGS s3_truncate_on_insert=1
"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url='http://localhost:11111/${S3_PATH}/test_2.parquet', format=Parquet)
    SELECT number + 4 AS timestamp, number + 4.0 AS a, toNullable(['hello', 'world', 'foo'][number + 1]) AS b
    FROM numbers(3)
    SETTINGS s3_truncate_on_insert=1
"

# Download the parquet files, create a tar archive, and upload it
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

curl -s "http://localhost:11111/${S3_PATH}/test_1.parquet" -o "${TMP_DIR}/test_1.parquet"
curl -s "http://localhost:11111/${S3_PATH}/test_2.parquet" -o "${TMP_DIR}/test_2.parquet"
tar cf "${TMP_DIR}/test.parquet.tar" -C "${TMP_DIR}" test_1.parquet test_2.parquet
curl -s -X PUT "http://localhost:11111/${S3_PATH}/test.parquet.tar" --upload-file "${TMP_DIR}/test.parquet.tar"

# Query with union mode — column b should have real values from test_2.parquet
$CLICKHOUSE_CLIENT -q "
    SET schema_inference_mode = 'union';
    SET schema_inference_use_cache_for_s3 = 0;
    SELECT *
    FROM s3(s3_conn, url='http://localhost:11111/${S3_PATH}/test.parquet.tar :: *.parquet')
    ORDER BY timestamp
"
