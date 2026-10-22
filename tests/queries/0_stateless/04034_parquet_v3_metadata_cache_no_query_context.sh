#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# Regression test: S3Queue background threads read Parquet files without a
# query context on CurrentThread. Before the fix, the format factory lambda
# called CurrentThread::getQueryContext()->getParquetMetadataCache() which
# crashed because getQueryContext() returned null in background threads.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_04034"

# Write a Parquet file to S3
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data.parquet', format = Parquet)
    SELECT number AS id, toString(number) AS name FROM numbers(100)
    SETTINGS s3_truncate_on_insert = 1
"

# Create destination table
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dest (id UInt64, name String)
    ENGINE = MergeTree ORDER BY id
"

# Create S3Queue table reading Parquet with metadata cache + native reader v3.
# The background processing thread has no query context on CurrentThread.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.queue (id UInt64, name String)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.parquet', 'Parquet')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/04034_s3queue',
        mode = 'unordered',
        after_processing = 'keep',
        s3queue_processing_threads_num = 1,
        s3queue_polling_min_timeout_ms = 100,
        s3queue_polling_max_timeout_ms = 500,
        use_parquet_metadata_cache = 1
"

# Create MV to trigger background reads from S3Queue into dest table
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv TO ${CLICKHOUSE_DATABASE}.dest
    AS SELECT id, name FROM ${CLICKHOUSE_DATABASE}.queue
"

# Wait for S3Queue to process the file (up to 30 seconds)
for _ in {1..60}; do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dest")
    if [ "$count" -ge 100 ]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id) FROM ${CLICKHOUSE_DATABASE}.dest"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.mv SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.queue SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dest SYNC"
