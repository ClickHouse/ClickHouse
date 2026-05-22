#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# Verify the ReaderExecutor observability surface added together: per-query
# ProfileEvents, registered HistogramMetrics, and rows in the
# system.reader_executor_log table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_re_obs"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE test_re_obs (key UInt32, value String)
    ENGINE=MergeTree() ORDER BY key
    SETTINGS storage_policy='s3_cache', min_bytes_for_wide_part=10485760
"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO test_re_obs SELECT number, toString(number) FROM numbers(10000)
"

# Run a read with reader-executor logging enabled. Tag the query so we can find it.
QUERY_TAG="04262_re_obs"
$CLICKHOUSE_CLIENT --use_reader_executor=1 --enable_reader_executor_log=1 --query "
    SELECT '$QUERY_TAG', * FROM test_re_obs FORMAT Null
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS reader_executor_log, query_log"

# 1. system.reader_executor_log has rows for our query.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(count() = 0, 'no reader_executor_log entries for tagged query')
    FROM system.reader_executor_log
    WHERE query_id IN (
        SELECT query_id FROM system.query_log
        WHERE query LIKE '%$QUERY_TAG%'
          AND type = 'QueryFinish'
          AND current_database = currentDatabase()
    )
    FORMAT Null
"

# 2. The query's ProfileEvents map contains at least one ReaderExecutor* key.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        countIf(arrayExists(k -> startsWith(k, 'ReaderExecutor'), mapKeys(ProfileEvents))) = 0,
        'no ReaderExecutor ProfileEvents on the tagged query')
    FROM system.query_log
    WHERE query LIKE '%$QUERY_TAG%'
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
    FORMAT Null
"

# 3. All five ReaderExecutor histograms are registered.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(uniqExact(metric) < 5, 'fewer than 5 reader_executor histograms registered')
    FROM system.histogram_metrics
    WHERE startsWith(metric, 'reader_executor_')
    FORMAT Null
"

echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE test_re_obs"
