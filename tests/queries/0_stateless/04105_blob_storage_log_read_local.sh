#!/usr/bin/env bash
# Tags: no-fasttest, no-distributed-cache

# Test Read events in blob_storage_log for LocalObjectStorage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_local_blob_log_read;

    CREATE TABLE test_local_blob_log_read (a Int32, b String)
    ENGINE = MergeTree() ORDER BY a
    SETTINGS disk = disk(type = 'local_blob_storage', path = '04105_test_local_blob_log_read/');

    INSERT INTO test_local_blob_log_read VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

    SELECT * FROM test_local_blob_log_read ORDER BY a
        SETTINGS enable_blob_storage_log_for_read_operations = 1;

    SYSTEM FLUSH LOGS blob_storage_log;

    -- Read events should have non-zero data_size and elapsed_microseconds.
    SELECT 'Read events:',
        count() > 0,
        countIf(error_code = 0) = count(),
        countIf(data_size > 0) > 0,
        countIf(elapsed_microseconds > 0) > 0
    FROM system.blob_storage_log
    WHERE event_type = 'Read'
        AND remote_path LIKE '%04105_test_local_blob_log_read%'
        AND event_date >= yesterday()
        AND event_time > now() - INTERVAL 5 MINUTE;

    DROP TABLE test_local_blob_log_read SYNC;
"
