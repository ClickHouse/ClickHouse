#!/usr/bin/env bash
# Tags: no-fasttest, no-distributed-cache

# Test blob_storage_log for LocalObjectStorage.
# Delete events are checked with retries because BlobKillerThread
# removes blobs asynchronously after DROP TABLE SYNC.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_local_blob_log;

    CREATE TABLE test_local_blob_log (a Int32, b String)
    ENGINE = MergeTree() ORDER BY a
    SETTINGS disk = disk(type = 'local_blob_storage', path = '03776_test_local_blob_log/');

    INSERT INTO test_local_blob_log VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

    SELECT * FROM test_local_blob_log ORDER BY a;

    SYSTEM FLUSH LOGS blob_storage_log;

    -- Check that upload events were logged
    SELECT 'Upload events:', count() > 0 FROM system.blob_storage_log
    WHERE event_type = 'Upload'
        AND remote_path LIKE '%03776_test_local_blob_log%'
        AND data_size > 0
        AND error_code = 0
        AND event_date >= yesterday()
        AND event_time > now() - INTERVAL 5 MINUTE;

    DROP TABLE test_local_blob_log SYNC;
"

# Wait for async blob removal by BlobKillerThread and check delete events
for _ in $(seq 1 30); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS blob_storage_log"
    result=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.blob_storage_log
        WHERE event_type = 'Delete'
            AND remote_path LIKE '%03776_test_local_blob_log%'
            AND error_code = 0
            AND event_date >= yesterday()
            AND event_time > now() - INTERVAL 5 MINUTE
    ")
    if [ "$result" -gt 0 ]; then
        echo -e "Delete events:\t1"
        exit 0
    fi
    sleep 0.5
done

echo -e "Delete events:\t0"
