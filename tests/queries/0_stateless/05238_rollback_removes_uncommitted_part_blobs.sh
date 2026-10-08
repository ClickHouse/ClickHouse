#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree, no-replicated-database, no-distributed-cache
# no-parallel: enables a global failpoint that the asserted server error depends on.
# no-fasttest: tests that arm a failpoint are left to the full stateless jobs.
# no-shared-merge-tree: the test is about the empty covering parts that plain `MergeTree` writes.
# no-replicated-database, no-distributed-cache: the test uses its own object storage disk.

# On an object storage disk the blobs of a part are uploaded when its files are written, but its
# directory only appears when its storage transaction commits. The failpoint makes the `TRUNCATE`
# fail after the first empty part has been committed, so the other empty parts are rolled back
# with uncommitted storage transactions: their blobs must not stay behind.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_rollback_blobs SYNC;

    CREATE TABLE t_rollback_blobs (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS disk = disk(type = 'local_blob_storage', path = '$disk_path');

    SYSTEM STOP MERGES t_rollback_blobs;
    INSERT INTO t_rollback_blobs SETTINGS async_insert = 0 VALUES (1);
    INSERT INTO t_rollback_blobs SETTINGS async_insert = 0 VALUES (2);
    INSERT INTO t_rollback_blobs SETTINGS async_insert = 0 VALUES (3);

    SYSTEM ENABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;
    TRUNCATE TABLE t_rollback_blobs; -- { serverError FAULT_INJECTED }
    SYSTEM DISABLE FAILPOINT merge_tree_transaction_fail_after_empty_part_rename;

    SELECT count() FROM t_rollback_blobs;
    DROP TABLE t_rollback_blobs SYNC;
"

# The blobs of the dropped table are removed asynchronously, so wait for every non-empty uploaded blob
# to have a successful delete event.
for _ in $(seq 1 60); do
    read -r uploaded leaked < <($CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS blob_storage_log;
        SELECT countIf(uploaded) > 0, countIf(uploaded AND NOT deleted) FROM
        (
            SELECT
                countIf(event_type = 'Upload' AND data_size > 0 AND error_code = 0) > 0 AS uploaded,
                countIf(event_type = 'Delete' AND error_code = 0) > 0 AS deleted
            FROM system.blob_storage_log
            WHERE event_date >= yesterday() AND remote_path LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}%'
            GROUP BY remote_path
        )")
    [ "$leaked" = "0" ] && break
    sleep 0.5
done
echo "uploaded blobs: $uploaded"
echo "leaked blobs: $leaked"
