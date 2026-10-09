#!/usr/bin/env bash
# A lightweight `UPDATE` of a column whose `RENAME COLUMN` mutation is still pending was acknowledged
# and then invisible on `ReplicatedMergeTree`: the patch part carrying the update was written at
# metadata version 0, which is behind every metadata mutation, so the pending rename was applied to it
# on read and the patch was looked up under the column's old name. The patch stores the new name, so
# nothing was found and the update was dropped from the result.
#
# The compaction of patch parts rewrote them from their synthetic metadata as well and lost the version
# again, so the same updates went invisible once a background merge of the patches went through.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RENAME COLUMN` with `alter_sync = 0` returns before the replica has even applied the metadata
# change, and the update below has to run after that but before the mutation materializes.
wait_for_column() {
    for _ in {1..100}
    do
        if [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = '$1' AND name = '$2'")" == "1" ]]
        then
            return
        fi
        sleep 0.1
    done
    echo "the rename of $1 was not applied to the metadata"
}

for table in t_lwu_pending_rename t_lwu_rename_after
do
    ${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${table} SYNC;
    CREATE TABLE ${table} (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${table}', '1')
    ORDER BY id PARTITION BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO ${table} SELECT number, 0 FROM numbers(2000);
    SYSTEM STOP MERGES ${table};
    "
done

echo 'an update of a column whose rename is pending'
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_lwu_pending_rename RENAME COLUMN v TO w SETTINGS alter_sync = 0"
wait_for_column t_lwu_pending_rename w
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_lwu_pending_rename SET w = 1 WHERE id < 200"
${CLICKHOUSE_CLIENT} -q "SELECT 'while the rename is pending', sum(w) FROM t_lwu_pending_rename"
${CLICKHOUSE_CLIENT} -q "
SYSTEM START MERGES t_lwu_pending_rename;
ALTER TABLE t_lwu_pending_rename DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', sum(w) FROM t_lwu_pending_rename;
"

echo 'and a rename that follows the update is still applied to the patch'
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_lwu_rename_after SET v = 1 WHERE id < 200"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_lwu_rename_after RENAME COLUMN v TO w SETTINGS alter_sync = 0"
wait_for_column t_lwu_rename_after w
${CLICKHOUSE_CLIENT} -q "SELECT 'while the rename is pending', sum(w) FROM t_lwu_rename_after"
${CLICKHOUSE_CLIENT} -q "
SYSTEM START MERGES t_lwu_rename_after;
ALTER TABLE t_lwu_rename_after DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', sum(w) FROM t_lwu_rename_after;
"

echo 'and a merge of the patches keeps the version they were written at'
# The rename has to stay unmaterialized across the merge of the patches. `SYSTEM STOP MERGES` would hold
# the merge as well, so instead keep the merge selector from scheduling any mutation: it still selects
# merges, and the explicit `OPTIMIZE` below is a merge. Only that `OPTIMIZE` may merge the patches.
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_lwu_merged_patches SYNC;
CREATE TABLE t_lwu_merged_patches (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_merged_patches', '1')
ORDER BY id PARTITION BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    max_replicated_mutations_in_queue = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_lwu_merged_patches SELECT number, 0 FROM numbers(2000);
ALTER TABLE t_lwu_merged_patches RENAME COLUMN v TO w SETTINGS alter_sync = 0;
"
wait_for_column t_lwu_merged_patches w
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_lwu_merged_patches SET w = 1 WHERE id < 200"
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_lwu_merged_patches SET w = 2 WHERE id >= 200 AND id < 400"
${CLICKHOUSE_CLIENT} -q "SELECT 'before the merge of the patches', sum(w) FROM t_lwu_merged_patches"

# The partition of the patches is hashed from their structure, so read it instead of spelling it out.
patch_partition_id=$(${CLICKHOUSE_CLIENT} -q "
SELECT DISTINCT partition_id FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_merged_patches' AND active AND startsWith(partition_id, 'patch')")
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_lwu_merged_patches PARTITION ID '$patch_partition_id' FINAL"
${CLICKHOUSE_CLIENT} -q "
SELECT 'active patch parts after the merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_merged_patches' AND active AND startsWith(partition_id, 'patch');
SELECT 'while the rename is pending', sum(w) FROM t_lwu_merged_patches;
ALTER TABLE t_lwu_merged_patches MODIFY SETTING max_replicated_mutations_in_queue = 8;
ALTER TABLE t_lwu_merged_patches DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', sum(w) FROM t_lwu_merged_patches;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_lwu_merged_patches SYNC; DROP TABLE t_lwu_rename_after SYNC; DROP TABLE t_lwu_pending_rename SYNC"
