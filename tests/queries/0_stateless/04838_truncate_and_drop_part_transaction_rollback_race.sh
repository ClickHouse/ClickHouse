#!/usr/bin/env bash

# Tags: no-parallel, no-ordinary-database, no-replicated-database, no-async-insert
# no-parallel: the pause failpoint is server-global, so a rollback in a concurrent test could steal the pause
# no-ordinary-database: transactions are not supported for databases with the Ordinary engine
# no-replicated-database: failpoints are single-server

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

set -e

TABLE_NAME="${CLICKHOUSE_DATABASE}_merge_tx_drop_rollback_race"
ROLLBACK_PAUSEPOINT="transaction_rollback_before_unlock_removal_tid_pause"

function check_state()
{
    $CLICKHOUSE_CLIENT --query "SELECT partition_id, name, removal_csn, removal_tid = (0, 0, '00000000-0000-0000-0000-000000000000')
        FROM system.parts WHERE table = '${TABLE_NAME}'
            AND database = currentDatabase() and active
        ORDER BY name
    "
}

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${ROLLBACK_PAUSEPOINT}" 2>/dev/null ||:
    tx_wait 1
    tx_wait 2
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_NAME} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE_NAME} SYNC"

# Max_bytes_to_merge_at_max_space_in_pool = 1 prevents automatic merging in the background
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${TABLE_NAME} (n Int64)
    ENGINE = MergeTree ORDER BY n
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1
"

# Create 4 parts to merge
$CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_NAME} VALUES (1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_NAME} VALUES (2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_NAME} VALUES (3)"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE_NAME} VALUES (4)"

# Initial state, all 4 parts should be active with removal_csn = 0, 4 rows in the table
check_state

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${ROLLBACK_PAUSEPOINT}"

# Run transaction dropping part 3 and roll it back
tx_sync 1 "BEGIN TRANSACTION"
tx_sync 1 "ALTER TABLE ${TABLE_NAME} DROP PART 'all_3_3_0'"
tx_async 1 "ROLLBACK"

# Deterministically wait until the background thread is paused inside the rollback
# when the part is already active, but before unlocking the removalTID.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${ROLLBACK_PAUSEPOINT} PAUSE"

# Rollback paused state, all 4 parts should be active with removal_csn = 0, part 3 should have removal_tid, 4 rows in the table
check_state

$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE ${TABLE_NAME} " 2>&1 | grep -oF "SERIALIZATION_ERROR" | uniq

# Merge failed state, should be the same as before
check_state

# Release the pause; the background ROLLBACK can now finish.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${ROLLBACK_PAUSEPOINT}"

tx_wait 1

# Final state, all 4 parts should be active with removal_csn = 0, removal_tid = 0, 4 rows in the table
check_state

# Non-transactional and transactional select should return the same rows. With the buggy implementation,
# parts all_1_1_0 and all_2_2_0 were marked as removing, but stayed Active, thus being visible to
# non-transactional reads and invisible for any transactional read.

$CLICKHOUSE_CLIENT --query "SELECT * FROM ${TABLE_NAME} ORDER BY n"

tx_sync 2 "BEGIN TRANSACTION"
tx_sync 2 "SELECT * FROM ${TABLE_NAME} ORDER BY n"
tx_sync 2 "COMMIT"

# Check that the table is still usable.
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE ${TABLE_NAME}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${TABLE_NAME}"
