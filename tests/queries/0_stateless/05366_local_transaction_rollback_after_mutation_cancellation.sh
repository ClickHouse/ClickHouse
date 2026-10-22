#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: requires transactions and an encrypted S3 disk.
# Tag no-parallel: pauses a server-wide mutation failpoint.
# Tag no-replicated-database: replicated DDL does not preserve the transaction.
# Tag no-shared-merge-tree: tests the local `MergeTree` mutation task.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

set -euo pipefail

failpoint=mt_mutation_pause_after_part_rename
mutation_output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_mutation.log"
killer_output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_killer.log"
killer_pid=""

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}" ||:
    if [[ -n "$killer_pid" ]]; then
        wait "$killer_pid" ||:
    fi
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES local_rollback" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE local_rollback (n UInt64) ENGINE=MergeTree ORDER BY tuple()
    SETTINGS storage_policy='s3_no_cache_encrypted',
             max_bytes_to_merge_at_max_space_in_pool=0,
             old_parts_lifetime=3600, remove_rolled_back_parts_immediately=0,
             cleanup_delay_period=100000, max_cleanup_delay_period=100000,
             cleanup_delay_period_random_add=0, merge_tree_clear_old_parts_interval_seconds=100000;
    INSERT INTO local_rollback VALUES (1), (2);
"
tx 1 "BEGIN TRANSACTION"
transaction_id=$(tx 1 "SELECT transactionID()" | cut -f2-)
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${failpoint}"
tx_async 1 "ALTER TABLE local_rollback UPDATE n=n+10 WHERE 1 SETTINGS mutations_sync=1" > "$mutation_output"
# shellcheck disable=SC2086
timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"
part=$($CLICKHOUSE_CLIENT --query "SELECT result_part_name FROM system.merges WHERE database=currentDatabase() AND table='local_rollback' AND is_mutation")
[[ -n "$part" ]]

# The local transaction owns the renamed part but has not registered it with
# `MergeTreeTransaction`. Cancellation must not interrupt its metadata rollback.
# `SYSTEM STOP MERGES` synchronously sets the task's cancellation predicate without
# acquiring the parts lock or the background processing mutex.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES local_rollback"
$CLICKHOUSE_CLIENT --query "KILL TRANSACTION WHERE tid=${transaction_id} FORMAT Null" > "$killer_output" 2>&1 &
killer_pid=$!

# Rollback marks the transaction cancelled before trying to kill its mutations.
# Wait for that state change, not for `killMutation` to finish: a background selector
# can hold its mutex while waiting for the parts lock held by the paused task.
# A transaction already removed from `system.transactions` has finished rollback.
deadline=$((SECONDS + 60))
running=1
while (( SECONDS < deadline )); do
    # shellcheck disable=SC2086
    running=$(timeout 10 $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.transactions
        WHERE tid=${transaction_id} AND state != 'ROLLED_BACK';
    ")
    if [[ "$running" -eq 0 ]]; then break; fi
    sleep 0.1
done
[[ "$running" -eq 0 ]]

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
wait "$killer_pid"
killer_pid=""
tx_wait 1
grep -q UNFINISHED "$mutation_output"
tx 1 "ROLLBACK"

deadline=$((SECONDS + 60))
# shellcheck disable=SC2086
while [[ $(timeout 10 $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database=currentDatabase() AND table='local_rollback'") -gt 0 ]]; do
    (( SECONDS < deadline ))
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'rolled back part', countIf(creation_csn=18446744073709551615), countIf(active)
    FROM system.parts WHERE database=currentDatabase() AND table='local_rollback' AND name='${part}';
    SELECT 'rows', arraySort(groupArray(n)) FROM local_rollback SETTINGS implicit_transaction=1;
    INSERT INTO local_rollback VALUES (3);
    SELECT 'after insert', arraySort(groupArray(n)) FROM local_rollback SETTINGS implicit_transaction=1;
    DROP TABLE local_rollback SYNC;
"
