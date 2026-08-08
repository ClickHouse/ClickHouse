#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: waits on the server-global failpoint rmt_mutate_task_pause_in_prepare

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Always disable the failpoint on exit. Under `set -e` an early exit would otherwise leave
# rmt_mutate_task_pause_in_prepare enabled, and every later MUTATE_PART on the whole server
# would then block indefinitely inside prepare().
trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
" 2>/dev/null || true' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_cross SYNC;

    CREATE TABLE t_lwu_cross (k UInt64, v String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_cross', '1')
    ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        merge_selecting_sleep_ms = 1000,
        max_merge_selecting_sleep_ms = 2000,
        merge_selecting_sleep_slowdown_factor = 1;

    SET insert_keeper_fault_injection_probability = 0;
    INSERT INTO t_lwu_cross VALUES (1, 'a') (2, 'b') (3, 'c');
    SYSTEM STOP MERGES t_lwu_cross;

    SET enable_lightweight_update = 1;
    UPDATE t_lwu_cross SET v = 'u1' WHERE k = 1;
"

# Stopped merges block execution of both MERGE_PARTS and MUTATE_PART, while assignment of
# both keeps running. So the whole scenario is built up first and only then released.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE t_lwu_cross UPDATE v = v || '_H' WHERE 1;
"

# ALTER returns before the MUTATE_PART entry is assigned (mutations_sync = 0 by default), and
# that entry carries the version the patch merge must not span, so wait for it.
for _ in {0..120}; do
    queued=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_cross' AND type = 'MUTATE_PART'")
    if [[ $queued != "0" ]]; then
        break
    fi
    sleep 1.0
done

for i in 3 4 5 6; do
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1, insert_keeper_fault_injection_probability = 0;
        UPDATE t_lwu_cross SET v = 'u$i' WHERE k = 2;
    "
done

# Drop the mutation metadata while its MUTATE_PART entry stays in the queue. The merge
# predicate then derives version 0 for every patch and sees no boundary, which is the state
# the finished-mutation cleaner reaches in production.
$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_cross'" > /dev/null

surviving=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 't_lwu_cross' AND type = 'MUTATE_PART'")
if [[ $surviving == "0" ]]; then
    echo "No MUTATE_PART entry survived the kill, so the state under test was never reached" >&2
    exit 1
fi

# Release execution and hold the mutation inside prepare(), so that it is provably still
# queued while the patch parts around it are merged.
$CLICKHOUSE_CLIENT --query "
    SYSTEM START MERGES t_lwu_cross;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE;
"

# The merge of the patches around the held mutation must be refused by the merge predicate.
# Only that refusal is accepted: a merge that merely sits in the queue, or one that does not
# span the mutation, would say nothing about the boundary.
decided=""
for _ in {0..120}; do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_cross'
          AND type = 'MERGE_PARTS' AND startsWith(new_part_name, 'patch')
          AND num_postponed > 0 AND postpone_reason LIKE '%would span the version%'")
    if [[ $postponed != "0" ]]; then
        decided="postponed"
        break
    fi
    sleep 1.0
done

if [[ -z $decided ]]; then
    queue=$($CLICKHOUSE_CLIENT --query "
        SELECT type, new_part_name, num_tries, num_postponed, postpone_reason
        FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_cross' FORMAT Vertical" || true)
    parts=$($CLICKHOUSE_CLIENT --query "
        SELECT name, level FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_cross' AND active ORDER BY name" || true)
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare" || true
    echo "The patch merge was not refused while the mutation was queued" >&2
    echo "$queue" >&2
    echo "$parts" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare"

# The mutation must complete and the postponed patch merge must follow it, so the
# queue drains. Before the fix the mutation aborted the server here instead.
for _ in {0..120}; do
    res=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_lwu_cross'")
    if [[ $res == "0" ]]; then
        break
    fi
    sleep 1.0
done

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_lwu_cross';
    SELECT k, v FROM t_lwu_cross ORDER BY k;
    DROP TABLE t_lwu_cross SYNC;
"
