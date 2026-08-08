#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: waits on the server-global failpoint rmt_mutate_task_pause_in_prepare

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

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

# Hold the MUTATE_PART for version 2 inside prepare() so that it is still queued
# while the patch parts around it are merged.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE t_lwu_cross UPDATE v = v || '_H' WHERE 1;
"

for i in 3 4 5 6; do
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1, insert_keeper_fault_injection_probability = 0;
        UPDATE t_lwu_cross SET v = 'u$i' WHERE k = 2;
    "
done

# Drop the mutation metadata while its MUTATE_PART entry stays in the queue. The
# merge predicate then derives version 0 for every patch and sees no boundary,
# which is the state the finished-mutation cleaner reaches in production.
$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_cross'" > /dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_lwu_cross"

# The patch merge must have been decided while the mutation is still queued: either
# refused by the merge predicate, or already executed. A MERGE_PARTS entry merely
# present in the queue is not enough, because the entry appears when the log is
# pulled, before it is ever evaluated.
observed=""
for _ in {0..120}; do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_cross'
          AND type = 'MERGE_PARTS' AND startsWith(new_part_name, 'patch')
          AND num_postponed > 0 AND postpone_reason LIKE '%would span the version%'")
    if [[ $postponed != "0" ]]; then
        observed="postponed"
        break
    fi
    merged=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_cross'
          AND active AND startsWith(name, 'patch') AND level > 0")
    if [[ $merged != "0" ]]; then
        observed="merged"
        break
    fi
    sleep 1.0
done

if [[ -z $observed ]]; then
    queue=$($CLICKHOUSE_CLIENT --query "
        SELECT type, new_part_name, num_tries, num_postponed, postpone_reason
        FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_cross' FORMAT Vertical" || true)
    parts=$($CLICKHOUSE_CLIENT --query "
        SELECT name, level FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_cross' AND active ORDER BY name" || true)
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare" || true
    echo "Timed out waiting for the patch merge to be decided" >&2
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
