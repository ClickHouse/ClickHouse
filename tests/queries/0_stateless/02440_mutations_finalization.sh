#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-parallel
# no-shared-merge-tree -- SMT doesn't assign mutations when merges are stopped.
# no-parallel -- uses server-wide failpoints that affect all RMT tables.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE mut (n int)
        ENGINE = ReplicatedMergeTree('/test/02440/{database}/mut', '1')
        ORDER BY tuple();

    INSERT INTO mut VALUES (1);

    -- Pause the merge-selecting task and wait for it to actually stop
    -- before creating the mutation, so no iteration can process it.
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;

    ALTER TABLE mut UPDATE n = 2 WHERE n = 1;

    -- Resume the scheduler with no-free-threads active so it skips the
    -- mutation, then check that it is still pending.
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;

    SYSTEM SYNC REPLICA mut PULL;
    SELECT mutation_id, command, parts_to_do_names, is_done
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 'mut';

    DETACH TABLE mut;

    -- Re-enable pause BEFORE attach so the new table's scheduler hits it
    -- on its very first run (otherwise it completes a cycle and sleeps).
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    ATTACH TABLE mut;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;

    -- Mutation should not be finished yet.
    SELECT * FROM mut;
    SELECT mutation_id, command, parts_to_do_names, is_done
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 'mut';
"

# Release both failpoints so the scheduler gets CPU time before we poll.
cleanup

# Poll until the mutation completes — SYSTEM SYNC REPLICA does not trigger
# the merge-selecting task, so we must wait for it to wake on its own.
for _ in $(seq 1 120); do
    is_done=$($CLICKHOUSE_CLIENT --query \
        "SELECT is_done FROM system.mutations
         WHERE database = currentDatabase() AND table = 'mut'
           AND mutation_id = '0000000000'" 2>/dev/null)
    [ "$is_done" = "1" ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SELECT * FROM mut"
$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, command, parts_to_do_names
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 'mut'
"
