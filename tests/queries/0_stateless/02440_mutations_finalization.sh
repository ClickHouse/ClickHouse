#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# no-shared-merge-tree -- SMT doesn't assign mutations when merges are stopped.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE mut (n int)
        ENGINE = ReplicatedMergeTree('/test/02440/${CLICKHOUSE_DATABASE}/mut', '1')
        ORDER BY tuple();

    INSERT INTO mut VALUES (1);

    -- Pause the merge-selecting task before submitting the mutation so the
    -- scheduler never gets a chance to process the MUTATE_PART entry.
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;
    ALTER TABLE mut UPDATE n = 2 WHERE n = 1;
    SYSTEM WAIT FAILPOINT rmt_merge_selecting_task_pause_when_scheduled PAUSE;

    SYSTEM SYNC REPLICA mut PULL;
    SELECT mutation_id, command, parts_to_do_names, is_done
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 'mut';

    -- Make the scheduler think there are no free pool threads so it skips
    -- the mutation, then release the pause so DETACH can proceed.
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled;

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

# Release both failpoints outside the main block so the scheduler actually
# gets CPU time to run before we start polling.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_pause_when_scheduled"

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
