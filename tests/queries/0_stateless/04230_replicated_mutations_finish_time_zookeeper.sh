#!/usr/bin/env bash
# Tags: replica, no-replicated-database, no-shared-merge-tree
# no-replicated-database: uses explicit local `ReplicatedMergeTree` replicas.
# no-shared-merge-tree: checks `ReplicatedMergeTree` mutation completion status.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

table_r1="replicated_mutations_finish_time_r1"
table_r2="replicated_mutations_finish_time_r2"
zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/replicated_mutations_finish_time"

function cleanup()
{
    # Leave local replicas in a normal state even if the test fails while merges are stopped.
    ${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES ${table_r1}" >/dev/null 2>&1 ||:
    ${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES ${table_r2}" >/dev/null 2>&1 ||:
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ${table_r1} SYNC" >/dev/null 2>&1 ||:
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ${table_r2} SYNC" >/dev/null 2>&1 ||:
}

function wait_for_unfinished_mutation()
{
    local table=$1
    local mutation_id=$2

    # Wait until the mutation is visible in `system.mutations` but has not completed locally.
    # This state is needed to verify that unfinished replicated mutations expose zero `finish_time`.
    for _ in {1..300}
    do
        local ready
        ready=$(${CLICKHOUSE_CLIENT} --query="
            SELECT count()
            FROM system.mutations
            WHERE database = '${CLICKHOUSE_DATABASE}'
              AND table = '${table}'
              AND mutation_id = '${mutation_id}'
              AND is_done = 0
              AND finish_time = 0
              AND parts_to_do > 0")

        if [[ "$ready" -eq 1 ]]; then
            return
        fi

        sleep 0.1
    done

    echo "Timed out while waiting for unfinished mutation ${mutation_id} on ${table}"
    ${CLICKHOUSE_CLIENT} --query="
        SELECT mutation_id, create_time, finish_time, is_done, parts_to_do, parts_to_do_names
        FROM system.mutations
        WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${table}'
        FORMAT Vertical"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE ${table_r1}
    (
        id UInt64,
        value UInt64
    )
    ENGINE = ReplicatedMergeTree('${zk_path}', 'r1')
    ORDER BY id
    SETTINGS finished_mutations_to_keep = 100"

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE ${table_r2}
    (
        id UInt64,
        value UInt64
    )
    ENGINE = ReplicatedMergeTree('${zk_path}', 'r2')
    ORDER BY id
    SETTINGS finished_mutations_to_keep = 100"

# `finished_mutations_to_keep` is a per-table `MergeTree` setting. Keep enough finished mutation
# records on both replicas so all expectations can read the full mutation history.
#
# Empty-table mutations should still record a finish time on every replica.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ${table_r1} UPDATE value = value + 1 WHERE id = 0 SETTINGS mutations_sync = 2"

# Expect two rows, one for each replica. Both must be done, have non-zero `finish_time`,
# have `finish_time >= create_time`, and have no remaining parts.
${CLICKHOUSE_CLIENT} --query="
    SELECT
        'empty',
        count(),
        countIf(finish_time != 0),
        countIf(finish_time >= create_time),
        countIf(is_done),
        sum(parts_to_do)
    FROM system.mutations
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table IN ('${table_r1}', '${table_r2}')"

${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 --query="
    INSERT INTO ${table_r1}
    SELECT number, number
    FROM numbers(20)"

# Inserts are written to one replica first. Sync the second replica before stopping merges on
# `table_r1` so both replicas start the data-mutation checks from the same set of parts.
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA ${table_r2}"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES ${table_r1}"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES ${table_r2}"

# Keep the mutation unfinished on one replica long enough to check that its finish time is still zero.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ${table_r1} UPDATE value = value + 1 WHERE id < 10"

wait_for_unfinished_mutation "${table_r1}" "0000000001"

# Expect the first data mutation to be visible on `table_r1` but unfinished there:
# zero `finish_time`, `is_done = 0`, and at least one part left to mutate.
${CLICKHOUSE_CLIENT} --query="
    SELECT
        'unfinished',
        count(),
        countIf(finish_time = 0),
        countIf(NOT is_done),
        countIf(parts_to_do > 0)
    FROM system.mutations
    WHERE database = '${CLICKHOUSE_DATABASE}'
      AND table = '${table_r1}'
      AND mutation_id = '0000000001'"

${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES ${table_r1}"
${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES ${table_r2}"

# Waiting for a later mutation on all replicas also waits for the previous mutation.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ${table_r1} UPDATE value = value + 10 WHERE id < 3 SETTINGS mutations_sync = 2"

wait_for_mutation "${table_r1}" "0000000002"
wait_for_mutation "${table_r2}" "0000000002"

# Expect all three mutations on both replicas to be finished: the empty-table mutation,
# the blocked mutation, and the later synchronous mutation.
${CLICKHOUSE_CLIENT} --query="
    SELECT
        'finished',
        count(),
        countIf(finish_time != 0),
        countIf(finish_time >= create_time),
        countIf(is_done),
        sum(parts_to_do)
    FROM system.mutations
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table IN ('${table_r1}', '${table_r2}')"

# When a user wants a single completion time for a replicated mutation across replicas,
# they can aggregate per-replica rows by `mutation_id` and use `max(finish_time)`.
# The output stays deterministic by checking counts instead of printing timestamps.
${CLICKHOUSE_CLIENT} --query="
    SELECT
        'max_finish_time',
        count(),
        countIf(replicas = 2),
        countIf(max_finish_time != 0),
        countIf(max_finish_time >= min_create_time)
    FROM
    (
        SELECT
            mutation_id,
            count() AS replicas,
            min(create_time) AS min_create_time,
            max(finish_time) AS max_finish_time
        FROM system.mutations
        WHERE database = '${CLICKHOUSE_DATABASE}' AND table IN ('${table_r1}', '${table_r2}')
        GROUP BY mutation_id
    )"

# Restart both replicas to verify that `finish_time` is persisted in Keeper and survives reload.
# `SYSTEM RESTART REPLICA` reinitialises the in-memory queue state from ZooKeeper, simulating
# what happens on server restart without requiring an actual process restart.
${CLICKHOUSE_CLIENT} --query="SYSTEM RESTART REPLICA ${table_r1}"
${CLICKHOUSE_CLIENT} --query="SYSTEM RESTART REPLICA ${table_r2}"

# After restart, all mutations must still report non-zero `finish_time` >= `create_time`.
${CLICKHOUSE_CLIENT} --query="
    SELECT
        'after_restart',
        count(),
        countIf(finish_time != 0),
        countIf(finish_time >= create_time),
        countIf(is_done),
        sum(parts_to_do)
    FROM system.mutations
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table IN ('${table_r1}', '${table_r2}')"

# The two data mutations add 10 from `id < 10` and 30 from `id < 3`.
# Both replicas must expose the same final data.
${CLICKHOUSE_CLIENT} --query="
    SELECT table, sum(value - id)
    FROM
    (
        SELECT '${table_r1}' AS table, value, id FROM ${table_r1}
        UNION ALL
        SELECT '${table_r2}' AS table, value, id FROM ${table_r2}
    )
    GROUP BY table
    ORDER BY table"
