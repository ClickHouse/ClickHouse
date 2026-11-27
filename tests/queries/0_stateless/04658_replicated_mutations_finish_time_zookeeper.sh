#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: creates two local ReplicatedMergeTree replicas with explicit arguments.
# no-shared-merge-tree: checks per-replica mutation finalization of ReplicatedMergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

table_r1="rep_mutations_finish_time_r1"
table_r2="rep_mutations_finish_time_r2"

# `finish_time` is stamped slightly after the mutation becomes observable as done
# through `system.mutations.is_done`, so poll for it.
function wait_for_finish_time()
{
    local table=$1
    for _ in {1..300}
    do
        if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT countIf(finish_time = 0) FROM system.mutations WHERE database = currentDatabase() AND table = '$table'") -eq 0 ]]; then
            return
        fi
        sleep 0.3
    done

    echo "Timed out while waiting for finish_time on table $table"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = '$table' FORMAT Vertical"
}

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE $table_r1 (id UInt64, value UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rep_mutations_finish_time', 'r1')
    ORDER BY id
    SETTINGS finished_mutations_to_keep = 100"

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE $table_r2 (id UInt64, value UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rep_mutations_finish_time', 'r2')
    ORDER BY id
    SETTINGS finished_mutations_to_keep = 100"

${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 --query="INSERT INTO $table_r1 SELECT number, number FROM numbers(20)"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA $table_r2"

# Stop mutation execution on both replicas to make the pending `finish_time = 0` state deterministic.
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES $table_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES $table_r2"

# `mutations_sync = 0` is explicit so that test environments enforcing synchronous
# mutations do not hang here while merges are stopped.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE $table_r1 UPDATE value = value + 1 WHERE id < 10 SETTINGS mutations_sync = 0"

# Wait until the mutation is visible on both replicas.
for _ in {1..300}
do
    if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table IN ('$table_r1', '$table_r2')") -eq 2 ]]; then
        break
    fi
    sleep 0.3
done

${CLICKHOUSE_CLIENT} --query="
    SELECT 'unfinished', count(), countIf(NOT is_done), countIf(finish_time = 0)
    FROM system.mutations
    WHERE database = currentDatabase() AND table IN ('$table_r1', '$table_r2')"

${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES $table_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES $table_r2"

wait_for_mutation "$table_r1" "0000000000"
wait_for_mutation "$table_r2" "0000000000"
wait_for_finish_time "$table_r1"
wait_for_finish_time "$table_r2"

# The second mutation is finalized in a separate round, so it advances the mutation pointer
# of each replica after the first one has already been finalized.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE $table_r1 UPDATE value = value + 10 WHERE id < 3 SETTINGS mutations_sync = 2"
wait_for_finish_time "$table_r1"
wait_for_finish_time "$table_r2"

${CLICKHOUSE_CLIENT} --query="
    SELECT 'finished', count(), countIf(is_done), countIf(finish_time >= create_time)
    FROM system.mutations
    WHERE database = currentDatabase() AND table IN ('$table_r1', '$table_r2')"

# After the replica state is reloaded, the finish time of the mutation at the mutation pointer
# is restored from the mtime of the pointer znode in Keeper, while the completion time of
# older mutations is unknown and reported as zero.
${CLICKHOUSE_CLIENT} --query="SYSTEM RESTART REPLICA $table_r1"

# Mutations are reloaded and marked as done asynchronously after the restart.
for _ in {1..300}
do
    if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT count() = 2 AND min(is_done) = 1 FROM system.mutations WHERE database = currentDatabase() AND table = '$table_r1'") -eq 1 ]]; then
        break
    fi
    sleep 0.3
done

${CLICKHOUSE_CLIENT} --query="
    SELECT 'after_restart',
        count(),
        countIf(is_done),
        countIf(mutation_id = '0000000000' AND finish_time = 0),
        countIf(mutation_id = '0000000001' AND finish_time >= create_time)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table_r1'"

# The replica that was not restarted keeps the completion times of both mutations.
${CLICKHOUSE_CLIENT} --query="
    SELECT 'kept_on_other_replica', count(), countIf(finish_time >= create_time)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table_r2'"

# The mutations add 1 to 10 rows and 10 to 3 rows on both replicas.
${CLICKHOUSE_CLIENT} --query="SELECT 'data_r1', sum(value - id) FROM $table_r1"
${CLICKHOUSE_CLIENT} --query="SELECT 'data_r2', sum(value - id) FROM $table_r2"

${CLICKHOUSE_CLIENT} --query="DROP TABLE $table_r1 SYNC"
${CLICKHOUSE_CLIENT} --query="DROP TABLE $table_r2 SYNC"
