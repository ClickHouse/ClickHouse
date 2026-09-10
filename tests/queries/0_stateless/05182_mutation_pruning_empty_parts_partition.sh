#!/usr/bin/env bash
# Mutation partition pruning ruled a partition out of a mutation because the parts the initiating
# replica holds in it are all empty - the state a delete-all mutation or a `TTL` expiry leaves behind
# until the cleanup thread removes the part - and recorded it as analyzed, which also kept the scope
# from being widened with the partitions only ZooKeeper knows. Rows another replica acknowledged in
# that partition then survived the mutation on every replica.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation of the parts this replica holds can finish while the queue is stopped, so the scope is
# read from the newest mutation of the table rather than from an unfinished one, and the wait below
# polls instead of relying on a later mutation to cover the same partitions.
wait_for_mutations() {
    for _ in {1..600}
    do
        pending=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count() FROM system.mutations
            WHERE database = currentDatabase() AND table = '$1' AND NOT is_done")
        if [[ "$pending" == "0" ]]; then
            return
        fi
        sleep 0.2
    done
    echo "the mutations of $1 did not finish"
}

count_mutations() {
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$1'"
}

# `system.mutations` of a replicated table is refreshed by the queue-updating task, so a mutation
# submitted with `alter_sync = 0` is not there the moment the `ALTER` returns.
newest_mutation_scope() {
    local table=$1
    local previous_count=$2

    for _ in {1..600}
    do
        if [[ "$(count_mutations "$table")" -gt "$previous_count" ]]; then
            break
        fi
        sleep 0.2
    done

    ${CLICKHOUSE_CLIENT} -q "
        SELECT arraySort(\`block_numbers.partition_id\`) FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table'
        ORDER BY mutation_id DESC LIMIT 1"
}

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_pruning_empty_r1 SYNC;
DROP TABLE IF EXISTS t_pruning_empty_r2 SYNC;

CREATE TABLE t_pruning_empty_r1 (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_pruning_empty', 'r1')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;

CREATE TABLE t_pruning_empty_r2 (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_pruning_empty', 'r2')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;

INSERT INTO t_pruning_empty_r1 SELECT 1, number FROM numbers(50);
INSERT INTO t_pruning_empty_r1 SELECT 2, number FROM numbers(50);
SYSTEM SYNC REPLICA t_pruning_empty_r2;

-- Empty out partition 2 everywhere; the 0-row part stays active.
ALTER TABLE t_pruning_empty_r1 DELETE WHERE p = 2 SETTINGS mutations_sync = 2;
SELECT 'the empty part is still active', count(), sum(rows)
FROM system.parts WHERE database = currentDatabase() AND table = 't_pruning_empty_r1' AND partition_id = '2' AND active;

-- Stands in for ordinary replication lag on the initiator.
SYSTEM STOP REPLICATION QUEUES t_pruning_empty_r1;
INSERT INTO t_pruning_empty_r2 SELECT 2, 1000000 + number FROM numbers(30);

"

mutations_before=$(count_mutations t_pruning_empty_r1)
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_pruning_empty_r1 DELETE WHERE p = 2 SETTINGS alter_sync = 0"

echo -n 'the mutation is scoped to the partition '
newest_mutation_scope t_pruning_empty_r1 "$mutations_before"

${CLICKHOUSE_CLIENT} -q "SYSTEM START REPLICATION QUEUES t_pruning_empty_r1; SYSTEM SYNC REPLICA t_pruning_empty_r1"
wait_for_mutations t_pruning_empty_r1

${CLICKHOUSE_CLIENT} -q "
SELECT 'r1', count() FROM t_pruning_empty_r1 WHERE p = 2;
SELECT 'r2', count() FROM t_pruning_empty_r2 WHERE p = 2;
SELECT 'the other partition is untouched', count() FROM t_pruning_empty_r1 WHERE p = 1;
"

echo 'and a partition the predicate does not match stays out of the scope'
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_pruning_scope SYNC;
CREATE TABLE t_pruning_scope (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_pruning_scope', 'r1')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;
INSERT INTO t_pruning_scope SELECT 1, number FROM numbers(10);
INSERT INTO t_pruning_scope SELECT 2, number FROM numbers(10);
ALTER TABLE t_pruning_scope DELETE WHERE p = 2 SETTINGS mutations_sync = 2;
"

mutations_before=$(count_mutations t_pruning_scope)
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_pruning_scope DELETE WHERE p = 1 SETTINGS alter_sync = 0"

newest_mutation_scope t_pruning_scope "$mutations_before"
wait_for_mutations t_pruning_scope
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_pruning_scope"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE t_pruning_scope SYNC;
DROP TABLE t_pruning_empty_r2 SYNC;
DROP TABLE t_pruning_empty_r1 SYNC;
"
