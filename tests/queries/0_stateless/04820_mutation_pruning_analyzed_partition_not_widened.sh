#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a global pauseable failpoint; a predicate mutation from a concurrent test could pause on it.
# no-replicated-database: the test relies on the mutation running on the same server that waits on the failpoint.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: the widening with ZK-only partitions must compare against the partition set
# the pruning analysis itself iterated, not against a separately read local partition list.
# A same-replica insert can commit a new partition right before the pruning analysis runs: the
# pruner then analyzes that partition and, when the predicate does not match it, rules it out.
# Widening based on an earlier local snapshot would re-add it (it is present in `block_numbers`
# but absent from the stale snapshot), so the mutation would allocate a block number in a
# partition that was already proven unaffected.

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;

    DROP TABLE IF EXISTS t_mut_prune_no_widen SYNC;

    CREATE TABLE t_mut_prune_no_widen (p UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_no_widen', '1')
    PARTITION BY p ORDER BY x;

    INSERT INTO t_mut_prune_no_widen SELECT 1, number FROM numbers(10);

    SYSTEM ENABLE FAILPOINT rmt_mutation_prune_pause_before_analysis;
"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_mut_prune_no_widen DELETE WHERE p = 1 SETTINGS mutations_sync = 1" &

# Wait until the mutation is paused right before the pruning analysis.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutation_prune_pause_before_analysis PAUSE"

# Create a new partition that does not match the predicate. It commits fully while the mutation
# is paused, so the pruning analysis observes it locally and its znode is already in
# ZooKeeper `block_numbers` when the widening runs.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    INSERT INTO t_mut_prune_no_widen SELECT 2, number FROM numbers(10);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutation_prune_pause_before_analysis"

wait

# The mutation must cover only partition 1: partition 2 was analyzed and ruled out by the
# pruner, so the widening must not re-add it.
$CLICKHOUSE_CLIENT --query "
    SELECT block_numbers.partition_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_mut_prune_no_widen';

    SELECT p, count() FROM t_mut_prune_no_widen GROUP BY p ORDER BY p;

    DROP TABLE t_mut_prune_no_widen SYNC;
"
