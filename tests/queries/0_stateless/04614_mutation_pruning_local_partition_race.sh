#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a global pauseable failpoint; a predicate mutation from a concurrent test could pause on it.
# no-replicated-database: the test relies on the mutation running on the same server that waits on the failpoint.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a same-replica insert can create a new matching partition between the
# pruning analysis and reading the partition list from ZooKeeper. By that point the new
# partition is fully committed: it is visible both locally and in `block_numbers`, so
# neither the version check nor a pruning recomputation on retry fires. The widening with
# ZK-only partitions must catch it, which requires the local partition set to be captured
# before the pruning analysis, not after `getChildren`.

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;

    DROP TABLE IF EXISTS t_mut_prune_local_race SYNC;

    CREATE TABLE t_mut_prune_local_race (p UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_local_race', '1')
    PARTITION BY p ORDER BY x;

    INSERT INTO t_mut_prune_local_race SELECT 1, number FROM numbers(10);

    SYSTEM ENABLE FAILPOINT rmt_mutation_prune_pause_before_zk_partition_list;
"

# Only partition 1 exists when the pruned partition set is computed.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_mut_prune_local_race DELETE WHERE p >= 1 SETTINGS mutations_sync = 1" &

# Wait until the mutation is paused between the pruning analysis and reading the ZooKeeper partition list.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutation_prune_pause_before_zk_partition_list PAUSE"

# Create a new partition that also matches the predicate. It commits fully while the
# mutation is paused, so it is already local and already in ZooKeeper when the mutation resumes.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    INSERT INTO t_mut_prune_local_race SELECT 2, number FROM numbers(10);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutation_prune_pause_before_zk_partition_list"

wait

# The mutation must cover the concurrently created partition too.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_prune_local_race;
    DROP TABLE t_mut_prune_local_race SYNC;
"
