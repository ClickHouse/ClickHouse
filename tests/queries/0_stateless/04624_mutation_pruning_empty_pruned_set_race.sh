#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a global pauseable failpoint; a predicate mutation from a concurrent test could pause on it.
# no-replicated-database: the test relies on the mutation running on the same server that waits on the failpoint.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: when the pruned partition set is initially empty (the predicate matches no
# existing partition), the mutation must still run the version-checked lock/check path. A
# concurrent insert on the same replica can create a new matching partition after the pruning
# and before the mutation entry is written; the rows of that partition must not escape the
# mutation just because the pruned set was empty at the time of the analysis.

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;

    DROP TABLE IF EXISTS t_mut_prune_empty_race SYNC;

    CREATE TABLE t_mut_prune_empty_race (p UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_empty_race', '1')
    PARTITION BY p ORDER BY x;

    INSERT INTO t_mut_prune_empty_race SELECT 1, number FROM numbers(10);

    SYSTEM ENABLE FAILPOINT rmt_mutation_prune_pause_before_block_allocation;
"

# The predicate matches only partition 2, which does not exist yet, so the pruned set is empty.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_mut_prune_empty_race DELETE WHERE p = 2 SETTINGS mutations_sync = 1" &

# Wait until the mutation is paused between pruning and block number allocation.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutation_prune_pause_before_block_allocation PAUSE"

# Create the matching partition after the (empty) pruned set was computed.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    INSERT INTO t_mut_prune_empty_race SELECT 2, number FROM numbers(10);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutation_prune_pause_before_block_allocation"

wait

# The mutation must cover the concurrently created partition 2, deleting its 10 rows.
# Only the 10 rows of partition 1 must remain.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_prune_empty_race;
    SELECT count() FROM t_mut_prune_empty_race WHERE p = 2;
    DROP TABLE t_mut_prune_empty_race SYNC;
"
