#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a global pauseable failpoint; a predicate mutation from a concurrent test could pause on it.
# no-replicated-database: the test relies on the mutation running on the same server that waits on the failpoint.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: the partition set pruned from the mutation predicate must be recomputed
# after ZBADVERSION. A concurrent insert on the same replica can create a new matching
# partition between the pruning and the block number allocation; the rows of that partition
# must not escape the mutation.

FAILPOINT="rmt_mutation_prune_pause_before_block_allocation"

# The failpoint is global and stays enabled until something disables it, so leaving it behind parks
# every later predicate mutation on this server. The trap covers the exits it can - a failing step,
# Ctrl-C locally - but not a harness timeout, which allows 0.1s between `SIGTERM` and `SIGKILL`, less
# than a client round trip. Hence also the cleanup on the way in, which is what actually recovers a
# server poisoned by a previous killed run.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_mut_prune_race SYNC" 2>/dev/null ||:
}
trap cleanup EXIT INT TERM
cleanup

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;

    DROP TABLE IF EXISTS t_mut_prune_race SYNC;

    CREATE TABLE t_mut_prune_race (p UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_race', '1')
    PARTITION BY p ORDER BY x;

    INSERT INTO t_mut_prune_race SELECT 1, number FROM numbers(10);

    SYSTEM ENABLE FAILPOINT ${FAILPOINT};
"

# Only partition 1 exists when the pruned partition set is computed. Pinning the pruning setting is
# load-bearing: with it off the mutation takes the unpruned path, the failpoint below is never
# reached, and the wait never returns.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_mut_prune_race DELETE WHERE p >= 1 SETTINGS mutations_sync = 1, optimize_mutations_with_partition_pruning = 1" &

# Wait until the mutation is paused between pruning and block number allocation.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

# Create a new partition that also matches the predicate.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    INSERT INTO t_mut_prune_race SELECT 2, number FROM numbers(10);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"

wait

# The mutation must cover the concurrently created partition too.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_prune_race;
    DROP TABLE t_mut_prune_race SYNC;
"
