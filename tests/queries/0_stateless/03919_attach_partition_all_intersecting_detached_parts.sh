#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-parallel, no-shared-merge-tree

# Test that ATTACH PARTITION ALL handles intersecting parts in the detached directory
# gracefully instead of throwing a LOGICAL_ERROR.
# SYSTEM RESTORE REPLICA moves ALL parts (including outdated merge intermediates) to
# detached. These parts can intersect (e.g., all_1_5_4 and all_1_10_6 from different
# merge stages). A subsequent ATTACH PARTITION ALL must handle this.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_intersect"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_intersect (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_intersect', 'r1')
    ORDER BY x
"

# Insert several parts and let them merge progressively to create a chain of
# intermediate merge results: all_1_2_1, all_1_3_2, all_1_4_3, all_1_5_4, etc.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_intersect"
for i in $(seq 1 10); do
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_intersect VALUES ($i)"
done
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_intersect"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_intersect FINAL"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_intersect"

# Get the ZK path before detaching.
ZK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 't_intersect'")

# Detach the table first — this releases the ZK session and ephemeral nodes,
# allowing us to drop the replica metadata from ZK.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_intersect"

# Drop the replica's ZK metadata while the table is detached.
# This bypasses the "can't drop local replica" check.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$ZK_PATH'"

# Re-attach — without the ZK replica node, the table enters readonly mode.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_intersect"

# Wait for the replica to become readonly.
for _ in $(seq 1 100); do
    is_readonly=$($CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't_intersect'")
    if [ "$is_readonly" = "1" ]; then
        break
    fi
    sleep 0.1
done

# SYSTEM RESTORE REPLICA moves ALL parts (active + outdated) to detached,
# then re-attaches from ZooKeeper. The outdated intermediate merge results
# remain in detached and intersect with each other.
$CLICKHOUSE_CLIENT -q "SYSTEM RESTORE REPLICA t_intersect"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_intersect"

# Now detached directory has intersecting parts from different merge stages.
# ATTACH PARTITION ALL should handle this gracefully.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_intersect ATTACH PARTITION ALL"

# Verify we can read data.
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM t_intersect"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_intersect"
