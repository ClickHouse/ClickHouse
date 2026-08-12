#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel: Uses PAUSEABLE_ONCE failpoints that fire exactly once globally; concurrent tests
#   from another parallel run could steal the failpoint pause and cause this test to hang.
# no-shared-merge-tree: The failpoints are injected in StorageReplicatedMergeTree::removeTableNodesFromZooKeeper;
#   SharedMergeTree uses a different code path and would not trigger them.
# no-replicated-database: Uses explicit ReplicatedMergeTree ZooKeeper paths that conflict with
#   the DDL replication mechanism of DatabaseReplicated.
#
# Regression test: LOGICAL_ERROR "There is a race condition between creation and removal of
# replicated table" must not be thrown when a ZooKeeper session expires mid-cleanup, allowing
# a concurrent operation to finish the ZK node removal.
#
# Two scenarios are exercised via DROP TABLE SYNC (which calls removeTableNodesFromZooKeeper):
#   A) Session expires before tryGetChildren: the path is gone → ZNONODE on tryGetChildren.
#   B) Session expires before the final tryMulti: the path is gone → ZNONODE on tryMulti.
#
# Both are handled gracefully with a warning log instead of throwing a LOGICAL_ERROR exception.
# Case A returns true (completely removed — the root node is gone); case B returns false
# (incomplete — only the ephemeral lock may be missing, not the root).

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/race_test"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT replicated_table_remove_zk_before_get_children" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT replicated_table_remove_zk_before_final_multi" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS race_test SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}'" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

# ===== Scenario A: ZNONODE on tryGetChildren =====
# Simulates: ZooKeeper session expired after acquiring /dropped/lock but before tryGetChildren.
# A concurrent process deletes the entire zookeeper_path.
# The fix: return true (completely removed) with a warning instead of throwing LOGICAL_ERROR.

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE race_test (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/race_test', 'r1')
    ORDER BY a
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT replicated_table_remove_zk_before_get_children"

# Background: DROP TABLE SYNC triggers dropReplica → removeTableNodesFromZooKeeper.
# The failpoint will pause execution before tryGetChildren is called.
${CLICKHOUSE_CLIENT} -q "DROP TABLE race_test SYNC" &
DROP_PID=$!

# Wait until removeTableNodesFromZooKeeper is paused before tryGetChildren.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT replicated_table_remove_zk_before_get_children PAUSE"

# Simulate another process completing the ZK cleanup: delete the entire path.
${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}'" 2>/dev/null ||:

# Resume. tryGetChildren returns ZNONODE; with the fix we return true (completely removed) instead of throwing.
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT replicated_table_remove_zk_before_get_children"

# If LOGICAL_ERROR is thrown (release build) or server crashes (debug build),
# wait returns non-zero and set -e exits the script immediately.
wait $DROP_PID

echo "Scenario A passed"

# ===== Scenario B: ZNONODE on final tryMulti =====
# Simulates: ZooKeeper session expired after tryGetChildren succeeded but before the final tryMulti.
# A concurrent process deletes the entire zookeeper_path (including the ephemeral lock).
# The fix: treat ZNONODE as a concurrent removal (log warning, return false) instead of throwing.

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE race_test (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/race_test', 'r1')
    ORDER BY a
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT replicated_table_remove_zk_before_final_multi"

${CLICKHOUSE_CLIENT} -q "DROP TABLE race_test SYNC" &
DROP_PID=$!

# Wait until removeTableNodesFromZooKeeper is paused before the final tryMulti.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT replicated_table_remove_zk_before_final_multi PAUSE"

# Simulate another process completing the ZK cleanup.
${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}'" 2>/dev/null ||:

# Resume. tryMulti returns ZNONODE; with the fix we log a warning and return false.
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT replicated_table_remove_zk_before_final_multi"

wait $DROP_PID

echo "Scenario B passed"
