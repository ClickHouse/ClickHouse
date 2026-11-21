#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: uses an explicit ReplicatedMergeTree ZooKeeper path and resets its
#   block-number counter directly; SharedMergeTree allocates block numbers differently.
# no-replicated-database: uses an explicit ReplicatedMergeTree ZooKeeper path that conflicts
#   with the DDL replication mechanism of DatabaseReplicated.
#
# Regression test for a LOGICAL_ERROR exception ("Part with name ... is already written by
# concurrent request") in ReplicatedMergeTreeSink::commitPart. When the ZooKeeper block-number
# counter is reset while a local part at the same block number survives (Keeper metadata loss,
# replica re-creation, or a DROP/lost-replica race), the next INSERT re-issues an already-used
# block number and renameTempPartAndAdd finds the surviving part. The sink used to treat this as
# an impossible condition and throw LOGICAL_ERROR. In debug/sanitizer builds (abort_on_logical_error)
# this aborts the server; in release builds it is a catchable exception. It must instead fail the
# INSERT with a normal (non-fatal) DUPLICATE_DATA_PART error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS rmt SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE rmt (k UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', 'r1') ORDER BY k
"

# Resolve the table's ZooKeeper path from the server so the keeper-client commands below act on
# exactly the path the table was created with (single source of truth, no duplicated literal).
ZK_PATH=$($CLICKHOUSE_CLIENT --query "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'rmt'")

# Stop merges so the surviving part is not replaced by a covering part: the forced collision
# below needs an exact-name match against the part that stays in the working set.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES rmt"

# Pin insert_keeper_fault_injection_probability=0 on both inserts. The CI default (0.01, from
# tests/config/users.d/insert_keeper_retries.xml) makes a faulted insert retry, and the retry
# allocates a HIGHER block number; that broke the forced collision below (~1-2% flake: the
# surviving part moved off block 0, or the colliding insert skipped past it). With faults off
# the allocation is deterministic; the surviving part's block is still read and aligned below
# so the collision holds even if that block is ever non-zero.
$CLICKHOUSE_CLIENT --async_insert 0 --insert_keeper_fault_injection_probability 0 --query "INSERT INTO rmt VALUES (0)"
BLOCK=$($CLICKHOUSE_CLIENT --query "SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND active")

# Roll the partition block-number counter back so the NEXT allocation re-issues the surviving
# part's block number. Recreating the node resets the sequential counter to 0, then BLOCK
# throwaway persistent-sequential children advance it to BLOCK. This mimics a ZooKeeper metadata
# reset that re-hands-out a block number already used locally. keeper-client can transiently fail
# under parallel CI load (errors go to /dev/null), so retry until system.zookeeper confirms the
# node's child-version reached BLOCK before the colliding INSERT.
for _ in {1..30}; do
    $CLICKHOUSE_KEEPER_CLIENT -q "rmr '$ZK_PATH/block_numbers/all'" >/dev/null 2>&1
    $CLICKHOUSE_KEEPER_CLIENT -q "touch '$ZK_PATH/block_numbers/all'" >/dev/null 2>&1
    for ((i = 0; i < BLOCK; i++)); do
        $CLICKHOUSE_KEEPER_CLIENT -q "create '$ZK_PATH/block_numbers/all/block-' 'x' persistent sequential" >/dev/null 2>&1
    done
    cversion=$($CLICKHOUSE_CLIENT --query "SELECT cversion FROM system.zookeeper WHERE path = '$ZK_PATH/block_numbers' AND name = 'all'")
    [ "$cversion" = "$BLOCK" ] && break
    sleep 0.3
done

# The next INSERT re-issues the surviving part's block number and collides with it locally.
# It must fail with DUPLICATE_DATA_PART, NOT raise a LOGICAL_ERROR (which aborts the server in
# debug/sanitizer builds). Faults off here too: a retry would allocate a higher, non-colliding
# block number and hide the condition under test.
$CLICKHOUSE_CLIENT --async_insert 0 --insert_keeper_fault_injection_probability 0 --query "INSERT INTO rmt VALUES (99)" 2>&1 \
    | grep -o -m1 "DUPLICATE_DATA_PART" || echo "NO_EXPECTED_ERROR"

# The server must still be alive and the data intact (1 row, no crash, no data loss).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM rmt"

$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC"
