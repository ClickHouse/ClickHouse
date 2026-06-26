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

# A single insert produces one surviving part. Its block number is NOT guaranteed to be 0:
# under load a background allocation can consume lower block numbers, so the part can land on
# block 1 (all_1_1_0) or higher. Read the part's actual block number instead of assuming 0;
# the reset below is aligned to it so the collision is deterministic (assuming a fixed block
# made this test flaky: ~3% of runs got all_1_1_0 and the reset-to-0 insert no longer collided).
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO rmt VALUES (0)"
BLOCK=$($CLICKHOUSE_CLIENT --query "SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND active")

# Roll the partition block-number counter so the NEXT allocation re-issues the surviving part's
# block number. The number handed out is the cversion of /block_numbers/all (child-create count),
# so recreating the node resets it to 0 and creating BLOCK throwaway children advances it to BLOCK.
# This mimics a ZooKeeper metadata reset that re-hands-out a block number already used locally.
# keeper-client can transiently fail under parallel CI load and its errors go to /dev/null, so
# retry until system.zookeeper confirms cversion == BLOCK before the colliding INSERT (reading
# cversion does not change it, and with merges stopped on one quiescent part nothing else does).
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
# debug/sanitizer builds).
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO rmt VALUES (99)" 2>&1 \
    | grep -o -m1 "DUPLICATE_DATA_PART" || echo "NO_EXPECTED_ERROR"

# The server must still be alive and the data intact (1 row, no crash, no data loss).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM rmt"

$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC"
