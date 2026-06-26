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

# Stop merges so the surviving part keeps block number 0. A background merge could otherwise
# replace all_0_0_0 with a covering part, and the forced collision below would no longer hit an
# exact-name match (that race made this test flaky when it inserted several parts).
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES rmt"

# One insert into the fresh table deterministically produces part all_0_0_0 (block number 0).
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO rmt VALUES (0)"

# Precondition guard: the surviving part must be at block 0 for the reset below to collide with it.
$CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND active"

# Roll the partition block-number counter back to zero while the part stays in the working set.
# Recreating /block_numbers/all resets its sequential counter (the next allocated block number is
# derived from the node's cversion), mimicking a ZooKeeper metadata reset under a surviving local
# part. keeper-client can transiently fail under load and the reset would silently not take effect,
# so retry until system.zookeeper confirms the counter is back at zero. This keeps the collision
# below deterministic; without it the next INSERT may get block 1 and not collide (flaky).
for _ in {1..30}; do
    $CLICKHOUSE_KEEPER_CLIENT -q "rmr '$ZK_PATH/block_numbers/all'" >/dev/null 2>&1
    $CLICKHOUSE_KEEPER_CLIENT -q "touch '$ZK_PATH/block_numbers/all'" >/dev/null 2>&1
    cversion=$($CLICKHOUSE_CLIENT --query "SELECT cversion FROM system.zookeeper WHERE path = '$ZK_PATH/block_numbers' AND name = 'all'")
    [ "$cversion" = "0" ] && break
    sleep 0.3
done

# The next INSERT re-issues block number 0 and collides with the surviving local all_0_0_0.
# It must fail with DUPLICATE_DATA_PART, NOT raise a LOGICAL_ERROR (which aborts the server in
# debug/sanitizer builds).
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO rmt VALUES (99)" 2>&1 \
    | grep -o -m1 "DUPLICATE_DATA_PART" || echo "NO_EXPECTED_ERROR"

# The server must still be alive and the data intact (1 row, no crash, no data loss).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM rmt"

$CLICKHOUSE_CLIENT --query "DROP TABLE rmt SYNC"
