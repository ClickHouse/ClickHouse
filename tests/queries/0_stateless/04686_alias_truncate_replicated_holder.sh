#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-fasttest
# Tag no-shared-merge-tree: SYSTEM STOP REPLICATION QUEUES does not stall a SharedMergeTree truncate the same way
# Tag no-replicated-database: TRUNCATE would run as a distributed DDL, so the stalled truncate is not the one we wait on
# Tag no-fasttest: needs a real Keeper

# TRUNCATE through an Alias forwards the interpreter's own exclusive lock on the MergeTree branch,
# because StorageReplicatedMergeTree::truncate releases it to keep the truncate asynchronous. Handing
# it a fresh empty holder instead leaves the ALIAS write-locked for that whole asynchronous phase, so
# a reader through the alias gets DEADLOCK_AVOIDED. Every other cell of 04677 truncates plain
# MergeTree, whose truncate ignores the holder, so only a Replicated leaf pins this forwarding.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unconditional: a failure between STOP and START would otherwise leave the queue stopped for
# whatever runs next against this server.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES $CLICKHOUSE_DATABASE.rmt" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rmt_alias;
    DROP TABLE IF EXISTS rmt;

    CREATE TABLE rmt (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', 'r1') ORDER BY k;
    INSERT INTO rmt SELECT number FROM numbers(100);
    CREATE TABLE rmt_alias ENGINE = Alias($CLICKHOUSE_DATABASE, 'rmt');
"

# With the queue stalled the truncate's own alter_sync = 1 wait cannot complete, so it stays inside
# its asynchronous phase -- after table_lock.release() -- for as long as this test needs.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES $CLICKHOUSE_DATABASE.rmt"

TRUNCATE_ID="truncate_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$TRUNCATE_ID" -q "TRUNCATE TABLE rmt_alias" > /dev/null 2>&1 &

# The precondition is asserted, not assumed. Mere presence in system.processes is not enough: the
# ProcessList entry is published before the interpreter is built, hence before the lock is taken and
# long before it is released. The queued DROP_RANGE entry is the observable proof that
# StorageReplicatedMergeTree::truncate got past table_lock.release() and is now in its wait.
truncate_in_async_phase=0
for _ in {1..300}; do
    if [[ $($CLICKHOUSE_CLIENT -q "
                SELECT count() FROM system.replication_queue
                WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'rmt' AND type = 'DROP_RANGE'") -gt 0 ]]; then
        truncate_in_async_phase=1
        break
    fi
    sleep 0.05
done
echo -e "truncate reached async phase\t$truncate_in_async_phase"

# The oracle. IdentifierResolver takes lockForShare on the RESOLVED storage, which for this query is
# the alias itself, so this read can only proceed if release() ran on a real forwarded holder.
# Status and stderr are reported as two separate rows on purpose: a bare grep for DEADLOCK_AVOIDED
# reads 0 both when the read succeeds and when it fails for an unrelated reason.
alias_err=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rmt_alias SETTINGS lock_acquire_timeout = 3" 2>&1 > /dev/null)
alias_rc=$?
echo -e "read through alias succeeded\t$((alias_rc == 0 ? 1 : 0))"
echo -e "read through alias blocked\t$(echo "$alias_err" | grep -c -m1 "DEADLOCK_AVOIDED")"

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES $CLICKHOUSE_DATABASE.rmt"
wait

# And the truncate itself still did its job once the queue was allowed to drain.
$CLICKHOUSE_CLIENT -q "
    SELECT 'rows after truncate', count() FROM rmt;
    INSERT INTO rmt SELECT 1;
    SELECT 'through alias', count() FROM rmt_alias;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rmt_alias;
    DROP TABLE rmt;
"
