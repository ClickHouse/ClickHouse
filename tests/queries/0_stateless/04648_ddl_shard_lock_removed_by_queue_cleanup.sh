#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-shared-merge-tree
# no-fasttest: needs ZooKeeper/Keeper and a Replicated database.
# no-shared-merge-tree: uses an explicit ReplicatedMergeTree zookeeper_path for the second replica.
#
# Regression test: DDLWorker::tryExecuteQueryOnSingleReplica takes an ephemeral lock at
# <ddl entry>/shards/<shard>/lock and holds it until processTask returns. DDLWorker::cleanupQueue
# recursively deletes an outdated entry's whole subtree (its keep list holds only "finished"), so
# that lock node can legitimately disappear while the executing session is still healthy.
# ~ZooKeeperLock -> unlock() used to raise
#   LOGICAL_ERROR "Lock is lost, node does not exist. Path: .../shards/<shard>/lock"
# for exactly that case, which aborts the server in debug/sanitizer builds and whenever
# abort_on_logical_error is set (as CI does). The DDL lock is now constructed with
# throw_if_lost = false, so a missing node is logged instead.
#
# The window is produced the same way as in the original occurrence: alter_sync = 2 blocks inside
# StorageReplicatedMergeTree::alter waiting for an inactive replica, so the executor sits in
# processTask holding the lock; the entry subtree is then removed (what cleanupQueue does), and the
# subsequent Code 341 UNFINISHED is rethrown on the initial-query path, unwinding processTask and
# destroying the lock.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RDB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
AUX="aux_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_ZK="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${AUX} SYNC SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${RDB} SYNC SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${DB_ZK}'" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

start_time=$(${CLICKHOUSE_CLIENT} -q "SELECT now64(6)")

# distributed_ddl_output_mode = 'none' on every setup statement: the per-host status rows a
# Replicated database prints otherwise are not part of what this test asserts.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "CREATE DATABASE ${RDB} ENGINE = Replicated('${DB_ZK}', 's1', 'r1')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "CREATE TABLE ${RDB}.t (x UInt64, y String) ENGINE = ReplicatedMergeTree ORDER BY x"

table_zk=$(${CLICKHOUSE_CLIENT} -q "SELECT zookeeper_path FROM system.replicas WHERE database = '${RDB}' AND table = 't'")

# Register a second replica of the same table and detach it, so it never processes the log entry.
# alter_sync = 2 then waits for it and finally fails with Code 341 UNFINISHED. DETACH must be SYNC:
# a later ATTACH is not performed here, but SYNC keeps the shutdown deterministic.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "CREATE DATABASE ${AUX}"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "
    CREATE TABLE ${AUX}.t2 (x UInt64, y String)
    ENGINE = ReplicatedMergeTree('${table_zk}', 'r2') ORDER BY x
    SETTINGS database_replicated_allow_replicated_engine_arguments = 3
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "DETACH TABLE ${AUX}.t2 SYNC"

# MODIFY COLUMN on a ReplicatedMergeTree is routed to a single replica per shard
# (DDLWorker::taskShouldBeExecutedOnLeader), which is what makes the shard lock be taken.
${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${RDB}.t MODIFY COLUMN y Nullable(String)
    SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = 15,
             distributed_ddl_task_timeout = 120, distributed_ddl_output_mode = 'throw'
" > /dev/null 2>&1 &
alter_pid=$!

# Wait until the executor has actually created the shard lock node. Synchronizing on the lock node
# itself (rather than on the entry, which exists earlier) guarantees the lock is held right now.
lock_entry=""
for _ in {1..600}; do
    entry=$(${CLICKHOUSE_CLIENT} -q "
        SELECT name FROM system.zookeeper
        WHERE path = '${DB_ZK}/log' AND name LIKE 'query-%' ORDER BY name DESC LIMIT 1" 2>/dev/null)
    if [ -n "$entry" ]; then
        present=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count() FROM system.zookeeper
            WHERE path = '${DB_ZK}/log/${entry}/shards/s1' AND name = 'lock'" 2>/dev/null || echo 0)
        if [ "$present" = "1" ]; then lock_entry="$entry"; break; fi
    fi
    sleep 0.05
done

if [ -z "$lock_entry" ]; then
    echo "FAIL: the DDL shard lock node never appeared, the test did not set up the race"
    wait "$alter_pid" 2>/dev/null ||:
    exit 1
fi

# This is what DDLWorker::cleanupQueue does to an outdated entry: recursively remove the entry
# subtree, which takes the live ephemeral shard lock with it.
${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${DB_ZK}/log/${lock_entry}'" > /dev/null 2>&1

# Assert the lock node is really gone, so a passing test cannot mean "the race was not set up".
gone=$(${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.zookeeper
    WHERE path = '${DB_ZK}/log/${lock_entry}/shards/s1' AND name = 'lock'" 2>/dev/null || echo 0)
if [ "$gone" != "0" ]; then
    echo "FAIL: the shard lock node is still present, the race was not set up"
    wait "$alter_pid" 2>/dev/null ||:
    exit 1
fi

# The ALTER is EXPECTED to fail with Code 341 UNFINISHED on both the fixed and the broken build
# (the wait for the detached replica genuinely times out), so its status is not the assertion.
wait "$alter_pid" 2>/dev/null ||:

# The server must still be running: without the fix the LOGICAL_ERROR above aborted it.
${CLICKHOUSE_CLIENT} -q "SELECT 'server is alive'"

# The discriminating observable. ~ZooKeeperLock swallows unlock() exceptions, and with the fix the
# very same text is still emitted at <Information> from the tolerant branch, so neither server
# liveness alone nor the absence of the message can tell the fixed and the broken build apart on a
# build where LOGICAL_ERROR does not abort. Assert instead that the message was never Fatal, i.e.
# that no LOGICAL_ERROR was raised for it.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('${start_time}', 6)
      AND level = 'Fatal' AND message LIKE '%Lock is lost%'
"
