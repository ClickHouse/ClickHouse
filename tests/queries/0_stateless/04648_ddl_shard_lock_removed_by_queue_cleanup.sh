#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-shared-merge-tree
# no-fasttest: needs ZooKeeper/Keeper and a `Replicated` database.
# no-shared-merge-tree: uses an explicit `ReplicatedMergeTree` `zookeeper_path` for the second replica.
#
# Regression test: the DDL shard lock at `<ddl entry>/shards/<shard>/lock` may legitimately
# disappear while its owning session is still healthy, because the lock lives inside the queue entry
# subtree and other actors remove that subtree recursively. `ZooKeeperLock::unlock` must tolerate that
# instead of raising `LOGICAL_ERROR` `Lock is lost, node does not exist`, which aborts the server in
# debug and sanitizer builds (in a release build it is caught in `~ZooKeeperLock` and only logged).
# The removal below stands in for any such actor; what is asserted is `unlock`'s reaction to it.
#
# Synchronization invariant: the test proceeds only once the lock node is observed to EXIST, which
# is the point at which the executor provably holds it; the node is then removed, and its absence
# is re-checked. Both checks fail loudly, so a pass cannot mean the race was never set up.
#
# The `ALTER` under test is EXPECTED to end in Code 341 UNFINISHED on every build (the
# `alter_sync = 2` wait for the detached replica genuinely times out), so its own status can never
# be the assertion.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RDB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
AUX="aux_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_ZK="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb"
TABLE_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/t"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${AUX} SYNC SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${RDB} SYNC SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null ||:
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${DB_ZK}'" 2>/dev/null ||:
    # The second replica is dropped while DETACHed, which leaves its replica znode behind, so the
    # explicit table path has to be removed here or a re-run would find a stale replica.
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${TABLE_ZK}'" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

start_time=$(${CLICKHOUSE_CLIENT} -q "SELECT now64(6)")

# `distributed_ddl_output_mode = 'none'` on every setup statement: the per-host status rows a
# `Replicated` database prints otherwise are not part of what this test asserts.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "CREATE DATABASE ${RDB} ENGINE = Replicated('${DB_ZK}', 's1', 'r1')"
# The table path is given explicitly and scoped to the per-run test prefix, so that the second
# replica below can name the same path. The `{shard}`/`{replica}` macros are required: a
# `Replicated` database rejects an explicit path that cannot differ between shards and replicas.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "
    CREATE TABLE ${RDB}.t (x UInt64, y String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t/{shard}', '{replica}') ORDER BY x
    SETTINGS database_replicated_allow_replicated_engine_arguments = 3
"

# Read the resolved path back rather than re-deriving it, so the second replica provably attaches
# to the first one's table instead of to a path this test merely believes is the same.
table_zk=$(${CLICKHOUSE_CLIENT} -q "SELECT zookeeper_path FROM system.replicas WHERE database = '${RDB}' AND table = 't'")

# Register a second replica of the same table and detach it, so it never processes the log entry.
# `alter_sync = 2` then waits for it and finally fails with Code 341 UNFINISHED. `DETACH` must be
# `SYNC`: a later `ATTACH` is not performed here, but `SYNC` keeps the shutdown deterministic.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "CREATE DATABASE ${AUX}"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "
    CREATE TABLE ${AUX}.t2 (x UInt64, y String)
    ENGINE = ReplicatedMergeTree('${table_zk}', 'r2') ORDER BY x
    SETTINGS database_replicated_allow_replicated_engine_arguments = 3
"

# Assert the two replicas really share one table, so a passing test cannot mean the second replica
# silently landed on a different path and the `alter_sync = 2` wait had nothing to wait for.
replicas=$(${CLICKHOUSE_CLIENT} -q "
    SELECT uniqExact(zookeeper_path) = 1 AND count() = 2
    FROM system.replicas WHERE database IN ('${RDB}', '${AUX}') AND table IN ('t', 't2')")
if [ "$replicas" != "1" ]; then
    echo "FAIL: the two replicas do not share one zookeeper_path, the test did not set up the race"
    exit 1
fi

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "DETACH TABLE ${AUX}.t2 SYNC"

# `MODIFY COLUMN` on a `ReplicatedMergeTree` is routed to a single replica per shard
# (`DDLWorker::taskShouldBeExecutedOnLeader`), which is what makes the shard lock be taken.
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

# Remove the entry subtree recursively, which takes the live ephemeral shard lock with it. The lock's
# owner does not own this subtree, so it cannot rule out such a removal while the lock is held.
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

wait "$alter_pid" 2>/dev/null ||:

# Liveness half: without the fix the `LOGICAL_ERROR` aborted the server here. Stateless CI always
# links `abort_on_logical_error.yaml`, so this holds even on the non-sanitizer flavours.
${CLICKHOUSE_CLIENT} -q "SELECT 'server is alive'"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"

# Positive control: the tolerant branch of `unlock` reported the loss for OUR lock path exactly
# once. Liveness alone cannot see this, so without it a change turning `unlock` into a silent
# no-op would pass. `max_rows_to_read = 0` is required because `system.text_log` is ordered by
# `event_date, event_time`, so the timestamp predicate is not a primary-key range.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('${start_time}', 6)
      AND level = 'Information'
      AND message LIKE '%Lock is lost, node does not exist%'
      AND message LIKE '%${DB_ZK}/log/${lock_entry}/shards/s1/lock%'
    SETTINGS max_rows_to_read = 0
"

# Negative control: that loss was not reported as an error for OUR lock path.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('${start_time}', 6)
      AND level IN ('Fatal', 'Critical', 'Error')
      AND message LIKE '%Lock is lost%'
      AND message LIKE '%${DB_ZK}/log/${lock_entry}/shards/s1/lock%'
    SETTINGS max_rows_to_read = 0
"
