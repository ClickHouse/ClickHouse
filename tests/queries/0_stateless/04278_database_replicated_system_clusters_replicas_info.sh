#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

# Companion regression test for 04252 / 04254. Exercises the third
# `DatabaseReplicated` method whose catch-block log level was lowered in
# this PR: `tryGetReplicasInfo`.
#
# `StorageSystemClusters::writeCluster` only calls `replicas_info_getter`
# (which invokes `DatabaseReplicated::tryGetReplicasInfo`) when the query
# requests one of the replica-state columns: `is_active`,
# `unsynced_after_recovery`, `replication_lag`, `recovery_time`, or
# `is_shared_catalog_cluster`. A `SELECT count()` such as the one used by
# 04252 reads metadata only and never enters this code path -- so the
# `tryGetReplicasInfo` log-level change needs its own coverage.
#
# We exercise the exact code path by:
#   1) creating a Replicated database (so `/replicas` is populated and
#      `tryGetCluster` returns a valid cluster on subsequent calls),
#   2) priming `tryGetCluster` so the cluster is cached and the next
#      `system.clusters` query reaches `tryGetReplicasInfo`,
#   3) deleting `${ZK_PATH}/max_log_ptr` so the `tryGet(paths)` in
#      `tryGetReplicasInfo` returns `ZNONODE` for `paths[0]` and the
#      function throws `Coordination::Exception(ZNONODE)`, which maps to
#      `KEEPER_EXCEPTION` (`Code: 999`),
#   4) querying `SELECT is_active FROM system.clusters` with the default
#      `send_logs_level=warning` and capturing stderr.
#
# After the fix the catch in `tryGetReplicasInfo` logs `KEEPER_EXCEPTION`
# at `<Information>` -- still visible in normal server logs but below the
# client log forwarding threshold -- so the captured stderr is empty.
# Without the fix the script prints one or more lines matching
# `DatabaseReplicated.*Code: 999`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/04278/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Cleanup. Unlike 04252 / 04254 the `/replicas` znode is intact here, so
# `DROP DATABASE` works normally. Keeper subtree cleanup is also done for
# good measure. Must run on both success and failure paths.
cleanup() {
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null || true
    $CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Make sure no leftover state from a previous run interferes.
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null || true
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"

# Prime `tryGetCluster` so the cluster is cached and the next query
# proceeds to `tryGetReplicasInfo`. Without this priming step
# `tryGetCluster` might still build the cluster on the assertion query
# and the timing of the subsequent `tryGet(/max_log_ptr)` is less
# predictable.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.clusters WHERE cluster = '${DB}'" >/dev/null

# Force `tryGetReplicasInfo` to throw on the next call by deleting
# `/max_log_ptr`. The function does `tryGet(['/max_log_ptr', ...])` and
# throws `Coordination::Exception(error)` when `error != ZOK` for the
# first path.
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}/max_log_ptr"

# The query must succeed; the server-side `tryGetReplicasInfo` failure
# must NOT be forwarded to client stderr at the default
# `send_logs_level=warning`. Select a column that triggers
# `replicas_info_getter` (here: `is_active`).
stderr=$($CLICKHOUSE_CLIENT --send_logs_level=warning --query "SELECT count() FROM (SELECT is_active FROM system.clusters)" 2>&1 >/dev/null)

if echo "${stderr}" | grep -qE "DatabaseReplicated.*Code: (254|279|999)"; then
    echo "FAIL: server log noise leaked to client stderr at send_logs_level=warning"
    echo "${stderr}" | head -3
    exit 1
fi

# Sanity: the query itself must still work (the exception is swallowed --
# only the log level changed).
result=$($CLICKHOUSE_CLIENT --send_logs_level=warning --query "SELECT count() > 0 FROM (SELECT is_active FROM system.clusters)" 2>/dev/null)
if [[ "${result}" != "1" ]]; then
    echo "FAIL: SELECT is_active FROM system.clusters did not return a result; got '${result}'"
    exit 1
fi

echo "OK"
