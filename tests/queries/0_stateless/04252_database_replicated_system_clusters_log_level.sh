#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

# Regression test for `01293_show_clusters` flakiness on parallel stateless
# test lanes.
#
# When `system.clusters` is queried while another session is creating or
# dropping a Replicated database, `DatabaseReplicated::tryGetCluster` may
# observe a missing or empty `/replicas` znode and throw `KEEPER_EXCEPTION`
# or `ALL_CONNECTION_TRIES_FAILED`. The exception is caught and ignored
# (the function returns nullptr, callers skip the database), but
# historically the catch block logged the exception at `<Error>` level. The
# test runner forwards server logs at `send_logs_level=warning` and above
# to the client stderr, so any test that incidentally hit this race --
# including the trivial `SHOW CLUSTERS` test -- showed transient stderr
# noise and was reported as a failure even though every assertion passed.
#
# This test reproduces the exact failure path by creating a Replicated
# database (the catalog entry caches `cluster=null`), deleting its
# `/replicas` znode via `keeper-client`, and running
# `SELECT count() FROM system.clusters` with the default
# `send_logs_level=warning` while capturing stderr.
#
# After the fix the catch in `tryGetCluster` / `tryGetAllGroupsCluster` /
# `tryGetReplicasInfo` logs the expected coordination/connection errors
# (`KEEPER_EXCEPTION`, `ALL_CONNECTION_TRIES_FAILED`) at `<Information>`
# -- still visible in normal server logs but below the client log
# forwarding threshold -- so the captured stderr is empty. Without the
# fix the same script prints one or more lines matching
# `Code: 999. Coordination::Exception` or
# `Code: 279. DB::Exception: No active replicas`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/04252/${CLICKHOUSE_TEST_UNIQUE_NAME}"
METADATA_FILE=""

# Cleanup. `DROP DATABASE` here would try to remove this replica from
# `/replicas` in Keeper, which we deleted above, so it fails. Removing the
# database the conventional way also fails because the keeper-client tool
# cannot escape the `|` character in `shard1|replica1` to recreate the
# missing znodes. Instead:
#   1) detach the database from the local catalog (works regardless of the
#      Keeper state),
#   2) delete the on-disk metadata file directly so the orphan won't be
#      reattached on the next server start,
#   3) clean up the Keeper subtree.
#
# Must run on both the success and failure paths -- this test deliberately
# leaves the Replicated database in a non-droppable state, and skipping
# cleanup makes any subsequent invocation of the test fail at
# `CREATE DATABASE` because the previous database is still attached.
cleanup() {
    $CLICKHOUSE_CLIENT --query "DETACH DATABASE ${DB} SYNC" 2>/dev/null || true
    [[ -n "${METADATA_FILE}" && -f "${METADATA_FILE}" ]] && rm -f "${METADATA_FILE}"
    $CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Make sure no leftover state from a previous run interferes.
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB} SYNC" 2>/dev/null || true
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"

# Remember where the database metadata file is so we can clean it up at the
# end even if `DROP DATABASE` fails (it will -- see the cleanup note below).
METADATA_FILE=$($CLICKHOUSE_CLIENT --query "SELECT metadata_path FROM system.databases WHERE name = '${DB}'")

# Force `tryGetCluster` to throw on the next call by deleting `/replicas`.
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}/replicas"

# The query must succeed; the server-side `tryGetCluster` failure must NOT
# be forwarded to client stderr at the default `send_logs_level=warning`.
stderr=$($CLICKHOUSE_CLIENT --send_logs_level=warning --query "SELECT count() > 0 FROM system.clusters" 2>&1 >/dev/null)

if echo "${stderr}" | grep -qE "DatabaseReplicated.*Code: (279|999)"; then
    echo "FAIL: server log noise leaked to client stderr at send_logs_level=warning"
    echo "${stderr}" | head -3
    exit 1
fi

# Sanity: the query itself must still work (the exception is swallowed --
# only the log level changed).
result=$($CLICKHOUSE_CLIENT --send_logs_level=warning --query "SELECT count() > 0 FROM system.clusters" 2>/dev/null)
if [[ "${result}" != "1" ]]; then
    echo "FAIL: SELECT FROM system.clusters did not return a result; got '${result}'"
    exit 1
fi

echo "OK"
