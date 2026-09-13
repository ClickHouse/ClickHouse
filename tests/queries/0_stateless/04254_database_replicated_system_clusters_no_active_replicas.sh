#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

# Companion regression test for 04252. Covers the `NO_ACTIVE_REPLICAS`
# (`Code: 254`) shape of the same race: `DatabaseReplicated::tryGetCluster`
# /`tryGetAllGroupsCluster` reach `getClusterImpl`, which finds the
# `/replicas` znode present but empty (the first replica is not fully
# created yet or the last replica was just dropped) and throws
# `NO_ACTIVE_REPLICAS`. The exception is caught and the function returns
# nullptr; the caller skips the database. Before the fix the catch block
# logged the exception at `<Error>`, which the test runner forwards to
# client stderr at `send_logs_level=warning` and turns into spurious
# failures for any test that touches `system.clusters` (e.g.
# `01293_show_clusters`).
#
# We exercise the exact code path by:
#   1) creating a Replicated database (its `/replicas` is non-empty),
#   2) deleting the replica child znode but leaving an empty `/replicas`
#      via `rmr` + `touch`,
#   3) querying `system.clusters` with the default
#      `send_logs_level=warning` and capturing stderr.
#
# After the fix the catch in `tryGetCluster` / `tryGetAllGroupsCluster`
# logs `NO_ACTIVE_REPLICAS` at `<Information>` -- still visible in normal
# server logs but below the client log forwarding threshold -- so the
# captured stderr is empty. Without the fix the script prints one or more
# lines matching `Code: 254. DB::Exception: No replicas of database ...
# found ... (NO_ACTIVE_REPLICAS)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/04254/${CLICKHOUSE_TEST_UNIQUE_NAME}"
METADATA_FILE=""

# Cleanup. This test, like 04252, deliberately leaves the Replicated
# database in a non-droppable state (`/replicas` is empty, so
# `DROP DATABASE` cannot remove this replica from Keeper). Detach the
# database, remove the on-disk metadata file directly, and clean up the
# Keeper subtree. Must run on both success and failure paths so that
# subsequent invocations of the test do not fail at `CREATE DATABASE`.
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

# Remember where the database metadata file is so we can clean it up at
# the end even if `DROP DATABASE` fails (it will -- see cleanup note).
METADATA_FILE=$($CLICKHOUSE_CLIENT --query "SELECT metadata_path FROM system.databases WHERE name = '${DB}'")

# Force `getClusterImpl` to throw `NO_ACTIVE_REPLICAS` on the next call by
# removing the replica child and leaving an empty `/replicas` znode.
# `rmr ${ZK_PATH}/replicas` clears the subtree, then `touch` recreates
# `/replicas` as an empty znode (`getChildren` -> empty -> throw 254).
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}/replicas"
$CLICKHOUSE_KEEPER_CLIENT --query "touch ${ZK_PATH}/replicas"

# The query must succeed; the server-side `tryGetCluster` failure must NOT
# be forwarded to client stderr at the default `send_logs_level=warning`.
stderr=$($CLICKHOUSE_CLIENT --send_logs_level=warning --query "SELECT count() > 0 FROM system.clusters" 2>&1 >/dev/null)

if echo "${stderr}" | grep -qE "DatabaseReplicated.*Code: 254"; then
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
