#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest, no-parallel, no-replicated-database
# no-parallel: uses a PAUSEABLE_ONCE failpoint, which is process-global and fires exactly once;
#   a concurrent copy of this test could steal the pause and hang.
# no-replicated-database: the failpoint is process-local, but CREATE TABLE is replicated DDL executed
#   on every replica server process, so the pause would not line up with the create being coordinated.
#
# Regression test: CREATE TABLE on a KeeperMap path with leftover nodes from an unfinished drop
# must not fail with a raw "Coordination error: No node .../metadata/drop_lock_version" when a
# concurrent drop removes the whole metadata subtree while the create is cleaning it up.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

KEEPER_MAP_PATH="/04647_keeper_map_metadata_race/$CLICKHOUSE_DATABASE"
ZK_ROOT="/test_keeper_map$KEEPER_MAP_PATH"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT keepermap_create_pause_before_drop_lock_version" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04647_first SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04647_second SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04647_recreated SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '$ZK_ROOT'" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

# Two tables share the path, so the last drop is the one that tears the metadata down.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04647_first (key UInt64, value UInt64) ENGINE = KeeperMap('$KEEPER_MAP_PATH') PRIMARY KEY(key)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04647_second (key UInt64, value UInt64) ENGINE = KeeperMap('$KEEPER_MAP_PATH') PRIMARY KEY(key)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_04647_first VALUES (1, 11)"

# Leave leftover nodes behind: the drop marks the path as dropped, then fails before removing it.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT keepermap_fail_drop_data"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04647_first SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04647_second SYNC"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_ROOT/metadata' AND name = 'dropped'"

# The create finds the leftover nodes and starts cleaning them up, then pauses.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT keepermap_create_pause_before_drop_lock_version"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04647_recreated (key UInt64, value UInt64) ENGINE = KeeperMap('$KEEPER_MAP_PATH') PRIMARY KEY(key)" &
CREATE_PID=$!

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT keepermap_create_pause_before_drop_lock_version PAUSE"; then
    echo "FAIL: CREATE never reached the failpoint (leftover-node cleanup branch not entered)"
    exit 1
fi

# What a concurrent drop of a sibling table on the same path does: remove the metadata subtree.
RMR_ERR=$(${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '$ZK_ROOT'" 2>&1 >/dev/null)
if [ -n "$RMR_ERR" ]; then
    echo "FAIL: could not remove the metadata subtree: $RMR_ERR"
    exit 1
fi
# keeper-client exits 0 even when the removal failed, so verify the subtree is really gone:
# without this the resumed CREATE would take the ordinary cleanup path and the test would pass
# without exercising the ZNONODE branch at all.
if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_ROOT'")" != "0" ]; then
    echo "FAIL: metadata subtree still present after rmr, the race was not set up"
    exit 1
fi

${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT keepermap_create_pause_before_drop_lock_version"

# Without the fix the create fails with KEEPER_EXCEPTION and the script exits here.
CREATE_RC=0
wait $CREATE_PID || CREATE_RC=$?
if [ "$CREATE_RC" != "0" ]; then
    echo "FAIL: CREATE TABLE failed after the concurrent metadata removal (rc=$CREATE_RC)"
    exit 1
fi

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_04647_recreated VALUES (2, 22)"
${CLICKHOUSE_CLIENT} -q "SELECT value FROM t_04647_recreated WHERE key = 2"
