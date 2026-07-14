#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest
# no-parallel: enables server-global failpoints
# no-fasttest: needs ZooKeeper for a Replicated database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test for a debug/sanitizer-only false positive in
# ReplicatedDatabaseQueryStatusSource::checkStatus. For a Replicated database the
# finished/<host_id> status node is always written with status 0, and on error the
# executing replica retries without persisting any error status. So an absent
# finished/<host_id> node (cleaned up, or the retry race) is benign, but the debug-only
# checkStatus read used to return the getExecutionStatus sentinel (code -1) for it,
# which generate() then reported as a non-zero remote error and threw a LOGICAL_ERROR
# ("There was an error on ...: Cannot obtain error message (probably it's a bug)"),
# aborting the server in debug/sanitizer builds.
# The failpoint below forces that "finished node missing" read deterministically.
#
# The second part asserts the LIST_WITH_STAT_AND_DATA atomic path is actually exercised.
# checkStatus reads finished/<host_id> either from the atomic list-with-data snapshot
# (finished_node_data) or, when that is unavailable, from a per-host getExecutionStatus.
# The second failpoint makes that per-host fallback throw, so a normal finished/ DDL can
# only succeed if it took the atomic snapshot path; if that path regresses off (e.g.
# wantsFinishedNodeData reverts to false or the feature gate keeps with_data off), the
# fallback fires and the DDL fails, turning this test red.

RDB="rdb_$CLICKHOUSE_DATABASE"

# Both failpoints are server-global and the Keeper path is fixed, so always clean up on
# every exit path: disable them (otherwise a mid-test failure leaves one enabled and
# poisons unrelated tests) and drop the database SYNC (so a retry does not race the
# previous replicated-database drop on the same Keeper path).
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_database_status_finished_node_missing" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_database_status_finished_node_fallback_get_fault" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $RDB SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $RDB SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $RDB ENGINE = Replicated('/clickhouse/databases/$RDB', '{shard}', '{replica}')"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT replicated_database_status_finished_node_missing"

# Without the fix this DDL aborts the server in debug/sanitizer builds because the
# forced-missing finished node is misread as a non-zero remote error.
# distributed_ddl_output_mode=none still exercises the throwing generate()/handleNonZeroStatusCode
# path (only NEVER_THROW skips it) while suppressing per-host status rows for stable output.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE $RDB.t (a UInt64) ENGINE = MergeTree ORDER BY a"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_database_status_finished_node_missing"

# Server must still be alive and the DDL must have succeeded.
$CLICKHOUSE_CLIENT -q "SELECT 'alive', count() FROM $RDB.t"

# The atomic finished_node_data path (list-with-data) is only taken when the connected
# Keeper advertises LIST_WITH_STAT_AND_DATA (a plain ZooKeeper does not), so only assert
# it there; against a Keeper without the flag the fallback is the correct path.
HAS_ATOMIC=$($CLICKHOUSE_CLIENT -q "SELECT countIf(has(enabled_feature_flags, 'LIST_WITH_STAT_AND_DATA')) > 0 FROM system.zookeeper_connection WHERE name = 'default'")

# Make the per-host getExecutionStatus fallback throw. A normal (async, finished/) DDL
# must then still succeed via the atomic snapshot; if the atomic path is not taken it hits
# the fallback and fails, which is exactly the regression we want to catch.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT replicated_database_status_finished_node_fallback_get_fault"

if [ "$HAS_ATOMIC" = "1" ]; then
    # Atomic path proof: this must succeed without touching the faulty fallback.
    $CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE $RDB.t_atomic (a UInt64) ENGINE = MergeTree ORDER BY a"
    $CLICKHOUSE_CLIENT -q "SELECT 'atomic_ok', count() FROM $RDB.t_atomic"

    # Positive control: synchronous settings wait on synced/ (empty payload), so the atomic
    # finished/ cache is intentionally disabled and checkStatus takes the fallback, which the
    # failpoint makes throw. This proves the failpoint is live and the fallback is used there.
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_enforce_synchronous_settings=1 \
        -q "CREATE TABLE $RDB.t_sync (a UInt64) ENGINE = MergeTree ORDER BY a" 2>&1 | grep -q -F "FAULT_INJECTED" && echo 1 || echo 0
else
    # No atomic path available (plain ZooKeeper): the fallback is the correct path and the
    # failpoint makes it throw. Emit the same lines as the atomic case for a stable reference.
    echo "atomic_ok	0"
    echo "1"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_database_status_finished_node_fallback_get_fault"
