#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest
# no-parallel: enables a server-global failpoint
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

RDB="rdb_$CLICKHOUSE_DATABASE"

# The failpoint is server-global and the Keeper path is fixed, so always clean up on
# every exit path: disable the failpoint (otherwise a mid-test failure leaves it enabled
# and poisons unrelated tests) and drop the database SYNC (so a retry does not race the
# previous replicated-database drop on the same Keeper path).
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_database_status_finished_node_missing" 2>/dev/null
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
