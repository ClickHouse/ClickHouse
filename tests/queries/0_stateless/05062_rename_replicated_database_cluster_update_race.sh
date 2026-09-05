#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-fasttest: the fast test builds without SSL, and the interserver secret handshake
# (`Connection::sendClusterNameAndSalt` / `TCPHandler::receiveHello`) is compiled out there.
# no-parallel: uses a global pauseable failpoint; a cluster update of an unrelated `Replicated`
# database from a concurrent test could pause on it.

# `DatabaseReplicated::updateCluster` reads `database_name` and publishes a cluster object whose
# connection parameters are stamped with that name; with a `cluster_secret` the name participates
# in the interserver handshake. Historically the name was read without synchronization with
# `RENAME DATABASE`, so a rename creeping in between the read and the publication left a cached
# cluster stamped with the old name, and every query routed through it failed with
# AUTHENTICATION_FAILED.
#
# This test pins that race using the `database_replicated_pause_after_database_name_fetch` pause
# point, which sits exactly in the read-publish window:
#  1. trigger the cluster update workflow: dropping a replica writes a dummy entry to the DDL log,
#     and the first replica's worker reacts to it by force-rebuilding the cached cluster
#     (`need_update_cached_cluster` in `scheduleTasks`) and pausing at the failpoint;
#  2. launch `RENAME DATABASE` asynchronously while the worker is paused;
#  3. resume the worker and check that queries through the database cluster still work.
# Since the whole window is protected by the database mutex, the rename simply blocks until the
# worker publishes the cluster and then invalidates it (`onDatabaseRenamed`), so the last probe
# rebuilds the cluster with the new name. If the window ever loses the mutex protection, the
# rename completes during the pause and the probe fails with AUTHENTICATION_FAILED.
#
# The second replica exercises the same workflow when it is added, but the paused iteration is
# triggered by its removal, deliberately: the failpoint is global, and a freshly added replica
# processes the dummy entry of its own registration, so its worker would race with the first
# replica's worker for the pause and `SYSTEM WAIT FAILPOINT` could return before the first
# replica's worker holds the database mutex. `DETACH DATABASE` joins the second replica's worker
# thread, after which the first replica's worker is the only thread that can pause, making the
# wait and the blocked rename deterministic.

# Keep server-side warnings/errors (an authentication failure is logged at `error`) out of the
# captured client output, so the probe below reports the query outcome and nothing else. This has
# to be set before shell_config.sh, which is what turns it into `--send_logs_level`.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_RENAMED="${DB}_renamed"
DB_SECOND="${DB}_r2"
COLL="coll_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/rdb"
FAILPOINT="database_replicated_pause_after_database_name_fetch"

# Run a query through the database's own implicit cluster. Only a zero exit status counts as
# success, and a failure is reported as a single error-code token, so an unrelated failure
# cannot be mistaken for the one this test is about (it produces an empty token instead).
probe() {
    local label="$1" db="$2" out rc
    out=$($CLICKHOUSE_CLIENT --prefer_localhost_replica 0 --connections_with_failover_max_tries 1 \
        -q "SELECT count() FROM clusterAllReplicas('$db', system.one)" 2>&1)
    rc=$?
    if [ "$rc" == "0" ]; then
        echo "$label ok rows=$(printf '%s' "$out" | tr -d '[:space:]')"
    else
        echo "$label failed $(printf '%s' "$out" | grep -oE 'AUTHENTICATION_FAILED|CLUSTER_DOESNT_EXIST|ALL_CONNECTION_TRIES_FAILED|ATTEMPT_TO_READ_AFTER_EOF|NETWORK_ERROR|SOCKET_TIMEOUT' | head -1)"
    fi
}

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $COLL AS cluster_username = 'default', cluster_secret = 'secret_${CLICKHOUSE_TEST_UNIQUE_NAME}'"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's1', 'r1') SETTINGS collection_name = '$COLL'"

# This also warms the cache: the cluster object is built on first use and kept in `DatabaseReplicated::cluster`.
probe before-rename "$DB"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB_SECOND ENGINE = Replicated('$ZK_PATH', 's1', 'r2') SETTINGS collection_name = '$COLL'"
# Joins the second replica's worker thread: no thread of it is running past this point.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB_SECOND"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"

# Removing the (inactive) second replica writes a dummy entry to the DDL log; the first replica's
# worker reacts to it by force-rebuilding the cached cluster and pauses at the failpoint right
# after fetching the database name, holding the database mutex.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP DATABASE REPLICA 's1|r2' FROM DATABASE $DB"

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# The paused worker holds the database mutex, so the rename cannot finish until the failpoint is
# released; run it asynchronously.
$CLICKHOUSE_CLIENT -q "RENAME DATABASE $DB TO $DB_RENAMED" &
rename_pid=$!

# Make sure the rename has reached the server (where it blocks on the mutex) before resuming the
# worker. The pattern is anchored at the start of the query text, so this poll does not match
# itself. The poll is capped at 60 seconds so that a lost rename cannot block the test forever;
# the outcome is asserted via the reference file.
rename_observed=0
for _ in $(seq 1 600); do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query LIKE 'RENAME DATABASE $DB TO%'")" != "0" ]; then
        rename_observed=1
        break
    fi
    sleep 0.1
done
echo "rename observed=$rename_observed"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"

wait $rename_pid

# The rename must have invalidated the cluster the worker published with the old name; this probe
# rebuilds it with the new name.
probe after-rename "$DB_RENAMED"

# The second replica was already removed from ZooKeeper; attaching marks it as probably dropped,
# and the drop then only removes it locally.
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB_SECOND"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_SECOND SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_RENAMED SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"
