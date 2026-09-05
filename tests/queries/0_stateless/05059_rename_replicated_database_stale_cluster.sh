#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: the fast test builds without SSL, and the interserver secret handshake
# (`Connection::sendClusterNameAndSalt` / `TCPHandler::receiveHello`) is compiled out there.

# `DatabaseReplicated` caches the implicit cluster it builds in `getClusterImpl`, and every
# `Cluster::Address` in it is stamped with the database name as `ClusterConnectionParameters::cluster_name`.
# `RENAME DATABASE` does not invalidate that cache, so the addresses keep pointing at the old name.
#
# The stamped name is what the sender puts on the wire when the database has a `cluster_secret`
# (a named collection referenced by the `collection_name` database setting): the receiver resolves
# it with `Context::getCluster` to look the secret up. After a rename the old name no longer names
# any database, `getCluster` throws, and the receiver rejects the peer, so every query routed
# through the database's own cluster stops working.
#
# One server is enough: `prefer_localhost_replica = 0` makes the initiator go over TCP to the
# single (local) replica instead of building a local plan, which is exactly the interserver path.

# Keep server-side warnings/errors (an authentication failure is logged at `error`) out of the
# captured client output, so the probe below reports the query outcome and nothing else. This has
# to be set before shell_config.sh, which is what turns it into `--send_logs_level`.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_RENAMED="${DB}_renamed"
COLL="coll_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/rdb"

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

$CLICKHOUSE_CLIENT -q "RENAME DATABASE $DB TO $DB_RENAMED"

# Control: `system.clusters` names each `Replicated` database cluster after the catalog entry
# (`StorageSystemClusters::fillData` iterates `DatabaseCatalog`), not after the cached object, so
# it already reports the new name. The staleness lives inside the cached addresses, not here.
$CLICKHOUSE_CLIENT -q "SELECT 'clusters old=' || toString(countIf(cluster = '$DB')) || ' new=' || toString(countIf(cluster = '$DB_RENAMED')) FROM system.clusters"

probe after-rename "$DB_RENAMED"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB_RENAMED SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"
