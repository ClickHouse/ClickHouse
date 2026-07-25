#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The meaning of USE db.ns is decided once at USE time: creating or dropping a
# database literally named "db.ns" afterwards must not flip an active session.

DB=$CLICKHOUSE_DATABASE
COLLIDER="$DB.ns"
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"
# a bare URL, the database parameter would override the session scope on every request
SESSION="${CLICKHOUSE_DATABASE}_frozen_scope"
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?session_id=${SESSION}&allow_experimental_table_namespaces=1&enable_analyzer=1"

in_session() { ${CLICKHOUSE_CURL} -sS "$URL" -d "$1"; }

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
"

echo "-- scope entered, physical database reported"
in_session "USE $DB.ns"
in_session "SELECT currentDatabase() = '$DB', currentSchemas(true) = ['$DB']"
in_session "SELECT count() FROM t"

echo "-- creating a database named db.ns must not retarget the active scope"
$CH -q "CREATE DATABASE \`$COLLIDER\`"
$CH -q "CREATE TABLE \`$COLLIDER\`.t (x Int32) ENGINE = Memory"
in_session "SELECT count() FROM t"
in_session "SELECT currentDatabase() = '$DB'"
in_session "SHOW TABLES"

echo "-- the next USE re-evaluates the name, now the exact database wins"
in_session "USE $DB.ns"
in_session "SELECT currentDatabase() = '$COLLIDER'"
in_session "SELECT count() FROM t"

echo "-- dropping the database must not silently turn the session into a scope"
$CH -q "DROP DATABASE \`$COLLIDER\`"
in_session "SELECT count() FROM t" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

$CH -q "DROP TABLE $DB.\`ns.t\`"
