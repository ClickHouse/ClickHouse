#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The current database keeps the full name selected by USE ("db.ns", "db.ns.sub") for logs,
# cache keys, while currentDatabase() reports the physical database.
# A quoted dotted database name is a plain database and is never split.

DB=$CLICKHOUSE_DATABASE
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"
SESSION="${CLICKHOUSE_DATABASE}_full_name"
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?session_id=${SESSION}&allow_experimental_table_namespaces=1&enable_analyzer=1"

in_session() { ${CLICKHOUSE_CURL} -sS "$URL" -d "$1"; }

$CH -m -q "
CREATE TABLE $DB.t (x Int32) ENGINE = Memory;
INSERT INTO $DB.t VALUES (1), (2);
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
CREATE TABLE $DB.\`ns.sub.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.sub.t\` VALUES (1), (2), (3);
"

echo "-- query_log keeps the full name, currentDatabase() the physical database"
in_session "USE $DB.ns"
in_session "SELECT count() FROM t SETTINGS log_comment = 'full_name_ns_$DB'"
in_session "SELECT currentDatabase() = '$DB'"

echo "-- a deeper path splits at the first dot only"
in_session "USE $DB.ns.sub"
in_session "SELECT count() FROM t SETTINGS log_comment = 'full_name_sub_$DB'"
in_session "SELECT currentDatabase() = '$DB'"

# retry: the query_log entry for a curl-issued request is written after the HTTP response is sent
for _ in {1..20}; do
    $CH -q "SYSTEM FLUSH LOGS query_log"
    result=$($CH -q "SELECT log_comment, current_database FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND log_comment IN ('full_name_ns_$CLICKHOUSE_DATABASE', 'full_name_sub_$CLICKHOUSE_DATABASE') ORDER BY log_comment FORMAT TSV")
    [ "$(echo "$result" | grep -c .)" -eq 2 ] && break
    sleep 0.5
done
echo "$result" | sed "s/$CLICKHOUSE_DATABASE/DB/g"

echo "-- the query result cache keys the database and the namespace separately"
in_session "USE $DB.ns"
in_session "SELECT count() FROM t SETTINGS use_query_cache = 1"
in_session "USE $DB"
in_session "SELECT count() FROM t SETTINGS use_query_cache = 1"

echo "-- a quoted dotted database name is not split"
$CLICKHOUSE_CLIENT -m -q "
CREATE DATABASE \`$DB.plain\`;
CREATE TABLE \`$DB.plain\`.t (x Int32) ENGINE = Memory;
SHOW TABLES FROM \`$DB.plain\`;
DROP DATABASE \`$DB.plain\`;
"

$CH -m -q "
DROP TABLE $DB.\`ns.sub.t\`;
DROP TABLE $DB.\`ns.t\`;
DROP TABLE $DB.t;
"
