#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A logical database name must bind with the request's own settings, not only
# the server profile: via the HTTP database parameter and via the fresh context
# rebuilt for SQL security re-execution.

DB=$CLICKHOUSE_DATABASE
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"
URL_BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${DB}.ns&allow_experimental_table_namespaces=1&enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
CREATE VIEW $DB.\`ns.v\` SQL SECURITY NONE AS SELECT x FROM $DB.\`ns.t\`;
"

echo "-- the HTTP database parameter can select a namespace with per-request settings"
${CLICKHOUSE_CURL} -sS "$URL_BASE" -d "SELECT count() FROM t"
${CLICKHOUSE_CURL} -sS "$URL_BASE" -d "SELECT currentDatabase() = '$DB'"

echo "-- reading a SQL SECURITY NONE view under a scope survives the rebuilt context"
$CH -m -q "USE $DB.ns; SELECT * FROM v"

$CH -m -q "
DROP TABLE $DB.\`ns.v\`;
DROP TABLE $DB.\`ns.t\`;
"
