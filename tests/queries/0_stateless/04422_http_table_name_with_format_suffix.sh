#!/usr/bin/env bash

# Tags: no-replicated-database

# Regression: a table whose name ends in a registered format (or compression) token — e.g.
# `events.JSON` — must be reachable through the HTTP "table as file" path. The unquoted last path
# component still splits off a trailing `.format[.compression]` suffix (so `/db/events.CSV` reads table
# `events` rendered as CSV), but a back-quoted component is a literal table name (its dots are part of
# the name and no suffix is stripped), mirroring SQL identifier quoting. A backtick travels in a URL
# percent-encoded as `%60`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.events"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`${DB}\`.\`events.CSV\`"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`${DB}\`.\`events.JSON\`"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.events (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.events VALUES (111)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`${DB}\`.\`events.CSV\` (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`${DB}\`.\`events.CSV\` VALUES (222)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`${DB}\`.\`events.JSON\` (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`${DB}\`.\`events.JSON\` VALUES (333)"

http_get() { curl -sS "$@"; }

echo "-- /db/events.CSV (unquoted): table 'events' rendered as CSV (111)"
http_get "${BASE_URL}/${DB}/events.CSV"
echo "-- /db/%60events.CSV%60 (back-quoted): the literal table 'events.CSV' (222, default TSV)"
http_get "${BASE_URL}/${DB}/%60events.CSV%60"
echo "-- /db/%60events.JSON%60 (back-quoted): the literal table 'events.JSON' is reachable (333, default TSV)"
http_get "${BASE_URL}/${DB}/%60events.JSON%60"
echo "-- /db/%60events.JSON%60?format=JSONEachRow: format chosen via URL parameter, not the path"
http_get "${BASE_URL}/${DB}/%60events.JSON%60?format=JSONEachRow"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.events"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`${DB}\`.\`events.CSV\`"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`${DB}\`.\`events.JSON\`"
