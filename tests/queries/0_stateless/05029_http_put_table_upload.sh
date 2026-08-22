#!/usr/bin/env bash

set -euo pipefail

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="put_table_05029"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"

echo "===== PUT table upload ====="
echo "-- CSV format from the path"
printf '1,"one"\n2,"two"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- JSONEachRow format from the path"
printf '{"a":3,"b":"three"}\n' \
    | curl -sS -X PUT -H 'Content-Type: application/json' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.JSONEachRow"

echo "-- inserted rows"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- a PUT path without a format suffix is not claimed"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --data-binary 'SELECT 1' "${BASE_URL}/${DB}/${TABLE}"

echo "-- a missing table is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/missing_table_05029.CSV" 2>&1 \
    | grep -oE "which does not exist"

echo "-- an unknown path format is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.UnknownFormat" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a known format"

echo "-- compressed path suffixes are rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV.gz" 2>&1 \
    | grep -oE "HTTP PUT table uploads do not support compression suffixes"

echo "-- an empty body is rejected"
curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary '' "${BASE_URL}/${DB}/${TABLE}.CSV" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a non-empty request body"

echo "-- an explicit INSERT query remains read-only on PUT"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?query=INSERT%20INTO%20${TABLE}%20FORMAT%20CSV" 2>&1 \
    | grep -oE "Cannot execute query in readonly mode"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.${TABLE}"
