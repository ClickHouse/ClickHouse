#!/usr/bin/env bash
# Tags: no-async-insert
# no-async-insert: Test expects new part after connection drop

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TABLE="test_insert_on_connection_drop"
SETTINGS="input_format_connection_handling=1,min_insert_block_size_bytes=0,min_insert_block_size_rows=0"
CLICKHOUSE_INSERT_URL="${CLICKHOUSE_URL}&max_query_size=1000&query=INSERT%20INTO%20${CLICKHOUSE_TABLE}%20SETTINGS%20${SETTINGS//,/%2C}%20FORMAT%20CSV"

echo "DROP TABLE IF EXISTS ${CLICKHOUSE_TABLE}" | \
    curl -sS -d@- "$CLICKHOUSE_URL"

echo "CREATE TABLE ${CLICKHOUSE_TABLE} (id UInt64, data String, ts UInt64, value UInt64) ENGINE = MergeTree ORDER BY id" | \
    curl -sS -d@- "$CLICKHOUSE_URL"

(
i=1
while true; do
    ts=$(date +%s)
    echo "$i,hello-$i,$ts,3" || exit 0
    ((i++))
done
) | curl -sS -N --no-buffer \
    -T - \
    -X POST \
    -H "Content-Type: text/csv" \
    -H "Transfer-Encoding: chunked" \
    "$CLICKHOUSE_INSERT_URL" 2>&1 &

PIPELINE_PID=$!

sleep 15

# Temporarily redirect the shell's own stderr to suppress expected
# "Broken pipe" and "Killed" job notification messages from bash.
exec {_stderr}>&2 2>/dev/null
kill -9 $PIPELINE_PID
wait $PIPELINE_PID
exec 2>&$_stderr {_stderr}>&-


sleep 5

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, part_log;"


parts_count=$(${CLICKHOUSE_CLIENT} --query "
SELECT count(*) 
FROM system.part_log 
WHERE table = '${CLICKHOUSE_TABLE}' 
  AND event_type = 'NewPart'
  AND query_id = (
        SELECT argMax(query_id, event_time) 
        FROM system.query_log 
        WHERE query LIKE CONCAT('%INSERT INTO ', '${CLICKHOUSE_TABLE}', '%') 
          AND current_database = currentDatabase()
    )
")

echo "Number of parts created: ${parts_count}"

echo "DROP TABLE IF EXISTS ${CLICKHOUSE_TABLE}" | \
    curl -sS -d@- "$CLICKHOUSE_URL"

