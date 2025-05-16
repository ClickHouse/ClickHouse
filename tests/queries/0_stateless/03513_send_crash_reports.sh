#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting clickhouse-server"

CLICKHOUSE_TMP_PORT_TCP=50111
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_BINARY server -- --tcp_port "${CLICKHOUSE_TMP_PORT_TCP}" --path "${CLICKHOUSE_TMP}/" --send_crash_reports.endpoint "${CLICKHOUSE_URL}&query=INSERT+INTO+crash+FORMAT+JSONAsObject" > server.log 2>&1 &
PID=$!

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_BINARY client --port ${CLICKHOUSE_TMP_PORT_TCP} --query "SELECT 'Server started'" 2>/dev/null && break
    if [[ $i == 30 ]]; then
        cat server.log
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS crash; CREATE TABLE crash (data JSON) ORDER BY ()"

# Initiate a crash
echo "Sending a signal"
kill -11 $PID
wait 2>/dev/null

$CLICKHOUSE_CLIENT --query "SELECT * FROM crash; DROP TABLE crash;"
