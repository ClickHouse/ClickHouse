#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_PORT_TCP=50111
CRASH_RECEIVER_PORT=50112
CLICKHOUSE_DATABASE=default

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting a crash receiver"
echo -ne "HTTP/1.0 200 Ok\r\n\r\n" | nc -l -p "${CRASH_RECEIVER_PORT}" | grep --max-count 1 -o -F '"signal_number":11' && exit &

echo "Starting clickhouse-server"

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_BINARY server -- --tcp_port "${CLICKHOUSE_PORT_TCP}" --path "${CLICKHOUSE_TMP}/" --send_crash_reports.endpoint "http://localhost:${CRASH_RECEIVER_PORT}/" > server.log 2>&1 &
PID=$!

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 'Server started'" 2>/dev/null && break
    if [[ $i == 30 ]]; then
        cat server.log
        exit 1
    fi
done

# Initiate a crash
echo "Sending a signal"
kill -11 $PID
wait 2>/dev/null
