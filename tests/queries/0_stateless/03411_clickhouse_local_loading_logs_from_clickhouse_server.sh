#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

server_opts=(
    "--config-file=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml"
)

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY --config-file="$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml" &> clickhouse-server.stderr &
server_pid=$!

server_port=
i=0 retries=300
# wait until server will start to listen (max 30 seconds)
while [[ -z $server_port ]] && [[ $i -lt $retries ]]; do
    server_port=$(lsof -n -a -P -i tcp -s tcp:LISTEN -p $server_pid 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1) }')
    ((++i))
    sleep 0.1
    if ! kill -0 $server_pid >& /dev/null; then
        echo "No server (pid $server_pid)"
        break
    fi
done
if [[ -z $server_port ]]; then
    echo "Cannot wait for LISTEN socket" >&2
    exit 1
fi

# wait for the server to start accepting tcp connections (max 30 seconds)
i=0 retries=300
while ! $CLICKHOUSE_CLIENT_BINARY  -u default --host 127.1 --port "$server_port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
    sleep 0.1
    if ! kill -0 $server_pid >& /dev/null; then
        echo "No server (pid $server_pid)"
        break
    fi
done


if ! $CLICKHOUSE_CLIENT_BINARY  -u default --host 127.1 --port "$server_port" --format Null -q "select 1"; then
    echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT_BINARY  -u default --host 127.1 --port "$server_port" -q "
SELECT 'Hello, test';
SYSTEM FLUSH LOGS;
";

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

rm -f clickhouse-server.stderr
rm -f "$CURDIR"/users.xml

${CLICKHOUSE_LOCAL} --path "$CURDIR" --query "SELECT query FROM system.query_log WHERE query LIKE '%Hello%' /* ignore current_database */ LIMIT 1" --output-format LineAsString
