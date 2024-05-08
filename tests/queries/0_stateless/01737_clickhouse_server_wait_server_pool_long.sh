#!/usr/bin/env bash
# Tags: long, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

server_opts=(
    "--config-file=$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml"
    "--"
    # to avoid multiple listen sockets (complexity for port discovering)
    "--listen_host=127.1"
    # we will discover the real port later.
    "--tcp_port=0"
    "--shutdown_wait_unfinished=0"
)
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" >& clickhouse-server.log &
server_pid=$!

trap cleanup EXIT
# Shellcheck wrongly process "trap" https://www.shellcheck.net/wiki/SC2317
# shellcheck disable=SC2317
function cleanup()
{
    kill -9 "$server_pid"
    kill -9 "$client_pid"

    echo "Test failed. Server log:"
    cat clickhouse-server.log
    rm -f clickhouse-server.log

    exit 1
}

server_port=
i=0 retries=300
# wait until server will start to listen (max 30 seconds)
while [[ -z $server_port ]] && [[ $i -lt $retries ]]; do
    server_port=$(lsof -n -a -P -i tcp -s tcp:LISTEN -p $server_pid 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1) }')
    ((++i))
    sleep 0.1
done
if [[ -z $server_port ]]; then
    echo "Cannot wait for LISTEN socket" >&2
    exit 1
fi

# wait for the server to start accepting tcp connections (max 30 seconds)
i=0 retries=300
while ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
    sleep 0.1
done
if ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --format Null -q 'select 1'; then
    echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
    exit 1
fi

query_id="$CLICKHOUSE_DATABASE-$SECONDS"
$CLICKHOUSE_CLIENT_BINARY --query_id "$query_id" --host 127.1 --port "$server_port" --format Null --function_sleep_max_microseconds_per_block 0 -q 'select sleepEachRow(1) from numbers(10)' 2>/dev/null &
client_pid=$!

# wait until the query will appear in processlist (max 10 second)
# (it is enough to trigger the problem)
i=0 retries=1000
while [[ $($CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" -q "select count() from system.processes where query_id = '$query_id'") != "1" ]] && [[ $i -lt $retries ]]; do
    sleep 0.01
done
if [[ $($CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" -q "select count() from system.processes where query_id = '$query_id'") != "1" ]]; then
    echo "Cannot wait until the query will start" >&2
    exit 1
fi

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

wait $client_pid

trap '' EXIT
if [ $return_code != 0 ]; then
    cat clickhouse-server.log
fi
rm -f clickhouse-server.log

exit $return_code
