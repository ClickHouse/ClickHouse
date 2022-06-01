#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

server_opts=(
    "--config-file=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml"
    "--"
    # to avoid multiple listen sockets (complexity for port discovering)
    "--listen_host=127.1"
    # we will discover the real port later.
    "--tcp_port=0"
    "--shutdown_wait_unfinished=0"
)
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" >clickhouse-server.log 2>clickhouse-server.stderr &
server_pid=$!

trap cleanup EXIT
function cleanup()
{
    kill -9 $server_pid

    echo "Test failed. Server log:"
    cat clickhouse-server.log
    cat clickhouse-server.stderr
    rm -f clickhouse-server.log
    rm -f clickhouse-server.stderr

    exit 1
}

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
while ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
    sleep 0.1
    if ! kill -0 $server_pid >& /dev/null; then
        echo "No server (pid $server_pid)"
        break
    fi
done
if ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --format Null -q 'select 1'; then
    echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
    exit 1
fi

# it is not mandatory to use existing table since it fails earlier, hence just a placeholder.
# this is format of INSERT SELECT, that pass these settings exactly for INSERT query not the SELECT
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiquery < $CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).sql1

# check that server is still alive
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --format Null -q 'SELECT 1'

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

trap '' EXIT
if [ $return_code != 0 ]; then
    cat clickhouse-server.log
    cat clickhouse-server.stderr
fi
rm -f clickhouse-server.log
rm -f clickhouse-server.stderr

exit $return_code
