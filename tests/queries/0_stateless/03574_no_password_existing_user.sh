#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
#
# Starts the server twice -
# 1st : Start with allow_no_password=1 and create a user with no_password and login as that user will also succeed.
# 2nd : Restart the server with allow_no_password=0. Now login as that user should error and not crash Clickhouse server.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp ${CLICKHOUSE_CONFIG_DIR}/users.xml "$CURDIR"/users.xml
sed -i 's/<password><\/password>/<password_sha256_hex>c64c5e4e53ea1a9f1427d2713b3a22bbebe8940bc807adaf654744b1568c70ab<\/password_sha256_hex>/g' "$CURDIR"/users.xml
sed -i 's/<!-- <access_management>1<\/access_management> -->/<access_management>1<\/access_management>/g' "$CURDIR"/users.xml

# First start the server with allow_no_password=1 
server_opts=(
    "--config-file=$CURDIR/03574_no_password_existing_user.config.xml.1"
    "--"
    # to avoid multiple listen sockets (complexity for port discovering)
    "--listen_host=127.1"
    # we will discover the real port later.
    "--tcp_port=0"
    "--shutdown_wait_unfinished=0"
)

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" &> clickhouse-server.stderr &
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
while ! $CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
    sleep 0.1
    if ! kill -0 $server_pid >& /dev/null; then
        echo "No server (pid $server_pid)"
        break
    fi
done


if ! $CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" --format Null -q 'select 1'; then
    echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "DROP USER IF EXISTS u_3574;"
$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER u_3574 IDENTIFIED WITH no_password;"
# This will work.
$CLICKHOUSE_CLIENT_BINARY  -u u_3574  --password "" --host 127.1 --port "$server_port" -q "SELECT user();"

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid

# start the server again with allow_no_password=0
server_opts=(
    "--config-file=$CURDIR/03574_no_password_existing_user.config.xml.2"
    "--"
    # to avoid multiple listen sockets (complexity for port discovering)
    "--listen_host=127.1"
    # we will discover the real port later.
    "--tcp_port=0"
    "--shutdown_wait_unfinished=0"
)

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" &> clickhouse-server.stderr &
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
while ! $CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
    sleep 0.1
    if ! kill -0 $server_pid >& /dev/null; then
        echo "No server (pid $server_pid)"
        break
    fi
done


if ! $CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" --format Null -q 'select 1'; then
    echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
    exit 1
fi

# This should fail.
$CLICKHOUSE_CLIENT_BINARY  -u u_3574  --password "" --host 127.1 --port "$server_port" -q "SELECT user(), ' should not appear '; -- { serverError 516 } --" &> /dev/null ;

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

rm -f clickhouse-server.stderr
rm -f "$CURDIR"/users.xml

exit $return_code
