#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp ${CLICKHOUSE_CONFIG_DIR}/users.xml "$CURDIR"/users.xml
sed -i 's/<password><\/password>/<password_sha256_hex>c64c5e4e53ea1a9f1427d2713b3a22bbebe8940bc807adaf654744b1568c70ab<\/password_sha256_hex>/g' "$CURDIR"/users.xml
 sed -i 's/<!-- <access_management>1<\/access_management> -->/<access_management>1<\/access_management>/g' "$CURDIR"/users.xml

server_opts=(
    "--config-file=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml"
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

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q " DROP USER IF EXISTS u_02207, u1_02207";

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER u_02207 IDENTIFIED WITH double_sha1_hash BY '8DCDD69CE7D121DE8013062AEAEB2A148910D50E'
"

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q " CREATE USER u1_02207 IDENTIFIED BY 'qwe123'";

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER u2_02207 HOST IP '127.1' IDENTIFIED WITH plaintext_password BY 'qwerty' " " -- { serverError 516 } --" &> /dev/null ;

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER u3_02207 HOST IP '127.1' IDENTIFIED WITH no_password " " -- { serverError 516 } --" &> /dev/null ;

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER u4_02207 HOST IP '127.1' NOT IDENTIFIED " " -- { serverError 516 } --" &> /dev/null ;

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q "CREATE USER IF NOT EXISTS  u5_02207 " " -- { serverError 516 } --" &> /dev/null ;

$CLICKHOUSE_CLIENT_BINARY  -u default --password='1w2swhb1' --host 127.1 --port "$server_port" -q " DROP USER u_02207, u1_02207";


# no sleep, since flushing to stderr should not be buffered.
 grep  'User is not allowed to Create users' clickhouse-server.stderr


# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

rm -f clickhouse-server.stderr
rm -f "$CURDIR"/users.xml

exit $return_code
