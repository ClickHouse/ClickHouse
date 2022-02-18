#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-parallel, no-fasttest
# Tag no-tsan: requires jemalloc to track small allocations
# Tag no-asan: requires jemalloc to track small allocations
# Tag no-ubsan: requires jemalloc to track small allocations
# Tag no-msan: requires jemalloc to track small allocations

#
# Regression for INSERT SELECT, that abnormally terminates the server
# in case of too small memory limits.
#
# NOTE: After #24483 had been merged the only place where the allocation may
# fail is the insert into PODArray in DB::OwnSplitChannel::log, but after
# #24069 those errors will be ignored, so to check new behaviour separate
# server is required.
#

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp "$CURDIR"/../../../programs/server/users.xml "$CURDIR"/users.xml
 sed -i 's/<password><\/password>/<no_password\/>/g' "$CURDIR"/users.xml
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

$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port"   -q "CREATE USER u_02207 HOST IP '127.1' IDENTIFIED WITH plaintext_password BY 'qwerty' " " -- { serverError 43 } --" &> /dev/null ;

# no sleep, since flushing to stderr should not be buffered.
 grep  'User is not allowed to Create users with type PLAINTEXT_PASSWORD' clickhouse-server.stderr


# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

rm -f clickhouse-server.stderr
rm -f "$CURDIR"/users.xml

exit $return_code
