#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp ${CLICKHOUSE_CONFIG_DIR}/users.xml "$CURDIR"/users.xml
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


$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "DROP USER IF EXISTS serviceuser, appuser1, appuser2, otheruser; DROP DATABASE IF EXISTS testdb03252;"
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "CREATE USER serviceuser; CREATE DATABASE testdb03252; GRANT ALL ON testdb03252.* TO serviceuser WITH GRANT OPTION;"
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" -q "CREATE USER appuser1; CREATE USER appuser2; CREATE USER otheruser; GRANT IMPERSONATE ON appuser1 TO serviceuser; GRANT IMPERSONATE ON appuser2 to serviceuser;"
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "CREATE TABLE testdb03252.test1 (id UInt64) Engine=Memory(); GRANT SELECT ON testdb03252.test1 TO appuser1; SELECT authUser(), currentUser();" --user serviceuser
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "EXECUTE AS appuser1; SELECT authUser(), currentUser(); SELECT count(*) FROM testdb03252.test1;"  --user serviceuser
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "EXECUTE AS appuser2; SELECT authUser(), currentUser(); SELECT count(*) FROM testdb03252.test1; -- {serverError ACCESS_DENIED} -- "  --user serviceuser
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "EXECUTE AS otheruser; -- {serverError ACCESS_DENIED} -- "  --user serviceuser
$CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$server_port" --multiline -q "DROP USER IF EXISTS serviceuser, appuser1, appuser2, otheruser; DROP TABLE testdb03252.test1; DROP DATABASE testdb03252;"
# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

rm -f clickhouse-server.stderr
rm -f "$CURDIR"/users.xml

exit $return_code
