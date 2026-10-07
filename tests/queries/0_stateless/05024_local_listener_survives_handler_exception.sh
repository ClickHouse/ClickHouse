#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A clickhouse-local that has started a listener must keep serving when a connection makes its handler
# fail. Sending an HTTP request to the native port is such a connection.
#
# OS-assigned ports (`--tcp_port 0 --http_port 0`) keep this parallel-safe; endpoints are built from
# the actually bound ports via `getServerPort`.

# The failure being guarded against kills the process, so the oracle is the exit status. The offending
# query itself is expected to fail: what matters is that it fails as an error, not as an abort.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 \
    --query "
    SYSTEM START LISTEN QUERIES ALL;
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('tcp_port')) || '/', LineAsString) FORMAT Null
        SETTINGS http_max_tries = 1, http_receive_timeout = 5, http_send_timeout = 5;
" >/dev/null 2>&1
rc=$?
# `clickhouse-local` exits with a ClickHouse error code, which the shell truncates to its low byte,
# so an ordinary error can land in the same numeric range as a signal status. Name the statuses that
# mean death by signal instead of testing the range.
case "$rc" in
    134) echo "died from signal 6" ;;
    139) echo "died from signal 11" ;;
    *) echo "no signal death" ;;
esac

# Exiting with an error is not enough, so keep a listener running past the offending connection and
# check that it still answers. Reading stdin from a pipe is the configuration this covers.
ports_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ports.txt"
fifo="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_stdin"
rm -f "$ports_file" "$fifo"
mkfifo "$fifo"
sleep 60 > "$fifo" &
sleep_pid=$!

$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 --interactive \
    --query "SYSTEM START LISTEN QUERIES ALL; SELECT getServerPort('tcp_port'), getServerPort('http_port') FORMAT TSV" \
    < "$fifo" > "$ports_file" 2>/dev/null &
local_pid=$!

# Closing the pipe ends the session, so the listener shuts down through the normal path. Runs on every
# exit path, including the mandatory checks below, so a failure never leaves the listener behind.
cleanup() {
    kill "$sleep_pid" 2>/dev/null
    for _ in {1..100}; do
        kill -0 "$local_pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -9 "$local_pid" 2>/dev/null
    wait "$local_pid" 2>/dev/null
    rm -f "$ports_file" "$fifo"
}
trap cleanup EXIT

for _ in {1..600}; do
    [ -s "$ports_file" ] && break
    sleep 0.1
done
read -r tcp_port http_port < "$ports_file"
# Both steps below are mandatory: skipping either leaves the final query answering from a listener that
# was never poked, which passes without testing anything.
[ -n "$tcp_port" ] && [ -n "$http_port" ] || { echo "failed to read listener ports"; exit 1; }

# `GET` and `POST` are answered as a wrong-port mistake, so the unexpected packet needs another verb.
# The server closes the connection in response, which makes writes to the socket raise SIGPIPE. Bash
# picks the descriptor, because a number chosen here can be one the shell already uses for itself.
trap '' PIPE
exec {sock}<>"/dev/tcp/127.0.0.1/${tcp_port}" || { echo "failed to connect to the native port"; exit 1; }
{ printf 'HEAD / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n' >&${sock}; } 2>/dev/null
head -c 20 <&${sock} >/dev/null 2>&1
exec {sock}<&- 2>/dev/null
exec {sock}>&- 2>/dev/null

# The listener cannot answer this if the failed connection took the process down.
${CLICKHOUSE_CURL} -sS --max-time 60 "http://127.0.0.1:${http_port}/?query=SELECT%20%27listener%20still%20serving%27"
