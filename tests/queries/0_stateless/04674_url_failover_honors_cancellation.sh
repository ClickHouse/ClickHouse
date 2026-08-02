#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener and the url table function

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A cancelled multi-option url read must report the cancellation itself instead of remapping it to
# NETWORK_ERROR, and must not attempt the remaining options. The dead listener accepts every
# connection and never answers, so every attempt there ends in a receive timeout.

DEAD_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")
LIVE_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

STDERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"

# Accepts every connection and never writes a response, so every attempt there ends in a timeout.
python3 -c "
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $DEAD_PORT))
srv.listen(128)
accepted = []
while True:
    conn, _ = srv.accept()
    accepted.append(conn)  # hold the connection open and never answer
" &
DEAD_PID=$!

# Answers a single TSV row, so failover past a dead option can be observed returning data.
python3 -c "
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $LIVE_PORT))
srv.listen(64)
body = b'failover_reached_second_option\n'
head = (b'HTTP/1.1 200 OK\r\nContent-Type: text/tab-separated-values\r\n'
        b'Content-Length: ' + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n')
while True:
    conn, _ = srv.accept()
    try:
        conn.recv(65536)
        conn.sendall(head + body)
    except OSError:
        pass
    conn.close()
" &
LIVE_PID=$!

trap 'kill $DEAD_PID $LIVE_PID 2>/dev/null ||:; wait $DEAD_PID $LIVE_PID 2>/dev/null ||:; rm -f "$STDERR"' EXIT

wait_for_port()
{
    for _ in $(seq 1 100); do
        python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(1)
try:
    s.connect(('127.0.0.1', $1))
except OSError:
    sys.exit(1)
s.close()
" && return 0
        sleep 0.1
    done
    return 1
}
wait_for_port "$DEAD_PORT"
wait_for_port "$LIVE_PORT"

# Counts the HTTP requests the still-running query itself has sent. The counter belongs to the
# query's own thread group, so readiness probes, earlier groups and parallel copies cannot contribute.
# A non-numeric reply only means "not observed yet", so it must not reach the arithmetic test below.
wait_for_requests()
{
    for _ in $(seq 1 600); do
        SENT=$(${CLICKHOUSE_CLIENT} --query "
            SELECT sum(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'])
            FROM system.processes WHERE query_id = '$1'" 2>/dev/null)
        case "$SENT" in
            '' | *[!0-9]* ) ;;
            * ) [ "$SENT" -ge "$2" ] && return 0 ;;
        esac
        sleep 0.1
    done
    echo "Timeout waiting for $2 HTTP requests from query $1"
    exit 1
}

kill_and_wait()
{
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$1'" > /dev/null
    for _ in $(seq 1 600); do
        [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" = "0" ] && return 0
        sleep 0.1
    done
    echo "Timeout waiting for query $1 to be cancelled"
    exit 1
}

# How many HTTP requests the finished query actually sent, and what it reported.
outcome()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'code ' || multiIf(exception_code = 159, 'TIMEOUT_EXCEEDED',
                                  exception_code = 394, 'QUERY_WAS_CANCELLED',
                                  exception_code = 210, 'NETWORK_ERROR',
                                  toString(exception_code))
               || ', options attempted: ' || toString(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'])
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

# Every SETTINGS pin below is measured load-bearing: http_max_tries turns the request count into a
# per-option counter, send_logs_level suppresses the failover loop's own Error logging, and
# parallel_replicas_for_cluster_engines keeps the read on the initiator. http_make_head_request is a guard.

# A cancelled two-option read must report the cancellation, not NETWORK_ERROR, and must not attempt
# the second option. Without the fix: NETWORK_ERROR with both options attempted.
echo "--- cancelled, two options ---"
QUERY_ID_TWO="04674_two_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_TWO" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 2, http_receive_timeout = 5, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" 2>&1 | grep -c -m1 'All uri' | sed 's/^0$/reported as a cancellation/;s/^1$/reported as an unreachable endpoint/'
outcome "$QUERY_ID_TWO"

# Same with three options: the count must not grow with the number of remaining options.
echo "--- cancelled, three options ---"
QUERY_ID_THREE="04674_three_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_THREE" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b|http://127.0.0.1:$DEAD_PORT/c', 'TSV', 's String')
    SETTINGS max_execution_time = 2, http_receive_timeout = 5, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" 2>&1 | grep -c -m1 'All uri' | sed 's/^0$/reported as a cancellation/;s/^1$/reported as an unreachable endpoint/'
outcome "$QUERY_ID_THREE"

# The last option has no successor, so only reporting the cancellation from the handler itself can
# keep the code: a check at the top of the failover loop is never reached again. The kill is fired
# once the query's own request counter reaches 2, so option 2 is in flight by observation, not by timing.
echo "--- KILL QUERY while the last option is in flight ---"
QUERY_ID_LAST="04674_last_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_LAST" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 0, http_receive_timeout = 10, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" > "$STDERR" 2>&1 &
CLIENT_PID=$!
wait_for_requests "$QUERY_ID_LAST" 2
kill_and_wait "$QUERY_ID_LAST"
wait "$CLIENT_PID" ||:
grep -c -m1 'All uri' "$STDERR" | sed 's/^0$/reported as a cancellation/;s/^1$/reported as an unreachable endpoint/'
outcome "$QUERY_ID_LAST"

# Must not regress: with no cancellation, all options genuinely down still reports the aggregate
# NETWORK_ERROR naming the option count, and still attempts every option.
echo "--- not cancelled, all options down ---"
QUERY_ID_DOWN="04674_down_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_DOWN" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 0, http_receive_timeout = 2, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" 2>&1 | grep -o -m1 'All uri (2) options are unreachable'
outcome "$QUERY_ID_DOWN"

# Must not regress: a dead first option followed by a working one still fails over and returns the
# data. This is the row that an error-code allow-list rethrow would have broken.
echo "--- not cancelled, failover to a working option ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$LIVE_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 0, http_receive_timeout = 2, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'"

# Must not regress: timeout_overflow_mode = 'break' does not mark the query killed, so the failover
# loop is not interrupted and the aggregate NETWORK_ERROR is still reported.
echo "--- timeout_overflow_mode = break ---"
QUERY_ID_BREAK="04674_break_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_BREAK" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 2, timeout_overflow_mode = 'break', http_receive_timeout = 5,
             http_max_tries = 1, http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" 2>&1 | grep -o -m1 'All uri (2) options are unreachable'
outcome "$QUERY_ID_BREAK"
