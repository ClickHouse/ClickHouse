#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener and the url() table function

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# StorageURLSource::getFirstAvailableURIAndReadBuffer walks the |-separated url() options and
# returns the first one that opens. Its catch(...) used to treat a cancellation exception as an
# ordinary endpoint failure: it kept only the message text, tried every remaining option, and
# finally remapped the whole thing to NETWORK_ERROR. So a cancelled multi-option read reported a
# network error rather than the cancellation, and issued one further read per remaining option.
#
# The dead listener accepts every connection and never answers, so each attempt ends in a receive
# timeout. max_execution_time fires while attempt 1 is still reading (it is shorter than
# http_receive_timeout), which is the phase where both halves are observable.
#
# http_max_tries = 1 makes the request count a per-OPTION counter: ReadWriteBufferFromHTTP treats
# attempt 1 as the last attempt, so it rethrows immediately instead of retrying and sleeping. This
# test therefore asserts that no further OPTION is attempted; it does not bound the retries within
# one option.
# http_make_head_request = 0 pins a request count that a HEAD probe would otherwise share. It
# defaults to true and the stress runner randomizes it. Against these listeners the counts happen to
# be the same either way, because a dead listener never completes the HEAD either; the pin keeps the
# counts exact if a future fixture answers some of the options.
# parallel_replicas_for_cluster_engines = 0 keeps the read on the initiator. With parallel replicas
# url() is served by StorageURLCluster, the reads happen in secondary queries, and the initiator's
# system.query_log row never reports the counter at all.
# send_logs_level = 'fatal' suppresses the per-option tryLogCurrentException that the failover loop
# has always emitted at Error level: shell_config.sh forwards server logs at warning, so otherwise
# every skipped option writes a stack trace to the client's stderr and the runner fails the test.

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

# Accepts every connection and never writes a response: every attempt ends in a receive timeout.
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

trap 'kill $DEAD_PID $LIVE_PID 2>/dev/null ||:; wait $DEAD_PID $LIVE_PID 2>/dev/null ||:' EXIT

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

# Cancellation observed while the LAST option is in flight. Here the loop has no further option to
# skip, so only reporting the cancellation from the handler itself can keep the code: a check placed
# at the top of the failover loop is never reached again and the remap below still wins. The window
# is chosen so the cancellation lands during the second option rather than the first
# (max_execution_time exceeds one receive timeout but not two).
echo "--- cancelled while the last option is in flight ---"
QUERY_ID_LAST="04674_last_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_LAST" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS max_execution_time = 3, http_receive_timeout = 2, http_max_tries = 1,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
             send_logs_level = 'fatal'
" 2>&1 | grep -c -m1 'All uri' | sed 's/^0$/reported as a cancellation/;s/^1$/reported as an unreachable endpoint/'
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
