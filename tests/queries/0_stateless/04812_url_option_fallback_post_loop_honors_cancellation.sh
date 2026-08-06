#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs a local HTTP listener and the url table function
# Tag no-parallel: a PAUSEABLE failpoint is a global channel, so concurrent instances would
# interfere with each other's ENABLE/WAIT/DISABLE sequence

# After the failover loop has tried every option, control reaches the post-loop code through the
# exception handler only. The handler checks the query status, but a cancellation observed between
# that check and the post-loop exits was still reported as an empty file or as the aggregate
# NETWORK_ERROR. The window is a few tens of microseconds wide on real code, so it is pinned with
# a PAUSEABLE failpoint rather than by racing a KILL against it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=url_failover_before_returning_last_option_pause

DEAD_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")
EMPTY_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# Accepts and closes without answering, so an attempt here fails at once and costs no timeout.
python3 -c "
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $DEAD_PORT))
srv.listen(128)
while True:
    conn, _ = srv.accept()
    try:
        conn.recv(65536)
    except OSError:
        pass
    conn.close()
" &
DEAD_PID=$!

# Answers an empty body, so this option is skipped for being empty and kept as the fallback the
# post-loop code would otherwise return.
python3 -c "
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $EMPTY_PORT))
srv.listen(128)
head = b'HTTP/1.1 200 OK\r\nContent-Type: text/tab-separated-values\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'
while True:
    conn, _ = srv.accept()
    try:
        conn.recv(65536)
        conn.sendall(head)
    except OSError:
        pass
    conn.close()
" &
EMPTY_PID=$!

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT '"$FP"'" 2>/dev/null ||:; kill $DEAD_PID $EMPTY_PID 2>/dev/null ||:; wait $DEAD_PID $EMPTY_PID 2>/dev/null ||:' EXIT

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
    echo "Timeout waiting for port $1 to open"
    exit 1
}
wait_for_port "$DEAD_PORT"
wait_for_port "$EMPTY_PORT"

# What the finished query reported, and how many HTTP requests it sent.
outcome()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'code ' || multiIf(exception_code = 159, 'TIMEOUT_EXCEEDED',
                                  exception_code = 394, 'QUERY_WAS_CANCELLED',
                                  exception_code = 210, 'NETWORK_ERROR',
                                  exception_code = 636, 'CANNOT_EXTRACT_TABLE_STRUCTURE',
                                  exception_code = 0, 'SUCCESS',
                                  toString(exception_code))
               || ', options attempted: ' || toString(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'])
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

# Runs one query paused at the post-loop failpoint, kills it while it is parked there, and reports
# what it ended up with. Because the pause is inside the window, the kill lands there by
# construction: no polling, no sleeping and no receive timeout is spent waiting for the race.
paused_then_killed()
{
    local qid="$1" args="$2" extra="$3"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT $FP"
    ${CLICKHOUSE_CLIENT} --query_id "$qid" --query "
        SELECT * FROM url($args)
        SETTINGS $extra, max_execution_time = 0, http_receive_timeout = 2, http_max_tries = 1,
                 http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0,
                 send_logs_level = 'fatal'
    " > /dev/null 2>&1 &
    local pid=$!
    ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$qid' FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $pid 2>/dev/null ||:
    outcome "$qid"
}

# The empty option is kept as the fallback and every other option failed, so without the check the
# post-loop code hands that empty buffer back and schema inference reports it as an empty file.
echo "--- cancelled inside the post-loop window, empty fallback kept ---"
paused_then_killed "04812_empty_${CLICKHOUSE_DATABASE}" \
    "'http://127.0.0.1:$EMPTY_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV'" \
    "engine_url_skip_empty_files = 1"

# No option was empty, so the post-loop code throws the aggregate NETWORK_ERROR instead. This is
# the same window reported against the contract this test file exists for.
echo "--- cancelled inside the post-loop window, no empty fallback ---"
paused_then_killed "04812_agg_${CLICKHOUSE_DATABASE}" \
    "'http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV'" \
    "engine_url_skip_empty_files = 0"

# Same window on the read path, which reaches the post-loop code through the same handler.
echo "--- cancelled inside the post-loop window, read path ---"
paused_then_killed "04812_read_${CLICKHOUSE_DATABASE}" \
    "'http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String'" \
    "engine_url_skip_empty_files = 0"

# Must not regress: with the failpoint armed but the query never killed, the post-loop code still
# reports what it always did. These are the rows that a check firing unconditionally would break.
echo "--- not cancelled, empty fallback kept ---"
QID_OKEMPTY="04812_okempty_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT $FP"
${CLICKHOUSE_CLIENT} --query_id "$QID_OKEMPTY" --query "
    SELECT * FROM url('http://127.0.0.1:$EMPTY_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV')
    SETTINGS engine_url_skip_empty_files = 1, max_execution_time = 0, http_receive_timeout = 2,
             http_max_tries = 1, http_make_head_request = 0,
             parallel_replicas_for_cluster_engines = 0, send_logs_level = 'fatal'
" > /dev/null 2>&1 &
OK_PID=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $FP"
wait $OK_PID 2>/dev/null ||:
outcome "$QID_OKEMPTY"

echo "--- not cancelled, no empty fallback ---"
QID_OKAGG="04812_okagg_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT $FP"
${CLICKHOUSE_CLIENT} --query_id "$QID_OKAGG" --query "
    SELECT * FROM url('http://127.0.0.1:$DEAD_PORT/a|http://127.0.0.1:$DEAD_PORT/b', 'TSV', 's String')
    SETTINGS engine_url_skip_empty_files = 0, max_execution_time = 0, http_receive_timeout = 2,
             http_max_tries = 1, http_make_head_request = 0,
             parallel_replicas_for_cluster_engines = 0, send_logs_level = 'fatal'
" > /dev/null 2>&1 &
OK_PID=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $FP"
wait $OK_PID 2>/dev/null ||:
outcome "$QID_OKAGG"
