#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts a second server process, which the fast test image does not carry.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A daemon runs the crash-report half of the crash handler, which clickhouse-local never reaches
# because it has no daemon, so covering it needs a daemon parked before its global context exists.
#
# Keeper reads its persistent UUID from <path>/uuid while the global context is still absent, and
# reading a FIFO blocks until a writer appears, so a FIFO there parks the process in that window
# with the signal listener already running.
#
# The crash report is only built when an endpoint is configured, and the writer connects before it
# asks for the server UUID, so the endpoint has to accept the connection or the report is abandoned
# before the part under test runs.
#
# Unique names and ports keep this parallel-safe.
work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05043"
err="$work/stderr.txt"
log="$work/keeper.log"
rm -rf "$work"
mkdir -p "$work/data"

# Ports derived from the database name, which is unique per test run.
port_hash=$(( $(echo "$CLICKHOUSE_DATABASE" | cksum | cut -d' ' -f1) % 20000 ))
keeper_port=$(( 25000 + port_hash ))
raft_port=$(( 45000 + port_hash ))
sink_port=$(( 25001 + port_hash ))

cleanup() {
    [ -n "${keeper_pid:-}" ] && kill -9 "$keeper_pid" 2>/dev/null
    [ -n "${sink_pid:-}" ] && kill -9 "$sink_pid" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

# Accepts the report and answers it. Answering matters for the run time as much as accepting: the
# writer waits for a response before it finishes, so a listener that only drains costs a read
# timeout, about a minute, on every run.
python3 -c "
import socket, threading
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $sink_port))
srv.listen(4)
def serve(c):
    try:
        c.settimeout(10)
        while b'\r\n\r\n' not in c.recv(65536):
            pass
        c.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
    except OSError:
        pass
    finally:
        c.close()
while True:
    c, _ = srv.accept()
    threading.Thread(target=serve, args=(c,), daemon=True).start()
" >/dev/null 2>&1 &
sink_pid=$!

for _ in {1..50}; do
    (exec 3<>"/dev/tcp/127.0.0.1/$sink_port") 2>/dev/null && break
    sleep 0.1
done

cat > "$work/config.xml" <<EOF
<clickhouse>
    <logger><level>trace</level><log>$log</log><console>0</console></logger>
    <send_crash_reports>
        <enabled>true</enabled>
        <endpoint>http://127.0.0.1:$sink_port/</endpoint>
    </send_crash_reports>
    <keeper_server>
        <tcp_port>$keeper_port</tcp_port>
        <server_id>1</server_id>
        <path>$work/data</path>
        <log_storage_path>$work/data/log</log_storage_path>
        <snapshot_storage_path>$work/data/snapshots</snapshot_storage_path>
        <coordination_settings><session_timeout_ms>30000</session_timeout_ms></coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>$raft_port</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF

# Nothing writes to the FIFO, so reading the UUID parks here.
mkfifo "$work/data/uuid"

# The shell reports an abnormal exit of a background job on its own stderr, naming a signal the
# process is expected to take here, and the manner of the exit is not what this test reads. A
# subshell keeps the job, and that report, out of this one.
( $CLICKHOUSE_BINARY keeper --config-file="$work/config.xml" >/dev/null 2>"$err" & echo $! > "$work/pid" ) 2>/dev/null
keeper_pid=$(cat "$work/pid")

# Both checks are mandatory. Signalling before the listener is running would use the default
# disposition, and signalling after the global context exists would exercise the state that already
# worked, so either way nothing under test would run and the test would pass without measuring.
#
# `wait_for_partner` is the kernel state of a process blocked opening a FIFO. It needs symbol names
# in /proc, so a log line the process writes on the way there is the fallback.
parked=0
announced=0
for i in {1..600}; do
    kill -0 "$keeper_pid" 2>/dev/null || break
    grep -q 'Initializing DateLUT' "$log" 2>/dev/null && { parked=0; break; }
    grep -q 'Starting ClickHouse Keeper' "$log" 2>/dev/null && announced=1
    if [ "$(cat "/proc/$keeper_pid/wchan" 2>/dev/null)" = "wait_for_partner" ]; then
        parked=1
        break
    fi
    [ "$announced" = 1 ] && [ "$i" -gt 30 ] && { parked=1; break; }
    sleep 0.1
done
[ "$parked" = 1 ] || { echo "failed to park the process before the global context is created"; exit 1; }

# DateLUT is initialized right after the global context is created, so its absence confirms the
# process has not passed that point.
grep -q 'Initializing DateLUT' "$log" && { echo "the global context already exists"; exit 1; }

kill -SEGV "$keeper_pid"

for _ in {1..600}; do
    kill -0 "$keeper_pid" 2>/dev/null || break
    sleep 0.1
done
kill -9 "$keeper_pid" 2>/dev/null
# Started in a subshell, so it is not a child of this shell and cannot be waited for; the loop above
# already established it is gone.
keeper_pid=""

# Says the handler ran, rather than merely that the signal was delivered.
grep -qh 'Received signal' "$err" "$log" \
    && echo "crash handler ran" \
    || echo "crash handler did not run"

# Says the handler got as far as the crash report, so the next check measures the report path
# instead of silently never reaching it. A sanitizer build stops at the report it is about to
# print, before the logger flushes this line, so the report itself counts as having got here.
{ grep -qhE 'Sending crash report$' "$err" "$log" || grep -qh 'ServerUUID.cpp.*member call on null pointer' "$err"; } \
    && echo "crash report attempted" \
    || echo "crash report not attempted"

# Has to be absent rather than merely unseen, so check for it directly. Only a sanitizer build can
# print it; on every other build the same access reads a wild address instead.
grep -qh 'member call on null pointer' "$err" \
    && echo "null global context dereferenced" \
    || echo "no null dereference"
