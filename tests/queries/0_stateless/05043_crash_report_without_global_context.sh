#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts further server processes, which the fast test image does not carry.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The keeper mode is a build option, and one of the two arms below drives a keeper.
if ! $CLICKHOUSE_BINARY help 2>&1 | grep -qF 'clickhouse keeper [args]'; then
    echo "@@SKIP@@: the binary has no keeper mode"
    exit 0
fi

# A daemon runs the crash-report half of the crash handler, which clickhouse-local never reaches
# because it has no daemon, so covering it needs a daemon parked before the report is complete.
#
# The report names the server UUID, which is read from <path>/uuid during startup, and reading a
# FIFO blocks until a writer appears, so a FIFO there parks the process at that read with the signal
# listener already running. Both arms below use that same park, and they differ only in which daemon
# they park, because the two daemons read the UUID on opposite sides of creating the global context:
#
#   keeper  reads it BEFORE  creating the context, so the context is absent   (arm 1)
#   server  reads it AFTER   creating the context, so the context exists but the UUID is unset (arm 2)
#
# Those are the two states in which the report is built without a UUID to name, and they are
# reachable from different daemons, so neither arm can stand in for the other.
#
# The crash report is only built when an endpoint is configured, and the writer connects before it
# asks for the server UUID, so the endpoint has to accept the connection or the report is abandoned
# before the part under test runs.
#
# Unique names keep this parallel-safe.
work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05043"
rm -rf "$work"
mkdir -p "$work"

pids="$work/pids"
: > "$pids"

cleanup() {
    while read -r p; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null
    done < "$pids"
    wait 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

# Accepts the report and answers it. Answering matters for the run time as much as accepting: the
# writer waits for a response before it finishes, so a listener that only drains costs a read
# timeout, about a minute, on every run.
#
# The kernel picks the port and the listener publishes it once it is accepting, so a parallel copy
# cannot lose a race for a predetermined number. The port file is published by rename, so reading it
# never yields a partial number, and its presence means the socket is already accepting.
#
# The two markers separate "the connection was opened" from "the whole report arrived". The report
# is sent with chunked transfer encoding, whose body ends with a zero-length chunk that is written
# only when the writer finalizes, downstream of everything the body is built from, so that chunk
# means the report was written in full rather than abandoned part way through.
#
# The terminator is matched against the body rather than the whole request because the header block
# ends in a digit followed by two CRLF pairs of its own, which matches the same bytes before any body
# byte exists.
#
# Each arm gets its own listener under its own directory, so one arm's markers cannot answer for the
# other's.
#
# This is killed at the end of the run, and a shell reports the death of its own background job on
# its own stderr, which the runner reads as a failure, so a subshell keeps the job out of this one.
start_sink() {
    local dir="$1"
    ( python3 -c "
import os, socket, threading
srv = socket.socket()
srv.bind(('127.0.0.1', 0))
srv.listen(4)
with open('$dir/sink_port.tmp', 'w') as f:
    f.write(str(srv.getsockname()[1]))
os.rename('$dir/sink_port.tmp', '$dir/sink_port')
def serve(c):
    try:
        # Outlasts a slow symbolization pass, so a read cannot decide the outcome instead.
        c.settimeout(300)
        data = b''
        while True:
            chunk = c.recv(65536)
            if not chunk:
                break
            data += chunk
            if b'\r\n\r\n' not in data:
                continue
            if not os.path.exists('$dir/request_started'):
                open('$dir/request_started', 'w').close()
            # A chunk boundary can fall anywhere, so this reads the accumulated body, not the last
            # read. The payload is a few KB.
            if data.split(b'\r\n\r\n', 1)[1].endswith(b'0\r\n\r\n'):
                open('$dir/report_complete', 'w').close()
                break
        c.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
    except OSError:
        pass
    finally:
        c.close()
while True:
    c, _ = srv.accept()
    threading.Thread(target=serve, args=(c,), daemon=True).start()
" >/dev/null 2>&1 & echo $! >> "$pids" ) 2>/dev/null

    local i
    for i in {1..300}; do
        [ -f "$dir/sink_port" ] && return 0
        sleep 0.1
    done
    echo "sink did not start"
    exit 1
}

# Waits for the report, then reads the four things this test is about off the run. The wait leaves on
# the marker when the report arrives, however slow symbolization was, and on the exit when the
# handler faults instead and stops waiting for a report it can no longer send. Its count only has to
# exceed that wait, so it decides no outcome; the checks below do.
report_checks() {
    local dir="$1" label="$2" pid="$3" err="$4" log="$5"
    local i
    for i in {1..4000}; do
        [ -f "$dir/report_complete" ] && break
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null

    # Says the handler ran, rather than merely that the signal was delivered.
    grep -qh 'Received signal' "$err" "$log" \
        && echo "$label: crash handler ran" \
        || echo "$label: crash handler did not run"

    # Says the handler got as far as opening the connection, so the next check measures the report
    # path instead of silently never reaching it. A sanitizer build stops inside the report it is
    # about to print, so the report it prints counts as having got here.
    { [ -f "$dir/request_started" ] || grep -qh 'ServerUUID.cpp.*member call on null pointer' "$err"; } \
        && echo "$label: crash report attempted" \
        || echo "$label: crash report not attempted"

    # Says the report was written in full. The part under test sits between the connection above and
    # the finalize that terminates the body, so a report abandoned in between never gets here. A
    # sanitizer build stops there and prints the null dereference instead, which is its equivalent
    # evidence.
    { [ -f "$dir/report_complete" ] || grep -qh 'ServerUUID.cpp.*member call on null pointer' "$err"; } \
        && echo "$label: crash report completed" \
        || echo "$label: crash report not completed"

    # Has to be absent rather than merely unseen, so check for it directly. Only a sanitizer build
    # can print it; on every other build the same access reads a wild address instead.
    grep -qh 'member call on null pointer' "$err" \
        && echo "$label: null global context dereferenced" \
        || echo "$label: no null dereference"
}

##########################################################################################
# Arm 1: keeper. The UUID is read before the global context is created, so the context is absent.
##########################################################################################

kdir="$work/keeper"
mkdir -p "$kdir/data"
kerr="$kdir/stderr.txt"
klog="$kdir/keeper.log"
start_sink "$kdir"
ksink_port=$(cat "$kdir/sink_port")

# No tcp_port: the test never connects to the keeper, and omitting it is supported.
cat > "$kdir/config.xml" <<EOF
<clickhouse>
    <logger><level>trace</level><log>$klog</log><console>0</console></logger>
    <send_crash_reports>
        <enabled>true</enabled>
        <endpoint>http://127.0.0.1:$ksink_port/</endpoint>
    </send_crash_reports>
    <keeper_server>
        <server_id>1</server_id>
        <path>$kdir/data</path>
        <log_storage_path>$kdir/data/log</log_storage_path>
        <snapshot_storage_path>$kdir/data/snapshots</snapshot_storage_path>
        <coordination_settings><session_timeout_ms>30000</session_timeout_ms></coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>0</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF

# Nothing writes to the FIFO, so reading the UUID parks here.
mkfifo "$kdir/data/uuid"

# The shell reports an abnormal exit of a background job on its own stderr, naming a signal the
# process is expected to take here, and the manner of the exit is not what this test reads. A
# subshell keeps the job, and that report, out of this one.
( $CLICKHOUSE_BINARY keeper --config-file="$kdir/config.xml" >/dev/null 2>"$kerr" & echo $! > "$kdir/pid" ) 2>/dev/null
keeper_pid=$(cat "$kdir/pid")
echo "$keeper_pid" >> "$pids"

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
    grep -q 'Initializing DateLUT' "$klog" 2>/dev/null && { parked=0; break; }
    grep -q 'Starting ClickHouse Keeper' "$klog" 2>/dev/null && announced=1
    if [ "$(cat "/proc/$keeper_pid/wchan" 2>/dev/null)" = "wait_for_partner" ]; then
        parked=1
        break
    fi
    [ "$announced" = 1 ] && [ "$i" -gt 30 ] && { parked=1; break; }
    sleep 0.1
done
[ "$parked" = 1 ] || { echo "keeper: failed to park the process before the global context is created"; exit 1; }

# DateLUT is initialized right after the global context is created, so its absence confirms the
# process has not passed that point.
grep -q 'Initializing DateLUT' "$klog" && { echo "keeper: the global context already exists"; exit 1; }

kill -SEGV "$keeper_pid"
report_checks "$kdir" "keeper" "$keeper_pid" "$kerr" "$klog"

##########################################################################################
# Arm 2: server. The UUID is read after the global context is created, so the context exists while
# the UUID is still unset. That is a state the keeper never has, and the arm above cannot reach it.
##########################################################################################

sdir="$work/server"
mkdir -p "$sdir/data"
serr="$sdir/stderr.txt"
slog="$sdir/server.log"
start_sink "$sdir"
ssink_port=$(cat "$sdir/sink_port")

# No ports: the test never connects to the server, and it parks long before it opens a listener.
cat > "$sdir/config.xml" <<EOF
<clickhouse>
    <logger><level>trace</level><log>$slog</log><console>0</console></logger>
    <path>$sdir/data</path>
    <tmp_path>$sdir/data/tmp</tmp_path>
    <user_files_path>$sdir/data/user_files</user_files_path>
    <send_crash_reports>
        <enabled>true</enabled>
        <endpoint>http://127.0.0.1:$ssink_port/</endpoint>
    </send_crash_reports>
    <users><default><profile>default</profile><quota>default</quota>
        <networks><ip>::/0</ip></networks></default></users>
    <profiles><default></default></profiles>
    <quotas><default></default></quotas>
</clickhouse>
EOF

mkfifo "$sdir/data/uuid"

# A server not attached to a terminal forks a watchdog and keeps running as its child, and the
# watchdog forwards only the termination signals, not this one, so the signal below has to reach the
# process that parked. Disabling the fork is what makes the started process that process.
( CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_BINARY server --config-file="$sdir/config.xml" >/dev/null 2>"$serr" & echo $! > "$sdir/pid" ) 2>/dev/null
server_pid=$(cat "$sdir/pid")
echo "$server_pid" >> "$pids"

# The state this arm exists for is bounded on both sides, so both bounds are checked, and the status
# file carries both bounds on its own. It is written after the global context is created and
# immediately before the UUID is read, so its presence says the context exists; and it is written by
# the thread that then blocks on the FIFO, which nothing ever writes to, so the process can never
# reach the load that would end the state. Signalling outside those bounds would measure one of the
# states the other arm or the integration test already covers.
#
# The file itself is the witness rather than the line the same code logs, because logging is
# asynchronous, so that line can still be queued at the moment the process is already parked, while
# the file is written synchronously before the read. Waiting on the line instead loses the race
# occasionally and reports a state the process has in fact already left.
parked=0
seen=0
for i in {1..600}; do
    kill -0 "$server_pid" 2>/dev/null || break
    [ -s "$sdir/data/status" ] && seen=1
    if [ "$seen" = 1 ] && [ "$(cat "/proc/$server_pid/wchan" 2>/dev/null)" = "wait_for_partner" ]; then
        parked=1
        break
    fi
    # `wait_for_partner` needs symbol names in /proc, so the file alone decides when it has none.
    [ "$seen" = 1 ] && [ "$i" -gt 30 ] && { parked=1; break; }
    sleep 0.1
done
[ "$parked" = 1 ] || { echo "server: failed to park the process before the server UUID is read"; exit 1; }

[ -s "$sdir/data/status" ] || { echo "server: the global context does not exist yet"; exit 1; }
grep -q 'Initializing DateLUT' "$slog" && { echo "server: the server UUID is already loaded"; exit 1; }

kill -SEGV "$server_pid"
report_checks "$sdir" "server" "$server_pid" "$serr" "$slog"
