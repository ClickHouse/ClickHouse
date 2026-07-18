#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate promptly even when the blocked terminal sink is the
# server-log / profile-events stream rather than the result set. That stream is a separate
# WriteBufferFromFileDescriptor on stderr (or std_out with --server_logs_file=-), and it is flushed
# by onLogData / onProfileEvents while the query runs. Unlike 04490 (which keeps stderr off the pty
# and blocks on the result-set write to stdout), here stdout goes to /dev/null and stderr is a pty
# whose master is never read, so the only terminal-facing sink that can wedge is the log stream.
# --print-profile-events with a long-running query keeps the server emitting incremental
# profile-events packets, so the stuck stderr pty fills and the client blocks in a profile-events
# flush. A single Ctrl+C must still terminate the client.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT" CLICKHOUSE_DATABASE="$CLICKHOUSE_DATABASE" python3 - <<'PYEOF'
import os
import shlex
import signal
import subprocess
import sys
import time

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
query_id = database + "_cancel_blocked_terminal_logs"

# A long-running query that does not sleep server-side (so cancellation is not gated on a server
# sleep) and keeps the server emitting incremental profile-events packets, which the client flushes
# to the stuck stderr pty. Disabled limits keep it immune to the randomized settings of the flaky
# check. The query is never meant to finish - it is cancelled.
query = (
    "SELECT sum(number) FROM numbers(20000000000) "
    "SETTINGS max_threads = 1, max_execution_time = 0, max_rows_to_read = 0"
)

# Connect the client's stderr (the default server-log / profile-events sink) to a pty whose master
# we never read, so the terminal buffer fills and the client blocks flushing profile events. stdout
# goes to /dev/null (never blocks), so the result-set write cannot be what wedges - the point of the
# test is the auxiliary log stream, not the result set (that is 04490's job).
master, slave = os.openpty()
null_fd = os.open(os.devnull, os.O_WRONLY)
proc = subprocess.Popen(
    client + ["--query_id", query_id, "--query", query,
              "--print-profile-events", "--profile-events-delay-ms", "0"],
    stdin=subprocess.DEVNULL,
    stdout=null_fd,
    stderr=slave,
    close_fds=True,
)
os.close(slave)
os.close(null_fd)


def client_running():
    out = subprocess.run(
        client + ["--query", f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out.isdigit() and int(out) >= 1


try:
    # Wait until the query is actually running on the server.
    started = False
    for _ in range(120):
        if client_running():
            started = True
            break
        if proc.poll() is not None:
            break
        time.sleep(0.5)

    if not started:
        print("FAIL: the query did not reach the running state")
        sys.exit(0)

    # Give the client time to fill the stuck stderr pty with profile-events packets and block in a
    # flush of the log stream.
    time.sleep(4)

    # A single Ctrl+C must be enough to terminate the client even though it is blocked writing the
    # profile events to a stuck terminal.
    os.kill(proc.pid, signal.SIGINT)

    try:
        proc.wait(timeout=10)
        print("OK: client terminated after Ctrl+C")
    except subprocess.TimeoutExpired:
        print("FAIL: client is still running after Ctrl+C")
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    os.close(master)
PYEOF
