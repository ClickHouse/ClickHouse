#!/usr/bin/env bash
# Tags: no-fasttest

# The exceptional teardown - `onReceiveExceptionFromServer()` and the `receiveResult()` cleanup
# catch - runs `resetOutput()` while the interrupt handler is still armed. With
# `partial_result_on_first_cancel = 1` the stage-one interrupt that led into this teardown is
# deliberately not latched by the teardown baseline (the partial result it asked for still has to
# be published), so the decorative progress clears on `tty_buf` cannot rely on the responsive
# cancellation hook: on a terminal that stopped draining they would hold the client until a second
# Ctrl+C. Here the primary `INTO OUTFILE` sink is a FIFO that never gets a reader, so the single
# Ctrl+C aborts the blocked `open()` with `QUERY_WAS_CANCELLED` and the client enters that
# exceptional teardown with the progress table still to be cleared on a stuck terminal.
# 04672_client_cancel_exception_teardown_pager_partial_result only covers the pager (a real output
# sink) and a signal arriving *during* the teardown.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_teardown_blocked_terminal.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_teardown_blocked_terminal.err"
trap 'rm -f "$FIFO" "$CLIENT_ERR"' EXIT

# No reader is ever attached to this FIFO, so the client stays inside `open()` until the signal.
# `APPEND` makes the client open the pre-created FIFO directly (a plain `INTO OUTFILE` refuses an
# existing file, and `TRUNCATE` would write to a temporary regular file instead).
mkfifo "$FIFO"

FIFO="$FIFO" CLIENT_ERR="$CLIENT_ERR" python3 - <<'PYEOF'
import fcntl
import os
import pty
import shlex
import signal
import subprocess
import sys
import time

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
fifo = os.environ["FIFO"]
client_err = os.environ["CLIENT_ERR"]
query_id = database + "_cancel_exception_teardown_blocked_terminal"

query = (
    "SELECT number, repeat('x', 100) FROM numbers(1000000000) "
    f"INTO OUTFILE '{fifo}' APPEND FORMAT TabSeparated "
    "SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0, "
    "max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0"
)

# The progress table is rendered to the terminal, which here is a pty whose master is never read.
# Fill its output queue up front, so that every further write to it blocks - including the clear
# that the teardown performs. stderr is a regular file, so the client diagnostics never get stuck:
# the point of the test is the decorative terminal write during teardown.
master, slave = os.openpty()
flags = fcntl.fcntl(slave, fcntl.F_GETFL)
fcntl.fcntl(slave, fcntl.F_SETFL, flags | os.O_NONBLOCK)
filler = b"." * 4096
try:
    while True:
        os.write(slave, filler)
except BlockingIOError:
    pass
fcntl.fcntl(slave, fcntl.F_SETFL, flags)

err_fd = os.open(client_err, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
proc = subprocess.Popen(
    client + ["--progress-table=on", "--partial_result_on_first_cancel=1",
              "--query_id", query_id, "--query", query],
    stdin=subprocess.DEVNULL,
    stdout=slave,
    stderr=err_fd,
    close_fds=True,
)
os.close(slave)
os.close(err_fd)


def query_running():
    out = subprocess.run(
        client + ["--query", f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out.isdigit() and int(out) >= 1


try:
    # Wait until the query is running, which means the client has received the first block and is
    # now blocked opening the output file.
    started = False
    for _ in range(120):
        if query_running():
            started = True
            break
        if proc.poll() is not None:
            break
        time.sleep(0.5)

    if not started:
        print("FAIL: the query did not reach the running state")
        with open(client_err) as f:
            sys.stdout.write("--- client stderr ---\n" + f.read())
        sys.exit(0)

    # A single Ctrl+C has to be enough: it aborts the blocked open() and the exceptional teardown
    # that follows must not wait for the stuck terminal to accept the progress clear.
    os.kill(proc.pid, signal.SIGINT)

    try:
        proc.wait(timeout=30)
        print("OK: client terminated after the first Ctrl+C")
    except subprocess.TimeoutExpired:
        print("FAIL: client is still waiting after the first Ctrl+C")
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    os.close(master)
PYEOF
