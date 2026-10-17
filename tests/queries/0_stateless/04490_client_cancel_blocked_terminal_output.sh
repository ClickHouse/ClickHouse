#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate the output of a result set promptly even when the
# output sink is a real terminal (a pty) that is not being drained - the headline case of the
# feature (a slow or stuck terminal). Unlike the pipe/FIFO sinks exercised by
# 04318_client_cancel_blocked_output.sh, a pty goes through the isatty() branch of the responsive
# write path, where a poll() reporting POLLOUT does not guarantee room for a whole chunk, so the
# write must be non-blocking for a single Ctrl+C to be honored.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_cancel_blocked_terminal.err"
trap 'rm -f "$CLIENT_ERR"' EXIT

CLIENT_ERR="$CLIENT_ERR" python3 - <<'PYEOF'
import os
import pty
import shlex
import signal
import subprocess
import sys
import time

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
client_err = os.environ["CLIENT_ERR"]
query_id = database + "_cancel_blocked_terminal_output"

# An effectively unbounded result; disabled limits and small blocks keep the test cheap and immune
# to the randomized settings used by the flaky check (which could otherwise error out early).
query = (
    "SELECT number, repeat('x', 100) FROM numbers(1000000000) "
    "SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0, "
    "max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0"
)

# Connect only the client's stdout to a pty whose master we never read, so the terminal output
# buffer fills and the client blocks writing the result set. stderr goes to a regular file (which
# never blocks), so the client's cancellation messages cannot get stuck on the full terminal - the
# point of the test is the result-set write on stdout, not the diagnostics.
master, slave = os.openpty()
err_fd = os.open(client_err, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
proc = subprocess.Popen(
    client + ["--query_id", query_id, "--query", query],
    stdin=subprocess.DEVNULL,
    stdout=slave,
    stderr=err_fd,
    close_fds=True,
)
os.close(slave)
os.close(err_fd)


def client_running():
    out = subprocess.run(
        client + ["--query", f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out.isdigit() and int(out) >= 1


try:
    # Wait until the query is actually running on the server (and thus blocked on the stuck pty).
    started = False
    for _ in range(120):
        if client_running():
            started = True
            break
        # If the client has already exited, it never reached the blocked state - fail explicitly.
        if proc.poll() is not None:
            break
        time.sleep(0.5)

    if not started:
        print("FAIL: the query did not reach the running state")
        with open(client_err) as f:
            sys.stdout.write("--- client stderr ---\n" + f.read())
        sys.exit(0)

    # A single Ctrl+C must be enough to terminate the client even though it is blocked writing to a
    # stuck terminal.
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
