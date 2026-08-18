"""
Regression test for the post-fuzz server-liveness probe loop in
ci/jobs/scripts/fuzzer/run-fuzzer.sh.

A run that ends cleanly (the fuzzer is SIGTERM'd at its 30m time limit, exit
143) was reported as a bogus "Received signal 15" FAIL whenever the very first
post-fuzz `SELECT 1` probe hit its 5s receive timeout: the server is alive but
slow to answer right after 30m of ASAN fuzzing, yet the loop's catch-all branch
treated that single timeout as `server_died=1`. The Python side then checks
`server_died` before the exit-143 OK branch and scrapes the server's NORMAL
shutdown "Received signal 15" line.

This test runs the actual loop text (extracted verbatim between the
BEGIN/END markers in run-fuzzer.sh) against a mock `clickhouse-client`, and
asserts:
  - a transient timeout that then recovers leaves server_died=0,
  - a genuinely dead server (Connection refused / EOF) sets server_died=1 at once,
  - a persistent timeout (a real hang) still sets server_died=1 once it persists.
"""

import os
import re
import stat
import subprocess
import textwrap

_RUN_FUZZER = os.path.join(
    os.path.dirname(__file__),
    "..",
    "jobs",
    "scripts",
    "fuzzer",
    "run-fuzzer.sh",
)


def _extract_loop() -> str:
    """The liveness loop, verbatim, from between the BEGIN/END markers."""
    text = open(_RUN_FUZZER, encoding="utf-8").read()
    m = re.search(
        r"# BEGIN: server-liveness probe loop.*?\n(.*?)\n\s*# END: server-liveness probe loop",
        text,
        re.DOTALL,
    )
    assert m, "BEGIN/END liveness-probe markers not found in run-fuzzer.sh"
    return textwrap.dedent(m.group(1))


def _run_loop(tmp_path, mock_body: str):
    """Run the extracted loop with `clickhouse-client` mocked by mock_body.

    The mock reads the per-attempt counter from a file so it can vary its
    behavior across attempts. Returns the final server_died value (0/1).
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    counter = tmp_path / "attempt"
    counter.write_text("0", encoding="utf-8")

    mock = bindir / "clickhouse-client"
    mock.write_text(
        "#!/bin/bash\n"
        f'n=$(cat "{counter}"); n=$((n+1)); echo "$n" > "{counter}"\n'
        + mock_body,
        encoding="utf-8",
    )
    mock.chmod(mock.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    # `sleep` is mocked to a no-op so the test does not actually wait.
    sleep = bindir / "sleep"
    sleep.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    sleep.chmod(sleep.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    script = (
        "set -u\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n'
        + _extract_loop()
        + '\necho "SERVER_DIED=$server_died"\n'
    )
    out = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    m = re.search(r"SERVER_DIED=(\d)", out.stdout)
    assert m, f"loop produced no SERVER_DIED marker:\nSTDOUT:\n{out.stdout}\nSTDERR:\n{out.stderr}"
    return int(m.group(1))


_TIMEOUT_ERR = (
    'echo "Code: 209. DB::NetException: Timeout exceeded while receiving data '
    'from server. Waited for 5 seconds, timeout is 5 seconds." >&2; exit 1'
)
_REFUSED_ERR = (
    'echo "Code: 210. DB::NetException: Connection refused (localhost:9000). '
    '(NETWORK_ERROR)" >&2; exit 1'
)


def test_transient_timeout_then_recovers_is_not_server_died(tmp_path):
    # First 3 probes time out (alive but slow), 4th succeeds -> NOT dead.
    mock = textwrap.dedent(
        f"""\
        if [[ "$n" -le 3 ]]; then
            {_TIMEOUT_ERR}
        fi
        echo 1
        exit 0
        """
    )
    assert _run_loop(tmp_path, mock) == 0


def test_connection_refused_is_server_died_immediately(tmp_path):
    # A genuinely dead server -> server_died=1 on the first probe.
    mock = f"{_REFUSED_ERR}\n"
    assert _run_loop(tmp_path, mock) == 1


def test_persistent_timeout_is_eventually_server_died(tmp_path):
    # The server never answers -> a real hang -> server_died=1 once timeouts persist.
    mock = f"{_TIMEOUT_ERR}\n"
    assert _run_loop(tmp_path, mock) == 1
