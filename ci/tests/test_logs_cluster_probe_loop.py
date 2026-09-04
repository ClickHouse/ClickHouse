"""
Regression test for the CI logs-cluster connectivity probe loop in
ci/jobs/scripts/functional_tests/setup_log_cluster.sh (check_logs_credentials).

The remote logs cluster occasionally resets the connection. That reset is
transient and unrelated to the credentials, so the probe retries it (up to 3
attempts). Everything else must fail fast, because log export is best-effort
and must not slow every CI job during a real outage or a permanent error.

The retry predicate matches the stderr message "Connection reset by peer", not
the process exit status: the shell keeps only the low 8 bits of the client's
exit code, so unrelated errors (e.g. 466/722) alias to 210, and the client
also raises NETWORK_ERROR (210) for permanent cases such as a protocol
mismatch. Matching on $? alone would retry those too.

This test runs the actual loop text (extracted verbatim between the BEGIN/END
markers in setup_log_cluster.sh) against a mock `clickhouse-client`, and asserts:
  - a reset that then recovers returns success (exit 0),
  - a non-reset failure (any code, incl. 210) fails fast without retrying,
  - a persistent reset exhausts the 3 attempts and then fails.
"""

import os
import re
import stat
import subprocess
import textwrap

_SETUP = os.path.join(
    os.path.dirname(__file__),
    "..",
    "jobs",
    "scripts",
    "functional_tests",
    "setup_log_cluster.sh",
)


def _extract_loop() -> str:
    """The probe loop, verbatim, from between the BEGIN/END markers."""
    text = open(_SETUP, encoding="utf-8").read()
    m = re.search(
        r"# BEGIN: logs-cluster connectivity probe loop\n(.*?)\n\s*# END: logs-cluster connectivity probe loop",
        text,
        re.DOTALL,
    )
    assert m, "BEGIN/END probe-loop markers not found in setup_log_cluster.sh"
    return textwrap.dedent(m.group(1))


def _run_loop(tmp_path, mock_body: str):
    """Run the extracted loop with `clickhouse-client` mocked by mock_body.

    The mock reads the per-attempt counter from a file so it can vary its
    behavior across attempts. Returns (rc, attempts_made).
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

    # The extracted loop uses CONNECTION_ARGS, `code`, `err`, `attempt` and
    # `return 0` on success, so it is wrapped in a function that returns $code
    # after the loop (mirroring check_logs_credentials).
    script = (
        "set -u\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n'
        "probe() {\n"
        "  local code=0 err attempt\n"
        "  CONNECTION_ARGS=(--host localhost)\n"
        + textwrap.indent(_extract_loop(), "  ")
        + "\n  return \"$code\"\n"
        "}\n"
        "probe\n"
        'echo "RC=$?"\n'
        f'echo "ATTEMPTS=$(cat "{counter}")"\n'
    )
    out = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    rc = re.search(r"RC=(\d+)", out.stdout)
    attempts = re.search(r"ATTEMPTS=(\d+)", out.stdout)
    assert rc and attempts, (
        f"loop produced no RC/ATTEMPTS marker:\nSTDOUT:\n{out.stdout}\nSTDERR:\n{out.stderr}"
    )
    return int(rc.group(1)), int(attempts.group(1))


_RESET_ERR = (
    'echo "Code: 210. DB::NetException: Connection reset by peer, while '
    'reading from socket (host:9440). (NETWORK_ERROR)" >&2; exit 210'
)
# A permanent NETWORK_ERROR (210) that is NOT a reset (e.g. protocol mismatch).
_PERMANENT_210_ERR = (
    'echo "Code: 210. DB::NetException: Unexpected packet from server "'
    '"(expected Hello or Exception, got Unknown). (NETWORK_ERROR)" >&2; exit 210'
)
# An unrelated error whose exit code aliases to 210 in the low 8 bits (466%256=210).
_ALIASED_ERR = (
    'echo "Code: 466. DB::Exception: some unrelated error. " >&2; exit 466'
)


def test_reset_then_success_recovers(tmp_path):
    # First 2 probes reset (transient), 3rd succeeds -> success.
    mock = textwrap.dedent(
        f"""\
        if [[ "$n" -le 2 ]]; then
            {_RESET_ERR}
        fi
        echo 1
        exit 0
        """
    )
    rc, attempts = _run_loop(tmp_path, mock)
    assert rc == 0
    assert attempts == 3


def test_permanent_network_error_fails_fast(tmp_path):
    # A permanent NETWORK_ERROR 210 that is not a reset -> no retry.
    rc, attempts = _run_loop(tmp_path, f"{_PERMANENT_210_ERR}\n")
    assert rc == 210
    assert attempts == 1


def test_aliased_exit_code_fails_fast(tmp_path):
    # An unrelated error aliasing to 210 in the low 8 bits -> no retry.
    rc, attempts = _run_loop(tmp_path, f"{_ALIASED_ERR}\n")
    assert rc != 0
    assert attempts == 1


def test_persistent_reset_exhausts_retries(tmp_path):
    # The cluster keeps resetting -> all 3 attempts used, then fails.
    rc, attempts = _run_loop(tmp_path, f"{_RESET_ERR}\n")
    assert rc == 210
    assert attempts == 3
