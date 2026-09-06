"""
Regression test for the `SYSTEM STOP DISTRIBUTED SENDS` loop in `stop_server`
(tests/docker_scripts/stress_tests.lib).

The loop retries the command up to 30 times before `clickhouse stop`. It passed
`--receive_timeout=10`, which does not bound an attempt against a server that
accepts the connection but never answers Hello: the client keeps the 300s
handshake timeout until the handshake completes, so each attempt blocked for
300s and the loop's only real bound was the job's own 10800s cap. When the cap
fired, the runner was killed before the artifact-upload step, so the server log
that would diagnose the stall was never uploaded.

This test runs the actual loop text (extracted verbatim between the BEGIN/END
markers in stress_tests.lib) against a mock `clickhouse` that never answers,
and asserts:
  - every attempt is bounded, so the loop finishes (the fix),
  - the pre-fix loop text is NOT bounded under the same budget (the oracle:
    without this arm the test would pass on unfixed code),
  - a server that recovers after a few failures is still retried (the fix does
    not buy the bound by reducing patience),
  - the client is invoked with a handshake bound, so the common case fails with
    a diagnostic rather than being killed.
"""

import os
import re
import stat
import subprocess
import textwrap

_LIB = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "tests",
    "docker_scripts",
    "stress_tests.lib",
)

# The pre-fix loop, as it stood before this test was added. Kept as a literal so
# the "fails without the fix" arm cannot silently start testing the fixed text.
_PREFIX_LOOP = textwrap.dedent(
    """\
    for i in {1..30}; do
        if clickhouse client --receive_timeout=10 -q "SYSTEM STOP DISTRIBUTED SENDS"; then
            break
        fi
    done
    """
)

# Wall-clock budget for one loop run. Above the scaled per-attempt bound times
# the scaled iteration count, below the unbounded cost of the same run.
_BUDGET_SEC = 12
_SCALED_BOUND_SEC = 2
_SCALED_ITERATIONS = 2
# How long the wedged mock hangs: far above both the scaled bound and _BUDGET_SEC.
_WEDGE_SEC = 60


def _extract_loop() -> str:
    """The teardown loop, verbatim, from between the BEGIN/END markers."""
    text = open(_LIB, encoding="utf-8").read()
    m = re.search(
        r"# BEGIN: stop-server distributed-sends loop.*?\n(.*?)\n\s*# END: stop-server distributed-sends loop",
        text,
        re.DOTALL,
    )
    assert m, "BEGIN/END teardown-loop markers not found in stress_tests.lib"
    return textwrap.dedent(m.group(1))


def _scale(loop: str) -> str:
    """Shrink the per-attempt bound and the retry count so the test stays fast.

    Only the two numbers are touched; the mechanism under test (a `timeout`
    wrapper around each attempt) is left verbatim. A missing wrapper is
    deliberately NOT an error here, so that the timing assertions observe the
    resulting unboundedness themselves rather than failing on the rewrite. The
    production shape is pinned separately by test_production_values.
    """
    scaled = re.sub(
        r"timeout -s KILL \d+", f"timeout -s KILL {_SCALED_BOUND_SEC}", loop
    )
    scaled, n = re.subn(r"\{1\.\.\d+\}", f"{{1..{_SCALED_ITERATIONS}}}", scaled)
    assert n == 1, f"expected exactly one brace range in:\n{loop}"
    return scaled


def _run_loop(tmp_path, loop: str, mock_body: str, budget_sec: int = _BUDGET_SEC):
    """Run `loop` with `clickhouse` mocked by mock_body, under a wall budget.

    Returns (timed_out, attempts, broke_early). `timed_out` is True when the
    loop did not finish within budget_sec, i.e. it is not bounded.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    counter = tmp_path / "attempts"
    counter.write_text("0", encoding="utf-8")
    argv_log = tmp_path / "argv"
    argv_log.write_text("", encoding="utf-8")

    mock = bindir / "clickhouse"
    mock.write_text(
        "#!/bin/bash\n"
        f'n=$(cat "{counter}"); n=$((n+1)); echo "$n" > "{counter}"\n'
        f'echo "$@" >> "{argv_log}"\n' + mock_body,
        encoding="utf-8",
    )
    mock.chmod(mock.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    done = tmp_path / "done"
    broke = tmp_path / "broke"
    for f in (done, broke):
        if f.exists():
            f.unlink()

    # `break` is observed by touching a file, because the loop's own exit status
    # does not distinguish "broke on success" from "exhausted the retries".
    instrumented, n = re.subn(
        r"(?m)^(\s*)break$", rf'\1touch "{broke}"; break', loop
    )
    assert n == 1, f"expected exactly one `break` in:\n{loop}"
    script = (
        "set -u\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n' + instrumented + f'\ntouch "{done}"\n'
    )
    proc = subprocess.run(
        ["timeout", "-s", "KILL", str(budget_sec), "bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=budget_sec + 30,
    )
    timed_out = proc.returncode == 137 or not done.exists()
    attempts = int(counter.read_text(encoding="utf-8").strip() or 0)
    return timed_out, attempts, broke.exists(), argv_log.read_text(encoding="utf-8")


# A server that accepts the connection and never answers: the state in which
# `--receive_timeout` is inert, because the client is still in the handshake.
_WEDGED = f"sleep {_WEDGE_SEC}\nexit 209\n"


def test_production_values():
    """The shipped loop bounds each attempt and keeps all 30 retries."""
    loop = _extract_loop()
    assert "timeout -s KILL 15 " in loop, loop
    assert "--handshake_timeout_ms=10000" in loop, loop
    assert "{1..30}" in loop, loop


def test_wedged_server_loop_is_bounded(tmp_path):
    """Each attempt is capped, so the loop finishes and `clickhouse stop` runs."""
    timed_out, attempts, broke, _ = _run_loop(
        tmp_path, _scale(_extract_loop()), _WEDGED
    )
    assert not timed_out, "loop did not finish within its budget"
    assert attempts == _SCALED_ITERATIONS, attempts
    assert not broke, "loop broke early although every attempt failed"


def test_wedged_server_prefix_loop_is_unbounded(tmp_path):
    """The oracle: the pre-fix loop text blows the same budget it now fits in."""
    timed_out, attempts, _, _ = _run_loop(tmp_path, _scale_prefix(), _WEDGED)
    assert timed_out, "pre-fix loop finished within the budget, so this test is vacuous"
    assert attempts == 1, attempts


def _scale_prefix() -> str:
    """The pre-fix loop with the same retry count as the scaled fixed loop."""
    scaled, n = re.subn(r"\{1\.\.\d+\}", f"{{1..{_SCALED_ITERATIONS}}}", _PREFIX_LOOP)
    assert n == 1
    return scaled


def test_recovering_server_is_still_retried(tmp_path):
    """Patience is unchanged: a server that answers on the 6th try is waited for."""
    mock = textwrap.dedent(
        """\
        if [[ "$n" -le 5 ]]; then
            exit 209
        fi
        exit 0
        """
    )
    timed_out, attempts, broke, _ = _run_loop(tmp_path, _extract_loop(), mock)
    assert not timed_out
    assert attempts == 6, attempts
    assert broke, "loop did not break on success"


def test_client_is_invoked_with_a_handshake_bound(tmp_path):
    """The common case fails with a diagnostic instead of being killed."""
    _, _, _, argv = _run_loop(tmp_path, _scale(_extract_loop()), "exit 209\n")
    assert argv.strip(), "mock recorded no invocation"
    for line in argv.strip().splitlines():
        assert "--handshake_timeout_ms=10000" in line, line
        assert "SYSTEM STOP DISTRIBUTED SENDS" in line, line
