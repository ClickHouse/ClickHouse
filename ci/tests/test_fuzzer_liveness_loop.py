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

It also covers the post-fuzz memory-stuck detection (#110074): a server pinned
above its cap after the fuzz run rejects every idle probe with the server-global
"(total) memory limit exceeded" tracker error and never reclaims. The loop must
detect that (two tiers: fast when host MemAvailable is low, patient otherwise),
write the server_memory_stuck.txt marker, and set server_died=1 -- while leaving
the pre-existing transient-241 / timeout / TOO_MANY tolerances intact, and
failing closed when the window exhausts with zero answered probes.

Both tier thresholds and the exhaustion dominance cutoff are covered at their
adjacent boundaries (just below / exactly at the 4 GiB MemAvailable floor, 29 vs
30 server-global rejections) and pinned statically, since values loosely inside
the brackets would otherwise change production behavior while staying green.

The reap and teardown polls are exercised verbatim with real child processes:
normal exits propagate the child status; deadline escalations SIGKILL and record
the watchdog (including the reap escalation's tracer and actual-client kills, and
its no-tracer ASan shape under `set -u`), and a child that stays alive well into
the deadline is tolerated (reaped normally, no watchdog) rather than killed early.
Because those tests mock `sleep`, the wall-clock magnitude of the bounds is pinned
statically instead: the shared 1s poll cadence, the counter step, and the reap /
teardown / grace limits are asserted against the marked block text.
"""

import os
import re
import stat
import subprocess
import time
import textwrap

_RUN_FUZZER = os.path.join(
    os.path.dirname(__file__),
    "..",
    "jobs",
    "scripts",
    "fuzzer",
    "run-fuzzer.sh",
)


def _extract_block(name: str) -> str:
    """A marked block, verbatim, from between its BEGIN/END markers."""
    text = open(_RUN_FUZZER, encoding="utf-8").read()
    m = re.search(
        rf"# BEGIN: {re.escape(name)}.*?\n(.*?)\n\s*# END: {re.escape(name)}",
        text,
        re.DOTALL,
    )
    assert m, f"BEGIN/END markers for {name!r} not found in run-fuzzer.sh"
    return textwrap.dedent(m.group(1))


def _extract_loop() -> str:
    """The liveness loop, verbatim (backwards-compatible name)."""
    return _extract_block("server-liveness probe loop")


def _run_loop(
    tmp_path,
    mock_body: str,
    mem_available_kb: int = 99999999,
    extra_env: dict | None = None,
):
    """Run the extracted loop with `clickhouse-client` mocked by mock_body.

    The mock reads the per-attempt counter from a file so it can vary its
    behavior across attempts. `mem_available_kb` seeds a fake /proc/meminfo via
    the loop's MEMINFO_PATH override (defaults to plenty of free memory so the
    fast tier does not fire unless a test asks for it). `extra_env`, when set,
    exports extra environment variables into the script preamble (e.g. the
    PROBE_STAGE_DEADLINE_SECONDS seam); default None keeps every existing call
    byte-compatible.

    Returns a dict with the loop's final server_died / server_memory_stuck /
    memory_limit_probes values, the marker file contents (or None), and the
    number of clickhouse-client attempts the mock served.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    counter = tmp_path / "attempt"
    counter.write_text("0", encoding="utf-8")

    mock = bindir / "clickhouse-client"
    mock.write_text(
        "#!/bin/bash\n"
        f'n=$(cat "{counter}"); n=$((n+1)); echo "$n" > "{counter}"\n' + mock_body,
        encoding="utf-8",
    )
    mock.chmod(mock.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    # `sleep` is mocked to a no-op so the test does not actually wait.
    sleep = bindir / "sleep"
    sleep.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    sleep.chmod(sleep.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        f"MemTotal:       64000000 kB\nMemAvailable:   {mem_available_kb} kB\n",
        encoding="utf-8",
    )

    env_exports = "".join(f'export {k}="{v}"\n' for k, v in (extra_env or {}).items())
    script = (
        # Same options as run-fuzzer.sh, so an unguarded failure inside an
        # extracted block aborts here exactly as it would in production.
        "set -euo pipefail\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n'
        f'export MEMINFO_PATH="{meminfo}"\n'
        + env_exports
        + _extract_loop()
        + '\necho "SERVER_DIED=$server_died"\n'
        + '\necho "SERVER_MEMORY_STUCK=$server_memory_stuck"\n'
        + '\necho "MEMORY_LIMIT_PROBES=$memory_limit_probes"\n'
    )
    started = time.monotonic()
    out = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    elapsed = time.monotonic() - started
    m = re.search(r"SERVER_DIED=(\d)", out.stdout)
    assert (
        m
    ), f"loop produced no SERVER_DIED marker:\nSTDOUT:\n{out.stdout}\nSTDERR:\n{out.stderr}"
    stuck = re.search(r"SERVER_MEMORY_STUCK=(\d)", out.stdout)
    probes = re.search(r"MEMORY_LIMIT_PROBES=(\d+)", out.stdout)
    marker = tmp_path / "server_memory_stuck.txt"
    return {
        "server_died": int(m.group(1)),
        "server_memory_stuck": int(stuck.group(1)) if stuck else 0,
        "memory_limit_probes": int(probes.group(1)) if probes else 0,
        "marker": marker.read_text(encoding="utf-8") if marker.exists() else None,
        "attempts": int(counter.read_text(encoding="utf-8")),
        # Wall-clock cost of the whole extracted stage. Only meaningful when the
        # mock itself blocks (`sleep` is mocked no-op), which is exactly the case
        # the aggregate-deadline overrun tests need.
        "elapsed": elapsed,
        "stdout": out.stdout,
    }


def _server_died(tmp_path, mock_body: str, mem_available_kb: int = 99999999) -> int:
    """Backwards-compatible shim for the original tests: just server_died."""
    return _run_loop(tmp_path, mock_body, mem_available_kb)["server_died"]


def _extract_function(name: str) -> str:
    """A whole shell function, verbatim, from run-fuzzer.sh."""
    text = open(_RUN_FUZZER, encoding="utf-8").read()
    m = re.search(rf"\nfunction {re.escape(name)}\n\{{\n(.*?)\n\}}\n", text, re.DOTALL)
    assert m, f"function {name!r} not found in run-fuzzer.sh"
    return f"function {name}\n{{\n{m.group(1)}\n}}\n"


def _extract_exit_trap() -> str:
    """The EXIT trap installation line, verbatim."""
    text = open(_RUN_FUZZER, encoding="utf-8").read()
    m = re.search(r"\n(trap '[^']*' EXIT)\n", text)
    assert m, "EXIT trap installation not found in run-fuzzer.sh"
    return m.group(1)


def _run_readability_trap(
    tmp_path,
    body: str,
    marker: "str | None",
    break_first_handler: bool = False,
    with_core: bool = False,
):
    """Run the real EXIT trap (with the real readability function) over a fixture.

    Seeds a workspace with a root-written-style 0640 sanitizer report plus the
    usual logs, installs run-fuzzer.sh's ACTUAL trap line and its ACTUAL
    `make_artifacts_host_readable` / `collect_sanitizer_reports` bodies, then runs
    `body` under production's `set -euo pipefail`. `marker` selects which failure
    marker exists (None = a healthy run). `break_first_handler` redefines the
    first trap handler to fail, which is how a `set -e` trap body drops the
    handlers after it. `with_core` also seeds a 0640 `core.31337`, the way a
    kernel-written core from the crashing root process looks.
    """
    ws = tmp_path / "ws"
    ws.mkdir()
    for name in ("server.log", "stderr.log", "fuzzer.log", "status.tsv"):
        (ws / name).write_text("x\n", encoding="utf-8")
    san = ws / "sanitizer.log.4242"
    san.write_text(
        "==4242==ERROR: AddressSanitizer: heap-use-after-free\n", encoding="utf-8"
    )
    san.chmod(0o640)
    (ws / "server.log").chmod(0o640)
    if with_core:
        core = ws / "core.31337"
        core.write_bytes(b"FAKECORE")
        core.chmod(0o640)
    if marker:
        (ws / marker).write_text("probes=60 tier=patient\n", encoding="utf-8")

    script = (
        "set -x\nset -e\nset -u\nset -o pipefail\n"
        f'cd "{ws}"\n'
        f'SANITIZER_LOG_BASE="{ws}/sanitizer.log"\n'
        + _extract_function("collect_sanitizer_reports")
        + _extract_function("make_artifacts_host_readable")
        + _extract_exit_trap()
        + "\n"
        + (
            "collect_sanitizer_reports() { echo BROKEN_HANDLER; return 3; }\n"
            if break_first_handler
            else ""
        )
        + body
        + "\n"
    )
    out = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60
    )
    modes = (
        stat.S_IMODE(san.stat().st_mode),
        stat.S_IMODE((ws / "server.log").stat().st_mode),
    )
    if with_core:
        return out, modes, stat.S_IMODE((ws / "core.31337").stat().st_mode)
    return out, modes


def test_readability_survives_an_abort_between_marker_and_end(tmp_path):
    # The reported gap: `set -e` can abort after the marker is written but before
    # the end of `fuzz` (a failing `zstd core.*` on a nearly-full box), and the
    # Python side then skips its own ownership repair because a marker exists.
    # Running the chmod from the EXIT trap covers that path.
    out, (san_mode, log_mode) = _run_readability_trap(
        tmp_path,
        "echo 'about to fail the way a failing zstd would'\nfalse\necho NEVER_REACHED",
        marker="server_memory_stuck.txt",
    )
    assert out.returncode == 1, out.stdout + out.stderr
    assert "NEVER_REACHED" not in out.stdout
    assert san_mode == 0o644, oct(san_mode)
    assert log_mode == 0o644, oct(log_mode)


def test_readability_runs_on_a_normal_failed_exit(tmp_path):
    # The ordinary abnormal path (no abort) must keep working, and the trap must
    # not swallow the script's exit code.
    out, (san_mode, _) = _run_readability_trap(
        tmp_path, "exit 137", marker="harness_watchdog.txt"
    )
    assert out.returncode == 137, out.stdout + out.stderr
    assert san_mode == 0o644, oct(san_mode)


def test_readability_is_skipped_on_a_healthy_run(tmp_path):
    # No marker and no watchdog: a healthy run must not chmod anything (the Python
    # side runs the real ownership repair for it instead), and the gate must not
    # turn a clean exit into a failure under `set -e`.
    out, (san_mode, _) = _run_readability_trap(tmp_path, "echo MAIN_OK", marker=None)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "MAIN_OK" in out.stdout
    assert san_mode == 0o640, oct(san_mode)


def test_readability_survives_a_failing_earlier_trap_handler(tmp_path):
    # The trap body itself runs under `set -e`, so an unguarded first handler
    # returning non-zero would abort the trap and skip the chmod -- silently
    # restoring the very gap the trap was added to close. Each handler is
    # `||:`-guarded, and the guard must not swallow the script's exit code.
    out, (san_mode, log_mode) = _run_readability_trap(
        tmp_path,
        "exit 137",
        marker="server_memory_stuck.txt",
        break_first_handler=True,
    )
    assert "BROKEN_HANDLER" in out.stdout + out.stderr
    assert out.returncode == 137, out.stdout + out.stderr
    assert san_mode == 0o644, oct(san_mode)
    assert log_mode == 0o644, oct(log_mode)


def test_readability_covers_a_root_owned_core(tmp_path):
    # A kernel-written core belongs to the crashing root process, so it is 0640 and
    # the uploading host user cannot read it. The host-side collector reads it to
    # compress+encrypt it and RAISES on `zstd: ... Permission denied`, which aborts
    # the job after classification and discards the whole Result -- including the
    # classified marker/watchdog. The in-container chmod must cover core.* too.
    out, (san_mode, _), core_mode = _run_readability_trap(
        tmp_path,
        "exit 137",
        marker="server_memory_stuck.txt",
        with_core=True,
    )
    assert out.returncode == 137, out.stdout + out.stderr
    assert san_mode == 0o644, oct(san_mode)
    assert core_mode == 0o644, oct(core_mode)


def test_readability_leaves_a_core_alone_on_a_healthy_run(tmp_path):
    # No marker/watchdog: the Python side runs the real ownership repair, so the
    # in-container chmod must stay a no-op and must not fail the run under `set -e`
    # (`core.*` is a literal when no core exists, hence the 2>/dev/null ||: guard).
    out, (san_mode, _), core_mode = _run_readability_trap(
        tmp_path, "echo MAIN_OK", marker=None, with_core=True
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert "MAIN_OK" in out.stdout
    assert san_mode == 0o640, oct(san_mode)
    assert core_mode == 0o640, oct(core_mode)


def test_exit_trap_runs_report_collection_before_the_chmod():
    # collect_sanitizer_reports appends the raw reports into stderr.log/server.log,
    # so it must run BEFORE the chmod or those two files stay unreadable.
    trap_line = _extract_exit_trap()
    assert "collect_sanitizer_reports" in trap_line, trap_line
    assert "make_artifacts_host_readable" in trap_line, trap_line
    assert trap_line.index("collect_sanitizer_reports") < trap_line.index(
        "make_artifacts_host_readable"
    ), trap_line


def test_fuzz_does_not_chmod_artifacts_inline():
    # The readability fix must live only in the trap function: an inline copy at
    # the end of `fuzz` would silently re-introduce the abort-skips-it gap.
    assert "chmod -R a+r" not in _extract_function("fuzz")


def _run_block(tmp_path, name: str, preamble: str, epilogue: str):
    """Run a marked run-fuzzer.sh block verbatim with real children.

    `sleep` is mocked to /bin/sleep 0.01 (fast, but NOT a no-op: a real child's
    exit must be observable before a deadline counter can spin to its limit).
    kill/wait stay real -- the children are real processes.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    sleep = bindir / "sleep"
    sleep.write_text("#!/bin/bash\nexec /bin/sleep 0.01\n", encoding="utf-8")
    sleep.chmod(sleep.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    script = (
        # Same options as run-fuzzer.sh, so an unguarded failure inside an
        # extracted block aborts here exactly as it would in production.
        "set -euo pipefail\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n'
        + preamble
        + "\n"
        + _extract_block(name)
        + "\n"
        + epilogue
        + "\n"
    )
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60
    )


_TIMEOUT_ERR = (
    'echo "Code: 209. DB::NetException: Timeout exceeded while receiving data '
    'from server. Waited for 5 seconds, timeout is 5 seconds." >&2; exit 1'
)
_REFUSED_ERR = (
    'echo "Code: 210. DB::NetException: Connection refused (localhost:9000). '
    '(NETWORK_ERROR)" >&2; exit 1'
)
# The real post-fuzz probe line from the #110074 job log: the server-global
# tracker rejects the ~0-byte idle probe. Only this "(total)" form counts toward
# the memory-stuck detection.
_TOTAL_MEM_ERR = (
    'echo "Code: 241. DB::Exception: (total) memory limit exceeded: would use '
    "55.80 GiB (attempt to allocate chunk of 0.00 B), current RSS: 57.00 GiB, "
    'maximum: 46.32 GiB. (MEMORY_LIMIT_EXCEEDED)" >&2; exit 1'
)
# A per-query/user 241 (NOT the global "(total)" form): tolerated as before, and
# it must NOT count toward memory-stuck detection.
_USER_MEM_ERR = (
    'echo "Code: 241. DB::Exception: Memory limit (for query) exceeded: would '
    'use 9.31 GiB. (MEMORY_LIMIT_EXCEEDED)" >&2; exit 1'
)
_TOO_MANY_ERR = (
    'echo "Code: 202. DB::Exception: Too many simultaneous queries. '
    '(TOO_MANY_SIMULTANEOUS_QUERIES)" >&2; exit 1'
)
_LOW_MEM_KB = 2 * 1024 * 1024  # 2 GiB free -> below the 4 GiB fast-tier floor
_AMPLE_MEM_KB = 40 * 1024 * 1024  # 40 GiB free -> fast tier cannot fire
# The production fast-tier floor is `mem_available_kb -lt 4194304` (4 GiB). The two
# constants above sit 2 GiB below / 36 GiB above it, so any cutoff in that range
# passes; these adjacent values pin the boundary itself.
_FAST_TIER_FLOOR_KB = 4 * 1024 * 1024  # -lt, so exactly-at-the-floor must NOT trip
_BELOW_FAST_TIER_FLOOR_KB = _FAST_TIER_FLOOR_KB - 1024


def test_transient_timeout_then_recovers_is_not_server_died(tmp_path):
    # First 3 probes time out (alive but slow), 4th succeeds -> NOT dead.
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 3 ]]; then
            {_TIMEOUT_ERR}
        fi
        echo 1
        exit 0
        """)
    assert _server_died(tmp_path, mock) == 0


def test_connection_refused_is_server_died_immediately(tmp_path):
    # A genuinely dead server -> server_died=1 on the first probe.
    mock = f"{_REFUSED_ERR}\n"
    assert _server_died(tmp_path, mock) == 1


def test_persistent_timeout_is_eventually_server_died(tmp_path):
    # The server never answers -> a real hang -> server_died=1 once timeouts persist.
    mock = f"{_TIMEOUT_ERR}\n"
    assert _server_died(tmp_path, mock) == 1


def test_patient_tier_trips_at_60th_consecutive_total_241(tmp_path):
    # Persistent server-global 241 with AMPLE free memory -> the fast tier cannot
    # fire; the patient tier trips at exactly the 60th consecutive probe.
    res = _run_loop(tmp_path, f"{_TOTAL_MEM_ERR}\n", mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["attempts"] == 60  # tripped exactly at 60, not the 100-probe cap
    assert res["marker"] is not None
    assert "(total) memory limit exceeded" in res["marker"]
    assert "tier=patient" in res["marker"]


def test_fast_tier_trips_at_12th_when_memavailable_low(tmp_path):
    # Persistent server-global 241 AND host memory nearly exhausted -> fast tier
    # trips at the 12th consecutive probe (preserving teardown runway).
    res = _run_loop(tmp_path, f"{_TOTAL_MEM_ERR}\n", mem_available_kb=_LOW_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["attempts"] == 12
    assert "tier=fast" in res["marker"]


def test_short_total_241_burst_then_recovers_low_mem_no_marker(tmp_path):
    # 10 server-global 241s (below the fast tier's count floor of 12) then a
    # success, even under host memory pressure -> the transient spike is NOT
    # treated as stuck (the 12-count floor protects it).
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 10 ]]; then
            {_TOTAL_MEM_ERR}
        fi
        echo 1
        exit 0
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_LOW_MEM_KB)
    assert res["server_died"] == 0
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None


def test_reclaim_one_probe_below_the_fast_tier_is_not_stuck(tmp_path):
    # The tightest false-positive case for the fast tier: 11 consecutive
    # server-global 241s under host memory pressure (one below the 12 floor), then
    # a successful probe. A healthy reclaim that finishes anywhere inside ~12 s
    # must not be classified as stuck, so this brackets the threshold from below
    # while test_fast_tier_trips_at_12th_when_memavailable_low brackets it from
    # above. Evidence for the count itself, not just for the literal.
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 11 ]]; then
            {_TOTAL_MEM_ERR}
        fi
        echo 1
        exit 0
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_LOW_MEM_KB)
    assert res["server_died"] == 0, "an 11-probe reclaim must not be called dead"
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None


def test_reclaim_one_probe_below_the_patient_tier_is_not_stuck(tmp_path):
    # Same bracket for the patient tier: 59 consecutive rejections with NO host
    # pressure (so the fast tier cannot fire), then success. A slow but genuine
    # reclaim -- jemalloc decay purge, post-query release -- completing just under
    # 60 s must stay healthy.
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 59 ]]; then
            {_TOTAL_MEM_ERR}
        fi
        echo 1
        exit 0
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 0, "a 59-probe reclaim must not be called dead"
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None


def test_transient_total_241_reclaim_case_preserved(tmp_path):
    # edecdd570's reclaim tolerance: 10 (total) 241s then success -> NOT dead,
    # NO marker (ample memory).
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 10 ]]; then
            {_TOTAL_MEM_ERR}
        fi
        echo 1
        exit 0
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 0
    assert res["marker"] is None


def test_too_many_resets_the_memory_counter(tmp_path):
    # 30 (total) 241s, then one TOO_MANY (resets the counter), then persistent
    # 241s: the trip must require 60 MORE consecutive rejections after the reset,
    # not 60 total -- so memory_limit_probes is exactly 60 at the trip (the 30
    # pre-reset rejections were discarded). (`attempts` is not asserted: the
    # TOO_MANY branch's diagnostic SHOW PROCESSLIST invokes the shared mock once.)
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 30 ]]; then
            {_TOTAL_MEM_ERR}
        elif [[ "$n" -eq 31 ]]; then
            {_TOO_MANY_ERR}
        fi
        {_TOTAL_MEM_ERR}
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["memory_limit_probes"] == 60  # reset discarded the pre-reset 30


def test_non_total_241_never_trips_but_exhaustion_fails_closed(tmp_path):
    # A per-query/user 241 is never the server-global stuck form: it must NOT
    # trip the memory counter or write a marker. But a window with ZERO answered
    # probes now fails closed (server_died=1) per the r11 rule -- WITHOUT a marker
    # (memory_limit_probes stays 0 for non-total 241s).
    res = _run_loop(tmp_path, f"{_USER_MEM_ERR}\n", mem_available_kb=_LOW_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    assert res["memory_limit_probes"] == 0
    # A live-but-unanswering exhaustion records the probes-stage watchdog so the
    # graceful stop's self-inflicted "Received signal 15" is not attributed.
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_a_hard_killed_probe_records_the_probes_watchdog(tmp_path):
    # The hard `timeout -k` bound introduced for the wedged-client case kills the
    # probe itself, which leaves NO recognized database error in `err`. Without
    # routing that exit status (124 TERM / 137 KILL) to the timeout branch, it fell
    # through to the catch-all `server_died=1` with no marker and no watchdog, and
    # the graceful stop below could then be parsed as a genuine signal failure.
    #
    # `sleep` is mocked to a no-op in this harness, so a genuinely wedged client
    # cannot be simulated; reproduce what the kill leaves behind instead -- the
    # exit status `timeout` reports (137 for SIGKILL) and an empty `err`.
    res = _run_loop(tmp_path, "exit 137")
    watchdog = tmp_path / "harness_watchdog.txt"
    assert watchdog.exists(), (
        "a probe killed by the loop's own hard bound must record a probes-stage "
        "watchdog, or the graceful stop's signal line is misattributed"
    )
    contents = watchdog.read_text(encoding="utf-8")
    assert "stage=probes" in contents, contents
    assert res["server_died"] == 1, "the loop must still stop probing"
    assert res["marker"] is None, "a killed probe is not the memory-stuck state"


def test_too_many_only_exhaustion_records_probes_watchdog(tmp_path):
    # Every probe rejected with TOO_MANY_SIMULTANEOUS_QUERIES: the server is
    # demonstrably alive (it rejects), memory_limit_probes stays 0 (no marker),
    # yet zero answers -> fail closed with the probes-stage watchdog line.
    res = _run_loop(tmp_path, f"{_TOO_MANY_ERR}\n", mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    assert res["memory_limit_probes"] == 0
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_probe_stage_deadline_bounds_the_loop(tmp_path):
    # Per-call bounds still allow ~16 s per TOO_MANY iteration; the aggregate
    # stage deadline is what actually protects the external-cancel margin. With
    # the seam forced to 0 the deadline fires before the first probe: zero
    # attempts, and the exit flows into the fail-closed exhaustion path (no
    # marker -- not memory-dominated -- but a probes-stage watchdog).
    res = _run_loop(
        tmp_path,
        f"{_TOO_MANY_ERR}\n",
        mem_available_kb=_AMPLE_MEM_KB,
        extra_env={"PROBE_STAGE_DEADLINE_SECONDS": "0"},
    )
    assert res["attempts"] == 0
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_probe_stage_deadline_bounds_a_running_loop(tmp_path):
    # The zero-second test above cannot tell an in-loop guard from one hoisted
    # before the loop (both exit with zero attempts). Give the mock real
    # wall-clock cost and a nonzero deadline: the loop must START, serve a few
    # probes, then be cut off well short of the 100-probe cap -- which only a
    # per-iteration check can do. Timings are deliberately loose (the assertion
    # is "bounded and well under the cap", not an exact count) so the test is not
    # itself flaky on a loaded runner.
    mock = textwrap.dedent(f"""\
        /bin/sleep 0.4
        {_TOO_MANY_ERR}
        """)
    res = _run_loop(
        tmp_path,
        mock,
        mem_available_kb=_AMPLE_MEM_KB,
        extra_env={"PROBE_STAGE_DEADLINE_SECONDS": "2"},
    )
    # The loop ran (unlike the deadline-0 case) but was cut off far short of the
    # 100-probe cap: a guard hoisted out of the loop would let all 100 through.
    assert 1 <= res["attempts"] < 50
    # The deadline exit is a zero-answer exit, so it fails closed exactly like an
    # ordinary exhaustion: dead by fiat on a live server -> probes-stage watchdog.
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_probe_stage_does_not_overrun_its_deadline_at_the_boundary(tmp_path):
    # The attempt-count test above proves the stage STOPS early, not that it stops
    # WITHIN its advertised window -- so it stayed green while the stage overran.
    # The overrun lived in the arithmetic at the boundary: with 1s of budget left,
    # reserving the 1s kill grace leaves 0, which a `< 1` clamp restored to a 1s
    # TERM timeout, so `timeout -k 1 1` could spend 2s (measured 2.013s) on a
    # TERM-ignoring client -- plus a 1s cool-down -- all past expiry.
    #
    # A wedged client that ignores SIGTERM is the shape that exposes it. `sleep` is
    # mocked no-op, so every second measured here is a real bounded client call.
    #
    # The deadline is 1s ON PURPOSE: that is the only value where the two forms
    # diverge, and a looser slack makes this test VACUOUS. Measured over 3 runs
    # each -- clamped (pre-fix): 2.01s at deadline 1, and IDENTICAL to the fixed
    # form (2.01/3.01/4.01s) at deadlines 2/3/4, because the bad clamp is only
    # reachable when exactly 1s of budget remains. Fixed form at deadline 1: 0.00s.
    victim = "trap '' TERM\n/bin/sleep 30\n"
    deadline = 1
    res = _run_loop(
        tmp_path,
        victim,
        mem_available_kb=_AMPLE_MEM_KB,
        extra_env={"PROBE_STAGE_DEADLINE_SECONDS": str(deadline)},
    )
    # Tight by necessity (see above), but with a wide margin against the two
    # measured populations: 0.00s (fixed) vs 2.01s (clamped) around a 1.5s line.
    assert res["elapsed"] <= deadline + 0.5, (
        f"probe stage took {res['elapsed']:.2f}s against a {deadline}s deadline: the "
        "aggregate bound is not being enforced at the boundary"
    )
    # A hard-killed probe is a timeout (124/137), so it must fail closed through the
    # probes-stage watchdog, not the catch-all branch.
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes" in wd


def test_probe_stage_refuses_a_budget_too_small_for_a_bounded_call(tmp_path):
    # The boundary fix must REFUSE to launch rather than clamp a too-small budget up
    # to a launchable one -- clamping is exactly how the bound got exceeded instead
    # of enforced. `timeout -k G S` costs S+G, so a budget under TERM+grace cannot
    # express any bound: the stage has to end. Pinned via the log line so a silent
    # return to clamping is caught even if the timing assertion above is loose.
    victim = "trap '' TERM\n/bin/sleep 30\n"
    res = _run_loop(
        tmp_path,
        victim,
        mem_available_kb=_AMPLE_MEM_KB,
        extra_env={"PROBE_STAGE_DEADLINE_SECONDS": "1"},
    )
    assert "cannot hold a bounded probe" in res["stdout"], (
        "a probe-stage budget smaller than TERM+grace must end the stage, not be "
        f"clamped up to a launchable value:\n{res['stdout']}"
    )
    # Ending the stage is still a zero-answer exit: fail closed.
    assert res["server_died"] == 1
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_diagnostic_is_skipped_rather_than_run_unbounded(tmp_path):
    # The diagnostic has the same boundary hazard as the probe, with a worse
    # failure: clamping a 1s budget down through the reserved kill grace yields
    # `timeout -k 1 0`, and `timeout 0` means NO LIMIT AT ALL (measured: a 4s child
    # under `timeout -k 1 0` ran the full 4s, rc=0). So the very call this PR wraps
    # to bound would become unbounded again -- on a thrashing server, holding the
    # stage and a query slot toward the external cancel.
    #
    # A TOO_MANY probe costing ~0.9s against a 2s deadline leaves the diagnostic
    # exactly 1s (verified by sweeping deadlines 2-7 x probe costs 0-4s: the
    # remaining budget seen at the diagnostic takes every value 1..7, so 1 is
    # reachable, not hypothetical). The diagnostic mock ignores SIGTERM and sleeps
    # well past the whole stage, so an unbounded call cannot hide.
    mock = textwrap.dedent(f"""\
        if [[ "$*" == *PROCESSLIST* ]]; then
            trap '' TERM
            /bin/sleep 25
            exit 0
        fi
        /bin/sleep 0.9
        {_TOO_MANY_ERR}
        """)
    res = _run_loop(
        tmp_path,
        mock,
        mem_available_kb=_AMPLE_MEM_KB,
        extra_env={"PROBE_STAGE_DEADLINE_SECONDS": "2"},
    )
    # Skipped, so the 25s wedged diagnostic is never entered: the stage stays inside
    # its window instead of running for the mock's full sleep.
    assert "skipping the SHOW PROCESSLIST diagnostic" in res["stdout"], (
        "a diagnostic budget too small to express a bound must be skipped, not run "
        f"with an unbounded `timeout 0`:\n{res['stdout']}"
    )
    assert res["elapsed"] < 10, (
        f"stage took {res['elapsed']:.1f}s: the diagnostic ran unbounded against a "
        "2s probe-stage deadline"
    )


def test_non_total_241_then_success_is_healthy(tmp_path):
    # Tolerance preserved within the window: 5 user-level 241s then success.
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 5 ]]; then
            {_USER_MEM_ERR}
        fi
        echo 1
        exit 0
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_LOW_MEM_KB)
    assert res["server_died"] == 0
    assert res["marker"] is None


def test_timeout_does_not_reset_the_memory_counter(tmp_path):
    # 30 (total) 241s, one probe timeout (must NOT reset the memory counter),
    # then more 241s: 30 + 1 timeout + 30 more 241s trips at the 60th consecutive
    # memory probe, which is overall attempt 61.
    mock = textwrap.dedent(f"""\
        if [[ "$n" -le 30 ]]; then
            {_TOTAL_MEM_ERR}
        elif [[ "$n" -eq 31 ]]; then
            {_TIMEOUT_ERR}
        fi
        {_TOTAL_MEM_ERR}
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    # 30 memory + 1 timeout + 30 more memory = 60th consecutive memory probe at
    # overall attempt 61.
    assert res["memory_limit_probes"] == 60
    assert res["attempts"] == 61


def test_alternating_total_241_and_timeout_exhausts_fail_closed(tmp_path):
    # Strict alternation of (total) 241 and timeout for all 100 probes reaches
    # neither the 60-count nor the 12-timeout threshold, yet answers zero probes.
    # Fail-closed: server_died=1 AND a marker (window is memory-dominated, >=30).
    mock = textwrap.dedent(f"""\
        if (( n % 2 == 1 )); then
            {_TOTAL_MEM_ERR}
        else
            {_TIMEOUT_ERR}
        fi
        """)
    res = _run_loop(tmp_path, mock, mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["memory_limit_probes"] >= 30
    assert res["attempts"] == 100  # exhausted the loop
    # Memory-dominated exhaustion carries ONLY the marker (which flips
    # self_killed_server); no probes-stage watchdog line is written.
    assert not (tmp_path / "harness_watchdog.txt").exists()


def test_fast_tier_trips_just_below_the_memavailable_floor(tmp_path):
    # Boundary companion to test_fast_tier_trips_at_12th_when_memavailable_low:
    # 1 MiB below the 4 GiB floor still takes the aggressive 12-probe tier.
    res = _run_loop(
        tmp_path, f"{_TOTAL_MEM_ERR}\n", mem_available_kb=_BELOW_FAST_TIER_FLOOR_KB
    )
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["attempts"] == 12
    assert "tier=fast" in res["marker"]


def test_fast_tier_does_not_trip_at_the_memavailable_floor(tmp_path):
    # Exactly AT the floor: `-lt` makes this the non-firing side, so the aggressive
    # 12-probe tier must not fire and the patient 60-probe tier trips instead.
    # Raising the floor (e.g. to 32 GiB) would fire the fast tier on hosts with
    # ample free memory -> mass false memory-stuck FAILs.
    res = _run_loop(
        tmp_path, f"{_TOTAL_MEM_ERR}\n", mem_available_kb=_FAST_TIER_FLOOR_KB
    )
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["attempts"] == 60
    assert "tier=patient" in res["marker"]


# Exhaustion-boundary mocks: a per-user 241 prefix (tolerated, resets both counters)
# then strict (total)-241 / timeout alternation, so `timeouts` never reaches 12 and
# the memory counter never reaches 60 -- the window exhausts with zero answered
# probes at exactly (100 - start + 1) / 2 rounded up memory rejections. start=44
# yields 29 (below the -ge 30 dominance cutoff), start=42 yields 30 (at it). Both
# verified empirically; see the assertions on memory_limit_probes.
def _exhaustion_mock(start: int) -> str:
    return textwrap.dedent(f"""\
        if (( n < {start} )); then
            {_USER_MEM_ERR}
        elif (( (n - {start}) % 2 == 0 )); then
            {_TOTAL_MEM_ERR}
        else
            {_TIMEOUT_ERR}
        fi
        """)


def test_exhaustion_marker_requires_thirty_memory_rejections(tmp_path):
    # 29 server-global rejections is BELOW the -ge 30 dominance cutoff: the window
    # still fails closed (server_died=1) but the failure is NOT attributed to
    # memory -- it records a probes-stage watchdog instead. Lowering the cutoff
    # would let a single stray (total) 241 in an otherwise TOO_MANY-dominated
    # window mint a memory-stuck marker and misattribute the failure.
    res = _run_loop(tmp_path, _exhaustion_mock(44), mem_available_kb=_AMPLE_MEM_KB)
    assert res["memory_limit_probes"] == 29
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=zero_answered_probes" in wd


def test_exhaustion_marker_at_exactly_thirty_memory_rejections(tmp_path):
    # The -ge 30 counterpart: one more rejection makes the window
    # memory-dominated, so the marker is written with tier=exhaustion.
    res = _run_loop(tmp_path, _exhaustion_mock(42), mem_available_kb=_AMPLE_MEM_KB)
    assert res["memory_limit_probes"] == 30
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 1
    assert res["marker"] is not None
    assert "tier=exhaustion" in res["marker"]


def test_persistent_timeout_exit_records_probes_watchdog_no_marker(tmp_path):
    # The persistent-timeout hang: server_died=1 via timeouts_max as before, and
    # NO memory marker (memory_limit_probes stays 0). The 12-consecutive-timeouts
    # exit declares a live-but-unanswering server dead (a dead server refuses, it
    # does not time out), so it now records the harness state -- otherwise the
    # graceful stop's self-inflicted "Received signal 15" would be scraped as a
    # bogus crash FAIL.
    res = _run_loop(tmp_path, f"{_TIMEOUT_ERR}\n", mem_available_kb=_AMPLE_MEM_KB)
    assert res["server_died"] == 1
    assert res["server_memory_stuck"] == 0
    assert res["marker"] is None
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=probes reason=persistent_probe_timeouts timeouts=12" in wd


def test_every_in_loop_client_invocation_is_bounded():
    # The probe loop runs inside the post-budget window this PR promises to
    # bound. An in-loop clickhouse-client call without an explicit bound
    # inherits the client's 300 s receive_timeout default and can hold the
    # stage (and a query slot) toward the external cancellation ceiling.
    for line in _extract_loop().splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "clickhouse-client" not in stripped:
            continue
        # The bound may be a literal or the capped variable holding it; resolve a
        # variable to its default assignment in the loop so the value is still
        # pinned exactly.
        m_recv = re.search(r"--receive_timeout=\$?(\w+)", stripped)
        m_wall = re.search(
            r"(?:^|\s)timeout\s+(?:-k\s+\S+\s+)?\$?[({]*(\w+)", stripped
        )
        assert m_recv or m_wall, f"unbounded in-loop clickhouse-client call: {stripped}"
        # Every in-loop call needs a HARD WALL-CLOCK bound. `--receive_timeout` is a
        # client setting that only bounds waiting for server data, so a client wedged
        # before that (connect, TLS, DNS) is unbounded without a `timeout` wrapper.
        # And plain `timeout N` sends SIGTERM then waits indefinitely if the child
        # ignores it (measured: 30 s for a TERM-ignoring child under `timeout 2`), so
        # --kill-after is what makes it hard.
        m_sched = re.search(r"(?:^|\s)timeout\s+-k\s+(\d+)\s+\$?(\w+)", stripped)
        assert m_sched, (
            "every in-loop clickhouse-client call needs a hard wall-clock bound "
            f"(`timeout -k <grace> <secs>`): {stripped}"
        )
        token = (m_recv or m_wall).group(1)
        if token.isdigit():
            bound = int(token)
        else:
            loop = _extract_loop()
            m_default = re.search(
                rf"^\s*{re.escape(token)}=(\d+)\s*$", loop, re.MULTILINE
            )
            if m_default:
                bound = int(m_default.group(1))
            else:
                # A bound may be DERIVED from another (the probe reserves its kill
                # grace out of the wall-clock budget), so follow one hop.
                m_derived = re.search(
                    rf"^\s*{re.escape(token)}=\$\(\(\s*(\w+)\s*-\s*(\d+)\s*\)\)\s*$",
                    loop,
                    re.MULTILINE,
                )
                assert m_derived, (
                    f"the bound `{token}` has no literal or derived default in the "
                    f"loop: {stripped}"
                )
                parent, subtracted = m_derived.group(1), int(m_derived.group(2))
                m_parent = re.search(
                    rf"^\s*{re.escape(parent)}=(\d+)\s*$", loop, re.MULTILINE
                )
                assert m_parent, f"`{parent}` has no literal default: {stripped}"
                bound = int(m_parent.group(1)) - subtracted
                token = parent  # the capped quantity is the parent
            # A variable bound must be capped by the remaining stage budget, or it
            # can outlive the aggregate deadline it is meant to respect. Check the
            # cap on ITS OWN line (this loop only sees the invocation line).
            assert re.search(
                rf"^\s*\(\(\s*{re.escape(token)}\s*>\s*probe_stage_remaining\s*\)\)"
                rf"\s*&&\s*{re.escape(token)}=\$probe_stage_remaining\s*$",
                loop,
                re.MULTILINE,
            ), (
                f"`{token}` is not capped by the remaining probe-stage budget: "
                f"{stripped}"
            )
            # The budget must be RECOMPUTED for each bound, not reused: a stale
            # value lets the diagnostic run past the aggregate deadline after the
            # probe has already spent most of it. Either spelling counts as a
            # recompute -- the inline subtraction, or a fresh call to the
            # `probe_stage_left` helper that performs the same subtraction (pinned
            # to actually do so by the assertion below).
            recomputes = len(
                re.findall(
                    r"probe_stage_remaining=(?:\$\(\(\s*probe_stage_deadline\s*-"
                    r"|\$\(probe_stage_left\))",
                    loop,
                )
            )
            capped = len(re.findall(r"=\$probe_stage_remaining\s*$", loop, re.MULTILINE))
            assert recomputes >= capped, (
                f"{capped} bounds are capped but the budget is only recomputed "
                f"{recomputes} times: a stale remaining value overruns the deadline"
            )
            # If the recompute goes through the helper, the helper must really derive
            # from the deadline and the stage start -- otherwise the pin above could
            # be satisfied by a function returning a constant.
            if "probe_stage_remaining=$(probe_stage_left)" in loop:
                assert re.search(
                    r"probe_stage_left\(\)\s*\{\s*echo\s+\$\(\(\s*probe_stage_deadline"
                    r"\s*-\s*\(\s*SECONDS\s*-\s*probe_stage_started_at\s*\)\s*\)\)\s*;?\s*\}",
                    loop,
                ), "`probe_stage_left` does not derive the remaining budget from the deadline"
        # Pin each shape to ITS OWN limit rather than a shared 60 s ceiling: the
        # liveness probe runs every iteration (5 s), while the diagnostics run once
        # per stuck detection (10 s). A generic ceiling lets a single diagnostic
        # grow to a fifth of the whole probe window while staying green.
        # `timeout -k G S` can take S+G, so the grace must be RESERVED inside the
        # budget, not added on top of it.
        grace = int(m_sched.group(1))
        assert grace >= 1, f"the kill grace must be at least 1s: {stripped}"
        assert bound + grace <= 5 or "PROCESSLIST" in stripped, (
            f"the probe's TERM+KILL schedule ({bound}+{grace}s) exceeds its 5s "
            f"budget: {stripped}"
        )
        if m_recv:
            # The published bound is the WHOLE schedule: 4s to TERM plus the 1s kill
            # grace = the 5s window, so a probe cannot outlive it.
            assert bound + grace == 5, (
                f"the liveness probe must stay bounded at 5s total, got "
                f"{bound}+{grace}s: {stripped}"
            )
        else:
            assert bound + grace == 10, (
                f"in-loop diagnostics must stay bounded at 10s total, got "
                f"{bound}+{grace}s: {stripped}"
            )


def test_in_loop_bounds_are_hard_against_a_term_ignoring_child(tmp_path):
    # The structural check above requires `timeout -k`; this measures WHY. A child
    # that ignores SIGTERM keeps plain `timeout N` waiting indefinitely, so the
    # published probe-stage window would not hold. Runs the two forms against a
    # real TERM-ignoring child and compares elapsed wall-clock time.
    victim = tmp_path / "ignores-term.sh"
    victim.write_text("#!/bin/bash\ntrap '' TERM\nsleep 20\n", encoding="utf-8")
    victim.chmod(0o755)

    def elapsed(command):
        started = time.monotonic()
        subprocess.run(command, shell=True, capture_output=True)
        return time.monotonic() - started

    soft = elapsed(f"timeout 1 {victim}")
    hard = elapsed(f"timeout -k 1 1 {victim}")
    assert soft > 5, (
        f"expected plain `timeout` to hang on a TERM-ignoring child, took {soft:.1f}s "
        "(if this ever stops holding, the --kill-after requirement can be relaxed)"
    )
    assert hard < 5, (
        f"`timeout -k` did not enforce a hard bound: {hard:.1f}s"
    )


def test_probe_stage_deadline_default_is_bounded():
    # The aggregate deadline is a stated contract of this change, so pin the
    # production default: a silent bump would remove the external-cancel margin
    # while every behavioral test (which overrides the seam) stayed green. The
    # floor keeps it above the loop's own detectors (fast ~13 s, patient ~60 s,
    # timeout declare ~72 s) so none of them can be preempted.
    m = re.search(
        r'probe_stage_deadline="\$\{PROBE_STAGE_DEADLINE_SECONDS:-(\d+)\}"',
        _extract_loop(),
    )
    assert m, "probe-stage deadline default not found in the extracted loop"
    default = int(m.group(1))
    # Pinned exactly: the PR body publishes 300 s as the aggregate probe window,
    # and a range down to 120 lets that promised recovery margin be more than
    # halved while every behavioral test (which overrides the seam) stays green.
    assert default == 300, f"probe stage deadline default out of range: {default}s"


def test_every_in_loop_wait_is_charged_against_the_stage_budget():
    # Structural, because it CANNOT be behavioral: `_run_loop` mocks `sleep` to a
    # no-op (otherwise a 60-probe test would take a minute), so a cool-down that
    # ignores the stage budget costs zero measurable time and every behavioral test
    # stays green -- verified by mutation (an unconditional `probe_stage_sleep` body
    # passed all 51 tests in this file).
    #
    # In production each of these waits is a real second spent past the advertised
    # window. So pin the shape: inside the probe loop the cool-down goes through the
    # budget-guarded helper, never a bare `sleep`.
    loop = _extract_loop()
    body_end = loop.index("for _ in {1..100}")
    in_loop = loop[body_end:]
    bare = [
        line.strip()
        for line in in_loop.splitlines()
        if re.match(r"^\s*sleep\s+\S+\s*$", line)
    ]
    assert not bare, (
        "in-loop waits must be charged against the probe-stage budget via "
        f"`probe_stage_sleep`, found bare: {bare}"
    )
    # And the helper must actually consult the budget -- otherwise the pin above is
    # satisfied by a helper that always sleeps.
    assert re.search(
        r"probe_stage_sleep\(\)\s*\{\s*if\s*\(\(\s*\$\(probe_stage_left\)\s*>\s*0\s*\)\)",
        loop,
    ), "`probe_stage_sleep` does not skip the wait once the stage budget is spent"


_POLLED_BLOCKS = (
    "server-liveness probe loop",
    "fuzzer-client reap poll",
    "server teardown poll",
)
# A one-shot guard, not a poll: it is exercised behaviorally (below) and stays
# out of the cadence / counter-step pins, which only apply to polling blocks.
_STUCK_KILL_BLOCK = "memory-stuck server kill"


def test_polled_blocks_use_a_one_second_cadence():
    # Every deadline in these blocks is a counter incremented next to a `sleep`,
    # so the counters only mean seconds while the cadence is 1 s. The behavioral
    # tests mock `sleep` ignoring its argument, so a cadence bump (sleep 1 ->
    # sleep 10, i.e. teardown 3 min -> 30 min) keeps them green while blowing the
    # external-cancel margin. Only the bare `sleep N` form is matched, so the
    # in-loop `timeout 10 clickhouse-client ...` diagnostic is not swept in.
    for name in _POLLED_BLOCKS:
        block = _extract_block(name)
        # A block may spend its cadence through a guarded helper instead of a bare
        # statement (the probe loop skips the wait once the stage budget is spent),
        # so match the `sleep N` inside a one-line function body too. Both forms are
        # still pinned to N == 1.
        durations = re.findall(
            r"^\s*sleep\s+(\S+)\s*$|\bthen\s+sleep\s+(\S+)\s*;\s*fi", block, re.MULTILINE
        )
        durations = [a or b for a, b in durations]
        assert durations, f"no `sleep` statement found in block {name!r}"
        for d in durations:
            assert d == "1", f"block {name!r} polls at `sleep {d}`, not the 1s cadence"


def test_reap_and_teardown_deadlines_are_bounded():
    # Companions to test_probe_stage_deadline_default_is_bounded: pin the reap and
    # teardown production limits, which the behavioral escalation tests cannot see
    # (they only prove escalation eventually happens, not its magnitude).
    reap = _extract_block("fuzzer-client reap poll")
    m = re.search(
        r"reap_deadline=\$\(\(\s*remaining_seconds\s*\+\s*(\d+)\s*\+\s*(\d+)\s*\)\)",
        reap,
    )
    assert m, "reap deadline slack not found in the reap poll block"
    kill_after, abandon = int(m.group(1)), int(m.group(2))
    slack = kill_after + abandon
    # The 300 mirrors the client `timeout --kill-after=5m` grace (kept
    # deliberately, see 4617bf64dda00); the 90 is the post-SIGKILL abandon
    # allowance (a client not gone 90 s after SIGKILL is unreapable).
    #
    # These are pinned EXACTLY, not range-checked: the post-budget stages are
    # summed against the observed cancellation ceiling by
    # test_core_collection_deadline_is_a_small_slice_of_the_budget, and a range
    # lets two stages each grow within their own bound while the SUM leaves that
    # ceiling. Widening either is a deliberate edit of the value and of the
    # margin argument that depends on it.
    assert (kill_after, abandon) == (300, 90), (
        f"reap deadline slack changed: {kill_after}+{abandon}s, expected 300+90s"
    )
    assert slack == 390, f"reap deadline slack out of range: {slack}s"

    teardown = _extract_block("server teardown poll")
    m = re.search(r"^teardown_deadline=(\d+)$", teardown, re.MULTILINE)
    assert m, "teardown_deadline not found in the teardown poll block"
    deadline = int(m.group(1))
    # Healthy teardowns are ~10-15 s. Pinned exactly for the same reason as the
    # reap slack above: this value is one term of the post-budget sum checked
    # against the observed cancellation ceiling.
    assert deadline == 180, f"teardown deadline out of range: {deadline}s"

    m = re.search(r"for _ in \{1\.\.(\d+)\}", teardown)
    assert m, "post-SIGKILL grace loop not found in the teardown poll block"
    grace = int(m.group(1))
    assert grace == 10, f"post-SIGKILL grace changed: {grace}s, expected 10s"


def test_poll_counters_advance_one_unit_per_iteration():
    # A poll's wall-clock bound is `deadline / step x cadence`. The cadence is
    # pinned by test_polled_blocks_use_a_one_second_cadence and the deadline
    # constants by test_reap_and_teardown_deadlines_are_bounded; this pins the
    # third factor, which both of those miss. Stepping by 10 instead of 1 shrinks
    # the reap bound from 3990 s to 399 s (it would SIGKILL a healthy 60-minute
    # fuzz client and write a bogus stage=reap watchdog) and the teardown bound
    # from 180 s to 18 s (at/below the 10-15 s healthy stop this block documents).
    # Flipping the comparison operator moves when escalation fires at all.
    for name, var, dl in (
        ("fuzzer-client reap poll", "reap_waited", "reap_deadline"),
        ("server teardown poll", "teardown_waited", "teardown_deadline"),
    ):
        block = _extract_block(name)
        m = re.search(
            rf"^\s*{var}=\$\(\(\s*{var}\s*\+\s*(\d+)\s*\)\)\s*$", block, re.MULTILINE
        )
        assert m, f"no `{var}` increment found in block {name!r}"
        assert m.group(1) == "1", (
            f"block {name!r}: {var} steps by {m.group(1)}, not 1, "
            f"so {dl} no longer counts seconds"
        )
        assert re.search(rf'\[\[\s*"\${var}"\s*-ge\s*"\${dl}"\s*\]\]', block), (
            f"block {name!r}: the `{var} -ge {dl}` escalation comparison is "
            f"missing or its direction changed"
        )
        assert re.search(
            rf"^\s*{var}=0\s*$", block, re.MULTILINE
        ), f"block {name!r}: {var} is not zero-initialised before the poll"


def test_memory_stuck_thresholds_are_pinned():
    # Companion to the reap/teardown deadline pins: the behavioral tier tests
    # bracket these numbers but cannot see their exact value (the memory constants
    # sit 2 GiB below / 36 GiB above the floor, and the exhaustion tests below use
    # 0 vs >=50 rejections), so a moved threshold stays green while changing
    # production behavior. The boundary tests added alongside this one close the
    # behavioral half; this pins the literals.
    loop = _extract_loop()

    m = re.search(
        r'\[\[\s*"\$memory_limit_probes"\s*-ge\s*(\d+)\s*&&\s*'
        r'"\$mem_available_kb"\s*-lt\s*(\d+)\s*\]\]',
        loop,
    )
    assert m, "the fast-tier conjunct was not found in the probe loop"
    assert m.group(1) == "12", (
        f"fast tier trips at {m.group(1)} rejections, not 12: raising it delays the "
        f"stuck kill past the teardown/upload runway, lowering it kills servers "
        f"that are still reclaiming"
    )
    assert m.group(2) == "4194304", (
        f"fast-tier MemAvailable floor is {m.group(2)} kB, not 4194304 (4 GiB): "
        f"raising it fires the aggressive 12-probe tier on hosts with ample free "
        f"memory (mass false memory-stuck FAILs), lowering it loses the runway the "
        f"stuck kill + teardown + status write + upload need"
    )

    m = re.search(r'elif\s*\[\[\s*"\$memory_limit_probes"\s*-ge\s*(\d+)\s*\]\]', loop)
    assert m, "the patient-tier comparison was not found in the probe loop"
    assert m.group(1) == "60", (
        f"patient tier trips at {m.group(1)} rejections, not 60: it must stay "
        f"comparable to the timeout branch's ~72 s tolerance and below the loop's "
        f"100-probe cap"
    )

    m = re.search(
        r'\[\[\s*"\$memory_limit_probes"\s*-ge\s*(\d+)\s*&&\s*'
        r"!\s*-f\s*server_memory_stuck\.txt\s*\]\]",
        loop,
    )
    assert m, "the exhaustion dominance cutoff was not found in the probe loop"
    assert m.group(1) == "30", (
        f"exhaustion dominance cutoff is {m.group(1)}, not 30: lowering it lets a "
        f"single stray (total) 241 in an otherwise TOO_MANY-dominated window mint a "
        f"memory-stuck marker and misattribute the failure"
    )


def test_reap_poll_normal_exit_propagates_child_status(tmp_path):
    # A real child that exits 7: the poll must notice it, reap it, and store its
    # status -- no watchdog, no kill.
    out = _run_block(
        tmp_path,
        "fuzzer-client reap poll",
        preamble="bash -c 'exit 7' & fuzzer_pid=$!\nremaining_seconds=0\n",
        epilogue='echo "FUZZER_EXIT_CODE=$fuzzer_exit_code"',
    )
    assert "FUZZER_EXIT_CODE=7" in out.stdout, out.stdout + out.stderr
    assert not (tmp_path / "harness_watchdog.txt").exists()


def test_reap_poll_escalation_kills_and_records_watchdog(tmp_path):
    # A child that never exits: the poll must hit its deadline, SIGKILL it,
    # store 137, and record the stage=reap watchdog line (fail-closed).
    out = _run_block(
        tmp_path,
        "fuzzer-client reap poll",
        preamble="tail -f /dev/null & fuzzer_pid=$!\nremaining_seconds=0\n",
        epilogue='echo "FUZZER_EXIT_CODE=$fuzzer_exit_code"',
    )
    assert "FUZZER_EXIT_CODE=137" in out.stdout, out.stdout + out.stderr
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=reap reason=client_unreapable" in wd


def test_teardown_poll_normal_stop_propagates_exit_code(tmp_path):
    # The server subshell exits on its own (code 70): no escalation, no watchdog.
    out = _run_block(
        tmp_path,
        "server teardown poll",
        preamble=(
            "stop_server() { :; }\n"
            "bash -c 'exit 70' & server_bg_pid=$!\n"
            "server_pid=$server_bg_pid\n"
        ),
        epilogue='echo "SERVER_EXIT_CODE=$server_exit_code"',
    )
    assert "SERVER_EXIT_CODE=70" in out.stdout, out.stdout + out.stderr
    assert not (tmp_path / "harness_watchdog.txt").exists()


def test_teardown_poll_escalation_sigkills_and_records_watchdog(tmp_path):
    # The server subshell hangs past the 180-iteration deadline: SIGKILL +
    # stage=teardown watchdog line, and the exit code reflects the kill (137).
    out = _run_block(
        tmp_path,
        "server teardown poll",
        preamble=(
            "stop_server() { :; }\n"
            "tail -f /dev/null & server_bg_pid=$!\n"
            "server_pid=$server_bg_pid\n"
        ),
        epilogue='echo "SERVER_EXIT_CODE=$server_exit_code"',
    )
    assert "SERVER_EXIT_CODE=137" in out.stdout, out.stdout + out.stderr
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=teardown reason=graceful_stop_hung" in wd


def _run_block_counting_sleep(
    tmp_path, name: str, preamble: str, epilogue: str, release_after: int
):
    """`_run_block`, but the `sleep` mock counts the poll iterations.

    One mock invocation == one poll iteration, so `release_after` selects the
    iteration on which the mock creates the release file the child waits for.
    That lets a test place a child's exit at a chosen point INSIDE the block's
    deadline. The child only ever tests the release file for EXISTENCE -- having
    it read the counter numerically races with the mock's truncating write.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    counter = tmp_path / "poll_iterations"
    release = tmp_path / "release"
    sleep = bindir / "sleep"
    sleep.write_text(
        "#!/bin/bash\n"
        f'n=$(cat "{counter}" 2>/dev/null || echo 0); n=$((n+1)); '
        f'printf "%s" "$n" > "{counter}"\n'
        f'[ "$n" -ge {release_after} ] && : > "{release}"\n'
        "exec /bin/sleep 0.005\n",
        encoding="utf-8",
    )
    sleep.chmod(sleep.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    script = (
        # Same options as run-fuzzer.sh, so an unguarded failure inside an
        # extracted block aborts here exactly as it would in production.
        "set -euo pipefail\n"
        f'cd "{tmp_path}"\n'
        f'export PATH="{bindir}:$PATH"\n'
        + preamble
        + "\n"
        + _extract_block(name)
        + "\n"
        + epilogue
        + "\n"
    )
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60
    )


def test_reap_poll_tolerates_a_live_child_until_its_deadline(tmp_path):
    # The escalation counterpart above uses a child that NEVER exits, so it can
    # only prove that escalation eventually happens -- never that the poll waits
    # for a still-live child. This covers that direction: the child exits on poll
    # iteration 100, which is well inside the 390-unit deadline
    # (remaining_seconds=0) while the counter steps by 1, but past 390/10=39 if the
    # step (or the comparison) is mutated. So a step bump flips this test from a
    # reaped exit 7 to a SIGKILL 137 plus a bogus watchdog file.
    release = tmp_path / "release"
    out = _run_block_counting_sleep(
        tmp_path,
        "fuzzer-client reap poll",
        preamble=(
            f'bash -c \'while [ ! -f "{release}" ]; do /bin/sleep 0.005; done; '
            "exit 7' & fuzzer_pid=$!\n"
            "remaining_seconds=0\n"
        ),
        epilogue='echo "FUZZER_EXIT_CODE=$fuzzer_exit_code"',
        release_after=100,
    )
    assert "FUZZER_EXIT_CODE=7" in out.stdout, out.stdout + out.stderr
    assert not (tmp_path / "harness_watchdog.txt").exists()


def test_teardown_poll_tolerates_a_slow_stop_until_its_deadline(tmp_path):
    # Tolerance counterpart to the teardown escalation test: a graceful stop that
    # takes 60 poll iterations is well inside the 180-unit deadline at step 1, but
    # past 180/10=18 under a step mutation. The assertions are the propagated exit
    # code and the ABSENCE of the watchdog -- never the iteration count, which
    # jitters 60/61 depending on when the child observes the release file.
    release = tmp_path / "release"
    out = _run_block_counting_sleep(
        tmp_path,
        "server teardown poll",
        preamble=(
            "stop_server() { :; }\n"
            f'bash -c \'while [ ! -f "{release}" ]; do /bin/sleep 0.005; done; '
            "exit 70' & server_bg_pid=$!\n"
            "server_pid=$server_bg_pid\n"
        ),
        epilogue='echo "SERVER_EXIT_CODE=$server_exit_code"',
        release_after=60,
    )
    assert "SERVER_EXIT_CODE=70" in out.stdout, out.stdout + out.stderr
    assert not (tmp_path / "harness_watchdog.txt").exists()


# The children below use an absolute /bin/sleep on purpose: a bare `sleep` would
# hit _run_block's PATH mock (0.01s) and the child would be gone before the guard
# runs, degenerating every assertion. They also exit 0 on their own after ~2s, so
# the negative direction terminates instead of hanging on `wait`. `trap "" TERM`
# proves the observed 137 came from a SIGKILL and not from a polite TERM.
_STUCK_CHILD = "bash -c 'trap \"\" TERM; /bin/sleep 2; exit 0' &"


def test_memory_stuck_kill_sigkills_the_server(tmp_path):
    # The deliberate kill of a memory-stuck server: the child must die by SIGKILL
    # (137), not survive into the hang-prone graceful stop.
    out = _run_block(
        tmp_path,
        _STUCK_KILL_BLOCK,
        preamble=f"{_STUCK_CHILD}\nserver_pid=$!\nserver_memory_stuck=1\n",
        epilogue='rc=0; wait "$server_pid" || rc=$?; echo "SERVER_WAIT_RC=$rc"',
    )
    assert "SERVER_WAIT_RC=137" in out.stdout, out.stdout + out.stderr


def test_memory_stuck_kill_also_kills_the_gdb_tracer(tmp_path):
    # The gdb tracer must die first: a ptrace tracer defers its tracee's reap, so
    # killing only the server would leave the teardown waiting on a zombie. ASan
    # builds attach no gdb (server_gdb_pid unset), covered by the no-op test below.
    out = _run_block(
        tmp_path,
        _STUCK_KILL_BLOCK,
        preamble=(
            f"{_STUCK_CHILD}\nserver_pid=$!\n"
            f"{_STUCK_CHILD}\nserver_gdb_pid=$!\n"
            "server_memory_stuck=1\n"
        ),
        epilogue=(
            'rc=0; wait "$server_pid" || rc=$?; echo "SERVER_WAIT_RC=$rc"\n'
            'rc=0; wait "$server_gdb_pid" || rc=$?; echo "GDB_WAIT_RC=$rc"'
        ),
    )
    assert "SERVER_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    assert "GDB_WAIT_RC=137" in out.stdout, out.stdout + out.stderr


def test_memory_stuck_kill_is_a_no_op_when_not_stuck(tmp_path):
    # Negative direction: a healthy server is never killed, it exits on its own
    # (0). This also runs the ${server_gdb_pid:-} guard shape under `set -u`.
    out = _run_block(
        tmp_path,
        _STUCK_KILL_BLOCK,
        preamble=(
            f"{_STUCK_CHILD}\nserver_pid=$!\n"
            f"{_STUCK_CHILD}\nserver_gdb_pid=$!\n"
            "server_memory_stuck=0\n"
        ),
        epilogue=(
            'rc=0; wait "$server_pid" || rc=$?; echo "SERVER_WAIT_RC=$rc"\n'
            'rc=0; wait "$server_gdb_pid" || rc=$?; echo "GDB_WAIT_RC=$rc"'
        ),
    )
    assert "SERVER_WAIT_RC=0" in out.stdout, out.stdout + out.stderr
    assert "GDB_WAIT_RC=0" in out.stdout, out.stdout + out.stderr


# Same shape as _STUCK_CHILD, but the reap poll runs all 390 of its (mocked-`sleep`)
# iterations before escalating -- ~9 s wall clock -- so a 2 s child would be gone
# first and degenerate every assertion. 30 s clears that with room to spare while
# still self-exiting, so a mutant that removes a kill ends the `wait` instead of
# hanging until _run_block's 60 s timeout. stdout/stderr are detached: the grandchild
# `/bin/sleep` outlives the SIGKILLed wrapper, and a held pipe would keep
# subprocess.run() blocked on EOF for the child's full lifetime (30 s -> 8 s).
_UNREAPABLE_CHILD = "bash -c 'trap \"\" TERM; /bin/sleep 30; exit 0' >/dev/null 2>&1 &"


def test_reap_escalation_kills_tracer_and_actual_client(tmp_path):
    # The reap escalation performs THREE kills: the reap-deferring gdb tracer, the
    # actual client, then the `timeout` wrapper. Killing only the wrapper would
    # orphan a live client that keeps issuing queries during the post-fuzz probes
    # -- the exact state this harness bounds -- so all three must really fire.
    out = _run_block(
        tmp_path,
        "fuzzer-client reap poll",
        preamble=(
            f"{_UNREAPABLE_CHILD}\nfuzzer_pid=$!\n"
            f"{_UNREAPABLE_CHILD}\nclient_gdb_pid=$!\n"
            f"{_UNREAPABLE_CHILD}\nactual_fuzzer_pid=$!\n"
            "remaining_seconds=0\n"
        ),
        epilogue=(
            'echo "FUZZER_EXIT_CODE=$fuzzer_exit_code"\n'
            'rc=0; wait "$fuzzer_pid" || rc=$?; echo "FUZZER_WAIT_RC=$rc"\n'
            'rc=0; wait "$client_gdb_pid" || rc=$?; echo "GDB_WAIT_RC=$rc"\n'
            'rc=0; wait "$actual_fuzzer_pid" || rc=$?; echo "CLIENT_WAIT_RC=$rc"'
        ),
    )
    assert "FUZZER_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    assert "GDB_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    assert "CLIENT_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=reap reason=client_unreapable" in wd


def test_reap_escalation_without_a_tracer_is_still_bounded(tmp_path):
    # The ASan shape: gdb is not attached, so ONLY client_gdb_pid is unset --
    # actual_fuzzer_pid is assigned before the IS_ASAN branch, so the client kill
    # must still fire here. The `[ -n "${...:-}" ]` guard must keep the block from
    # aborting under `set -u`, and the client + wrapper kills and the watchdog
    # must all still happen.
    out = _run_block(
        tmp_path,
        "fuzzer-client reap poll",
        preamble=(
            f"{_UNREAPABLE_CHILD}\nfuzzer_pid=$!\n"
            f"{_UNREAPABLE_CHILD}\nactual_fuzzer_pid=$!\n"
            "remaining_seconds=0\n"
        ),
        epilogue=(
            'echo "FUZZER_EXIT_CODE=$fuzzer_exit_code"\n'
            'rc=0; wait "$fuzzer_pid" || rc=$?; echo "FUZZER_WAIT_RC=$rc"\n'
            'rc=0; wait "$actual_fuzzer_pid" || rc=$?; echo "CLIENT_WAIT_RC=$rc"'
        ),
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert "unbound variable" not in out.stderr, out.stderr
    assert "FUZZER_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    assert "CLIENT_WAIT_RC=137" in out.stdout, out.stdout + out.stderr
    wd = (tmp_path / "harness_watchdog.txt").read_text(encoding="utf-8")
    assert "stage=reap reason=client_unreapable" in wd
