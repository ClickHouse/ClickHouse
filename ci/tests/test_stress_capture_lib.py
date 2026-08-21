"""
Contract tests for the capture helpers in `tests/docker_scripts/stress_tests.lib`:
the pure `script_failure_info`, `flush_capture` and `drain_capture`, and the
`run_capturing_output` wrapper the two stress runners share.

No server, no docker, and no sourcing of a runner: the pure helpers are file in,
string out, and the wrapper needs only a stub command now that it is library code
rather than a copy inside each runner. What they carry -- the encode, offset and
truncation arithmetic, and the mirror -- is what decides whether a `Test script
failed` row names the reason or just an exit code, and it breaks silently:
inverting the exit-code comparison or pointing the mirror at `/dev/null` leaves
the whole stress suite green.

The library is sourced as shipped, with only `/test_output` redirected into the
test's temporary directory, so these tests cannot drift from the code they guard.

See https://github.com/ClickHouse/ClickHouse/pull/114029
"""

import re
import subprocess
from pathlib import Path

import pytest

_LIB = (
    Path(__file__).resolve().parent.parent.parent
    / "tests"
    / "docker_scripts"
    / "stress_tests.lib"
)

# `stress_tests.lib` is `set -ex` at the top, which is how the runners source it.
# Keep that, so a helper that trips errexit fails the test rather than being
# papered over.
_PREAMBLE = """set -ex
source '{lib}'
"""


# The production waits are 5/15/30 seconds. Running them for real would spend most
# of the 600-second budget shared by all of `ci/tests` on sleeping, so the copy of
# the library under test is rewritten to a scaled-down set with the same ordering
# (ok grace < failure grace < timeout). `test_the_production_timing_constants` pins
# the real values separately, so shrinking these cannot hide a change to them.
_TEST_TIMING = {
    "FAILURE_DRAIN_GRACE": 8,
    "FAILURE_DRAIN_GRACE_OK": 1,
    "FAILURE_DRAIN_TIMEOUT": 12,
}

# Only `test_wrapper_puts_late_output_on_the_console_and_in_the_row` has to tell the
# two graces apart, which needs a wide gap above the stable-sample floor. Everything
# else can use a narrow set and not pay for it, keeping the same ordering.
_FAST_TIMING = {"FAILURE_DRAIN_GRACE": 4, "FAILURE_DRAIN_TIMEOUT": 7}


def _run(body: str, tmp_path: Path, timing: dict | None = None):
    """Source the shipped library and run `body`, with `/test_output` redirected and
    the drain waits scaled down. `timing` overrides individual scaled constants."""
    test_output = tmp_path / "test_output"
    test_output.mkdir(exist_ok=True)
    text = _LIB.read_text(encoding="utf-8").replace("/test_output", str(test_output))
    for name, value in {**_TEST_TIMING, **(timing or {})}.items():
        text, count = re.subn(
            rf"^{name}=\d+$", f"{name}={value}", text, count=1, flags=re.M
        )
        assert count == 1, f"{name} is no longer a top-level constant"
    lib = tmp_path / "stress_tests.lib"
    lib.write_text(text, encoding="utf-8")
    script = _PREAMBLE.format(lib=lib) + body
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
    )


def _info(tmp_path: Path, capture: Path, exit_code: int = 1) -> str:
    """The `info` cell `script_failure_info` would put in a failure row."""
    proc = _run(
        f"script_failure_info {exit_code} '{capture}' > '{tmp_path}/info.out'", tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    return (tmp_path / "info.out").read_text(encoding="utf-8")


def test_info_reports_exit_code_when_capture_is_empty(tmp_path):
    capture = tmp_path / "cap.log"
    capture.write_text("", encoding="utf-8")
    assert _info(tmp_path, capture, 42) == " script exit code: 42"


def test_info_embeds_the_tail_as_one_escaped_cell(tmp_path):
    """The row must stay four tab-separated cells: `read_test_results` drops any
    line whose field count differs, so a raw newline or tab in the payload would
    discard the very row this carries."""
    capture = tmp_path / "cap.log"
    capture.write_text(
        "first line\nDB::Exception: Syntax error\twith a tab\nlast line\n",
        encoding="utf-8",
    )
    info = _info(tmp_path, capture)
    assert info.startswith(" script exit code: 1\\n")
    assert "\n" not in info
    assert "\t" not in info
    # The exception line survives, escaped at exactly one level.
    assert "DB::Exception: Syntax error" in info


def test_info_reports_lines_when_the_line_limit_truncates(tmp_path):
    capture = tmp_path / "cap.log"
    capture.write_text("".join(f"line{i:04d}\n" for i in range(200)), encoding="utf-8")
    info = _info(tmp_path, capture)
    assert " (last 30 of 200 lines)" in info
    # The tail is kept, not the head.
    assert "line0199" in info
    assert "line0000" not in info


def test_info_reports_bytes_when_the_byte_limit_truncates(tmp_path):
    """`escaped_tail` cuts bytes after `tail -n` cut lines, so a line count alone
    can claim more lines than the cell shows. Whichever limit bit is named in its
    own unit."""
    capture = tmp_path / "cap.log"
    # Thirty lines, so the line limit does not apply, but far past the byte limit.
    # Each line carries its own index, so the assertions below can tell the kept
    # end from the dropped one -- an all-identical filler would read the same
    # either way and could not catch a cut taken from the wrong end.
    # 30 lines of 502 bytes plus a newline each: 15090 bytes.
    capture.write_text(
        "".join(f"line{i:04d}" + "X" * 494 + "\n" for i in range(30)), encoding="utf-8"
    )
    info = _info(tmp_path, capture)
    assert re.search(r" \(last 3000 of 15090 bytes\)", info), info
    assert "lines)" not in info
    # 3000 bytes reaches back about six lines: the tail is kept, the head dropped.
    assert "line0029" in info
    assert "line0000" not in info


def test_info_counts_an_unterminated_last_line(tmp_path):
    """A killed script leaves the last line without its newline. `wc -l` counts
    newline bytes and would report one line short, presenting a truncated tail as
    the whole output."""
    capture = tmp_path / "cap.log"
    capture.write_text(
        "".join(f"line{i:04d}\n" for i in range(40)) + "killed mid-line",
        encoding="utf-8",
    )
    info = _info(tmp_path, capture)
    assert " (last 30 of 41 lines)" in info


def test_info_names_the_encoder_when_it_fails(tmp_path):
    """A partial encode carries raw newlines, which would split the row and make
    `read_test_results` drop both halves. The guard must reject the output on a
    nonzero encoder status and say so: the capture is not empty here, so falling
    back silently would read as no captured output at all."""
    capture = tmp_path / "cap.log"
    capture.write_text("DB::Exception: the actual reason\n", encoding="utf-8")
    results = tmp_path / "test_results.tsv"
    proc = _run(
        f"""
# A broken encoder: some output, then a nonzero status.
escaped_tail() {{ printf 'partial\\nwith a raw newline\\n'; return 3; }}
append_script_result_row '{results}' 1 '{capture}'
""",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    row = results.read_text(encoding="utf-8")
    assert "(could not encode the captured output)" in row, row
    # Still one line of four fields, so the row survives parsing.
    assert row.count("\n") == 1, row
    assert len(row.rstrip("\n").split("\t")) == 4, row
    # None of the partial output leaked in.
    assert "with a raw newline" not in row


def test_info_names_the_encoder_when_it_returns_nothing(tmp_path):
    """An encoder that succeeds but emits nothing would otherwise produce a row
    ending in the `\\n` separator with no payload after it."""
    capture = tmp_path / "cap.log"
    capture.write_text("DB::Exception: boom\n", encoding="utf-8")
    proc = _run(
        f"""
escaped_tail() {{ return 0; }}
script_failure_info 1 '{capture}' > info.out
""",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    info = (tmp_path / "info.out").read_text(encoding="utf-8")
    assert info == " script exit code: 1 (could not encode the captured output)", info


def test_flush_capture_prints_only_the_bytes_past_the_offset(tmp_path):
    """A byte belongs to one flush, never to both: the second flush starts where
    the first ended."""
    capture = tmp_path / "cap.log"
    capture.write_text("AAAA\nBBBB\n", encoding="utf-8")
    proc = _run(
        f"""
flush_capture '{capture}' 5 > first.out
echo "offset1=$flushed_offset" >> report
printf 'CCCC\\n' >> '{capture}'
flush_capture '{capture}' "$flushed_offset" > second.out
echo "offset2=$flushed_offset" >> report
""",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "first.out").read_text(encoding="utf-8") == "BBBB\n"
    assert (tmp_path / "second.out").read_text(encoding="utf-8") == "CCCC\n"
    report = (tmp_path / "report").read_text(encoding="utf-8")
    assert "offset1=10" in report
    assert "offset2=15" in report


def test_flush_capture_is_a_noop_when_nothing_is_new(tmp_path):
    capture = tmp_path / "cap.log"
    capture.write_text("AAAA\n", encoding="utf-8")
    proc = _run(
        f"flush_capture '{capture}' 5 > out; echo \"offset=$flushed_offset\" > report",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "out").read_text(encoding="utf-8") == ""
    assert "offset=5" in (tmp_path / "report").read_text(encoding="utf-8")


def test_drain_capture_honours_its_grace_before_returning(tmp_path):
    """A holder that has not written yet is indistinguishable from none, so
    size-stability alone must not end the wait."""
    capture = tmp_path / "cap.log"
    capture.write_text("stable from the start\n", encoding="utf-8")
    proc = _run(
        f"""
started=$SECONDS
drain_capture '{capture}' 4
echo "elapsed=$(( SECONDS - started ))" > report
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    elapsed = int(
        re.search(r"elapsed=(\d+)", (tmp_path / "report").read_text()).group(1)
    )
    assert 4 <= elapsed < FAILURE_DRAIN_TIMEOUT_CEILING


def test_drain_capture_waits_for_a_late_writer(tmp_path):
    """The point of the wait: bytes appended after the wrapped command exits are
    still on disk when the row and the artifact are composed.

    The write is scheduled inside the grace window and the capture is snapshotted
    in the shell the instant the drain returns. Reading the file from Python
    instead would prove nothing: `subprocess.run` collects output, so it does not
    return until the writer has closed its inherited pipes -- by which time the
    late byte is there whether the drain waited for it or not."""
    capture = tmp_path / "cap.log"
    capture.write_text("early\n", encoding="utf-8")
    proc = _run(
        f"""
( sleep 2; printf 'late-diagnostic\\n' >> '{capture}' ) &
writer=$!
drain_capture '{capture}' 6
cp '{capture}' snapshot.log
wait "$writer"
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    assert "late-diagnostic" in (tmp_path / "snapshot.log").read_text(encoding="utf-8")


def test_drain_capture_is_bounded_when_a_writer_never_stops(tmp_path):
    """An unbounded wait here would spend the job's whole time limit: the earlier
    pipe form of this wrapper cost a 2h09m `Stress test (amd_tsan)` timeout.

    The writer keeps going past the ceiling asserted below, so a drain that never
    gave up would exceed it rather than merely finishing late."""
    capture = tmp_path / "cap.log"
    capture.write_text("", encoding="utf-8")
    proc = _run(
        f"""
( for _ in $(seq 30); do printf 'x\\n' >> '{capture}'; sleep 0.5; done ) &
writer=$!
started=$SECONDS
drain_capture '{capture}'
echo "elapsed=$(( SECONDS - started ))" > report
kill "$writer" 2>/dev/null ||:
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    elapsed = int(
        re.search(r"elapsed=(\d+)", (tmp_path / "report").read_text()).group(1)
    )
    assert elapsed <= FAILURE_DRAIN_TIMEOUT_CEILING


def test_drain_capture_does_not_leak_xtrace_into_the_console(tmp_path):
    """Both runners are `set -ex`, so an untraced poll would emit roughly ten
    trace lines a second for up to 30 s, interleaved with the mirrored capture.
    `set -e` must still be active for the caller afterwards."""
    capture = tmp_path / "cap.log"
    capture.write_text("stable\n", encoding="utf-8")
    proc = _run(f"drain_capture '{capture}' 1\necho \"opts=$-\"", tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "stat -c %s" not in proc.stderr, proc.stderr
    opts = re.search(r"opts=(\S+)", proc.stdout).group(1)
    assert "e" in opts and "x" in opts, opts


# The narrow `FAILURE_DRAIN_TIMEOUT` plus a poll interval and scheduling slack. Kept
# tight: a generous ceiling is one an unbounded wait would also satisfy.
FAILURE_DRAIN_TIMEOUT_CEILING = _FAST_TIMING["FAILURE_DRAIN_TIMEOUT"] + 4


def test_the_production_timing_constants():
    """Read from the shipped library, not the scaled copy the tests run against:
    these are the values a stress job actually waits, and the bound they place on a
    failing job's exposure is what a change to them would widen."""
    shipped = dict(
        re.findall(r"^(FAILURE_[A-Z_]+)=(\d+)$", _LIB.read_text(encoding="utf-8"), re.M)
    )
    assert shipped["FAILURE_DRAIN_GRACE_OK"] == "5"
    assert shipped["FAILURE_DRAIN_GRACE"] == "15"
    assert shipped["FAILURE_DRAIN_TIMEOUT"] == "30"
    assert shipped["FAILURE_TAIL_LINES"] == "30"
    assert shipped["FAILURE_TAIL_MAX_BYTES"] == "3000"
    assert shipped["FAILURE_DRAIN_STABLE_SAMPLES"] == "3"
    assert shipped["FAILURE_DRAIN_INTERVAL"] == "1"
    # The ordering the two graces rely on: a passing run waits least, a failing run
    # more, and neither past the ceiling.
    assert (
        int(shipped["FAILURE_DRAIN_GRACE_OK"])
        < int(shipped["FAILURE_DRAIN_GRACE"])
        < int(shipped["FAILURE_DRAIN_TIMEOUT"])
    )
    # The scaled set keeps the ordering the two graces rely on, and keeps the ok grace
    # below the stable-sample floor so only the failure grace is observable above it.
    assert (
        _TEST_TIMING["FAILURE_DRAIN_GRACE_OK"]
        < _TEST_TIMING["FAILURE_DRAIN_GRACE"]
        < _TEST_TIMING["FAILURE_DRAIN_TIMEOUT"]
    )


# --- `run_capturing_output`: the wrapper the two runners now share ----------------
#
# Not a runner-sourcing harness: the wrapper is library code, so a stub command is
# all it needs. Every behaviour asserted below has a mutant that turns these red --
# the mirror opened as a path, an inverted exit-code comparison, a mirror pointed at
# /dev/null, a dropped final flush and a dropped artifact copy.

_STUB = """#!/bin/bash
echo 'line one'
( sleep {late}; echo 'late-child-diagnostic' ) &
echo 'traceback: RuntimeError boom'
exit {code}
"""


def _wrap(tmp_path, exit_code: int, late: int, stdout_is_file: bool, timing=None):
    """Run the shipped wrapper over a stub, with this shell's stdout either a pipe
    (as in CI, where the container's stdout is a pipe) or a regular file (as in a
    local `./stress_runner.sh > log 2>&1`)."""
    stub = tmp_path / "stub.sh"
    stub.write_text(_STUB.format(code=exit_code, late=late), encoding="utf-8")
    stub.chmod(0o755)
    body = f"""
run_capturing_output '{tmp_path}/cap.log' '{stub}'
wrapped_status=$?
set -e
echo "STATUS=$wrapped_status"
printf 'ROW=%s\\n' "$(script_failure_info "$wrapped_status" '{tmp_path}/cap.log')"
"""
    if not stdout_is_file:
        # `_run` already captures through a pipe, which is the CI shape.
        proc = _run(body, tmp_path, timing)
        return proc, proc.stdout
    # `exec >`, not a `{ ... } > file` group: a group's redirection is undone before the
    # shell's EXIT trap runs, so the trap's flush would miss the file and this would read
    # as a product defect. The runners are redirected the same way, by the job.
    console = tmp_path / "console.log"
    proc = _run(f"exec > '{console}' 2>/dev/null\n{body}", tmp_path, timing)
    return proc, console.read_text(encoding="utf-8", errors="replace")


def _count(text: str, token: str) -> int:
    return text.count(token)


@pytest.mark.parametrize("stdout_is_file", [False, True], ids=["pipe", "regular_file"])
def test_wrapper_mirrors_the_output_whatever_stdout_is(tmp_path, stdout_is_file):
    """`tee <path>` would `open()` the path: for a regular-file stdout that is a
    second file description with its own offset, and the shell's later writes then
    overwrite the mirrored bytes. CI redirects to a pipe, where both forms behave
    the same, so only this second case catches it.

    Each token is expected twice -- once from the mirror, once from the row cell --
    which is what distinguishes a lost mirror from a lost capture."""
    proc, out = _wrap(
        tmp_path,
        exit_code=1,
        late=1,
        stdout_is_file=stdout_is_file,
        timing=_FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    for token in ("line one", "traceback: RuntimeError boom"):
        assert _count(out, token) == 2, (token, out)
    assert "STATUS=1" in out


def test_wrapper_puts_late_output_on_the_console_and_in_the_row(tmp_path):
    """A descendant inherits the command's stdout and can append after it exits.
    Those bytes must reach the console and the failure row, not just the file.

    The delay sits above the wait a passing run makes and below the one a failing
    run makes, so only the failure path covers it. That is what pins the exit-code
    comparison selecting between the two graces: a delay inside both, or above
    both, could not tell them apart."""
    proc, out = _wrap(tmp_path, exit_code=1, late=5, stdout_is_file=False)
    assert proc.returncode == 0, proc.stderr
    assert _count(out, "late-child-diagnostic") == 2, out
    row = [l for l in out.splitlines() if l.startswith("ROW=")][0]
    assert "late-child-diagnostic" in row


@pytest.mark.parametrize("exit_code", [0, 1, 42, 143])
def test_wrapper_propagates_the_status_and_gates_the_artifact_on_it(
    tmp_path, exit_code
):
    """The status reaches `$?` -- a wrapper that swallowed it would report every run
    as a pass. The artifact is kept only for a failure: a passing run would
    otherwise upload the whole capture on every stress job."""
    proc, out = _wrap(
        tmp_path,
        exit_code=exit_code,
        late=1,
        stdout_is_file=False,
        timing=_FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    assert f"STATUS={exit_code}" in out, out
    artifact = tmp_path / "test_output" / "stress_script.log"
    assert artifact.exists() is (exit_code != 0)


def test_wrapper_returns_while_a_descendant_still_holds_the_output(tmp_path):
    """The reason the command is redirected to a file and followed, rather than piped:
    `stress.py` drops databases with a fire-and-forget `Popen(..., shell=True)` that
    inherits its stdout and is never reaped, and a pipe reader waits for every such
    holder. The earlier pipe form of this wrapper cost a 2h09m `Stress test (amd_tsan)`
    timeout for exactly that reason.

    The holder here outlives the drain and is never reaped inside the wrapper, so a
    reader that waited on the descriptor could not return at all. Only `tail --pid`
    ending with the command can, which is what the elapsed bound below asserts."""
    stub = tmp_path / "s.sh"
    # Writes once, then leaves a child holding stdout without ever writing or exiting.
    stub.write_text(
        "#!/bin/bash\necho early\n( sleep 600 ) &\nexit 1\n", encoding="utf-8"
    )
    stub.chmod(0o755)
    # `timeout` around the wrapper, so a reader that waits on the descriptor fails here
    # with the bound named rather than by exhausting the harness timeout minutes later.
    bound = FAILURE_DRAIN_TIMEOUT_CEILING
    proc = _run(
        f"""
started=$SECONDS
timeout {bound} bash -c '
    source "$1"
    run_capturing_output "$2" "$3"
    echo "status=$?"' _ '{tmp_path}/stress_tests.lib' '{tmp_path}/c.log' '{stub}'
echo "timeout_status=$?"
set -e
echo "elapsed=$(( SECONDS - started ))"
# Reap the holder only now: the wrapper had to return without its help.
pkill -P $$ sleep ||:
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    # 124 is `timeout` killing a wrapper that never returned.
    assert "timeout_status=0" in proc.stdout, proc.stdout
    assert "status=1" in proc.stdout, proc.stdout
    elapsed = int(re.search(r"elapsed=(\d+)", proc.stdout).group(1))
    assert elapsed <= FAILURE_DRAIN_TIMEOUT_CEILING, (elapsed, proc.stdout)
    # The mirror still ran: the bound is not met by giving up on the output.
    assert "early" in proc.stdout, proc.stdout


def test_wrapper_leaves_errexit_off_for_the_caller(tmp_path):
    """The runners read `$?` on the next line, which only works while errexit is
    off: with `set -e` restored inside the wrapper, a nonzero status would abort
    the script before the `Test script failed` row was ever appended."""
    stub = tmp_path / "s.sh"
    stub.write_text("#!/bin/bash\nexit 3\n", encoding="utf-8")
    stub.chmod(0o755)
    proc = _run(
        f"""
run_capturing_output '{tmp_path}/c.log' '{stub}'
echo "REACHED status=$?"
set -e
echo "opts=$-"
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    assert "REACHED status=3" in proc.stdout, proc.stdout
    assert "e" in re.search(r"opts=(\S+)", proc.stdout).group(1)


def test_wrapper_keeps_the_artifact_on_a_failing_script_that_the_job_survives(tmp_path):
    """`upgrade_runner.sh`'s allow-list exits 0 after a failing stress script. The
    trap must still report the script's own status, so the capture is preserved and
    refreshed with post-settle bytes: taking the trap's `$?` instead would see 0
    there and drop the artifact for the run that most needs it."""
    stub = tmp_path / "s.sh"
    stub.write_text(
        "#!/bin/bash\necho early\n" "( sleep 8; echo AFTER-SETTLE ) &\nexit 1\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    console = tmp_path / "c3.log"
    proc = _run(
        f"""
exec > '{console}' 2>/dev/null
run_capturing_output '{tmp_path}/c.log' '{stub}'
set -e
sleep 10
# The allow-list path: the script failed, the job does not.
exit 0
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, (proc.returncode, proc.stderr)
    artifact = tmp_path / "test_output" / "stress_script.log"
    assert artifact.exists(), "artifact dropped because the job exited 0"
    assert "AFTER-SETTLE" in artifact.read_text(encoding="utf-8")
    assert "AFTER-SETTLE" in console.read_text(encoding="utf-8", errors="replace")


def test_wrapper_flushes_late_output_of_a_passing_script(tmp_path):
    """A passing run keeps no artifact, but its late output must still reach the
    console: the flush is unconditional and only the artifact is gated on status."""
    stub = tmp_path / "s.sh"
    stub.write_text(
        "#!/bin/bash\necho early\n" "( sleep 8; echo LATE-ON-SUCCESS ) &\nexit 0\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    console = tmp_path / "c4.log"
    proc = _run(
        f"""
exec > '{console}' 2>/dev/null
run_capturing_output '{tmp_path}/c.log' '{stub}'
set -e
sleep 10
exit 0
""",
        tmp_path,
        _FAST_TIMING,
    )
    assert proc.returncode == 0, proc.stderr
    out = console.read_text(encoding="utf-8", errors="replace")
    assert "LATE-ON-SUCCESS" in out, out
    assert not (tmp_path / "test_output" / "stress_script.log").exists()


def test_wrapper_recovers_output_that_arrives_after_an_early_exit(tmp_path):
    """Both runners exit early after this point -- `start_server` failing, the
    upgrade allow-list `exit 0`. The EXIT trap is what preserves output that
    arrived after the settle wait on exactly those runs, and it must not replace
    the status the job reports.

    The write is scheduled past the drain's timeout, so the drain provably
    cannot have caught it: a writer inside the drain window would be recovered
    whether the trap existed or not, and this would assert nothing."""
    stub = tmp_path / "s.sh"
    stub.write_text(
        "#!/bin/bash\necho early\n"
        "( sleep 8; echo APPEARS-AFTER-SETTLE ) &\nexit 1\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    console = tmp_path / "c2.log"
    proc = _run(
        f"""
exec > '{console}' 2>/dev/null
run_capturing_output '{tmp_path}/c.log' '{stub}'
set -e
echo 'Failed to start server'
# The runners reach their early exit some time after the wrapper returns; give the late
# writer that long, so the trap has something left to recover.
sleep 10
exit 1
""",
        tmp_path,
        _FAST_TIMING,
    )
    # The simulated early exit is the harness's own status.
    assert proc.returncode == 1, (proc.returncode, proc.stderr)
    out = console.read_text(encoding="utf-8", errors="replace")
    assert "APPEARS-AFTER-SETTLE" in out, out
    artifact = tmp_path / "test_output" / "stress_script.log"
    assert "APPEARS-AFTER-SETTLE" in artifact.read_text(encoding="utf-8")


# --- `append_script_result_row`: the row branch both runners now share ------------


def _row(tmp_path, exit_code: int, capture_text: str) -> str:
    capture = tmp_path / "cap.log"
    capture.write_text(capture_text, encoding="utf-8")
    results = tmp_path / "test_results.tsv"
    proc = _run(
        f"append_script_result_row '{results}' {exit_code} '{capture}'", tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    return results.read_text(encoding="utf-8")


def test_result_row_reports_success_without_the_capture(tmp_path):
    """`read_test_results` keys off the status column, so a passing run must not be
    written as a failure -- and must not carry the capture, which on a green stress
    job is the whole log."""
    row = _row(tmp_path, 0, "irrelevant output\n")
    assert row.split("\t")[0] == "Test script exit code"
    assert row.split("\t")[1] == "OK"
    assert "irrelevant output" not in row


def test_result_row_reports_failure_with_the_reason(tmp_path):
    row = _row(tmp_path, 1, "DB::Exception: the actual reason\n")
    fields = row.rstrip("\n").split("\t")
    assert fields[0] == "Test script failed"
    assert fields[1] == "FAIL"
    assert "DB::Exception: the actual reason" in fields[3]
    assert "script exit code: 1" in fields[3]


def test_result_row_is_four_fields_on_one_line(tmp_path):
    """`read_test_results` drops any line whose field count is not four, so a payload
    that leaked a raw tab or newline would discard the row it was meant to explain."""
    row = _row(tmp_path, 7, "first\nwith\ta\ttab\nDB::Exception: boom\n")
    assert row.count("\n") == 1, row
    assert len(row.rstrip("\n").split("\t")) == 4, row


@pytest.mark.parametrize("exit_code", [0, 1], ids=["success", "failure"])
def test_result_row_appends_rather_than_truncates(tmp_path, exit_code):
    """The runners write this row into a results file earlier stages have already
    added to, so truncating it would discard every result before this one. Both
    branches write, so both are checked."""
    capture = tmp_path / "cap.log"
    capture.write_text("boom\n", encoding="utf-8")
    results = tmp_path / "test_results.tsv"
    results.write_text("Existing row\tOK\t\\N\t\n", encoding="utf-8")
    proc = _run(
        f"append_script_result_row '{results}' {exit_code} '{capture}'", tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    lines = results.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2, lines
    assert lines[0].startswith("Existing row")
    assert lines[1].startswith("Test script")


# --- the runners' call sites -------------------------------------------------------
#
# Checked statically, not by sourcing and stubbing a runner: that harness shape was
# explicitly rejected in review, and the runners reach these lines only after
# installing packages and starting a server. What is left in each runner is two
# library calls, a `$?` read and a `set -e`, so the risk here is drift between the
# two copies and a path mismatch, both of which the text shows.

_RUNNERS = (Path(__file__).resolve().parent.parent.parent / "tests" / "docker_scripts",)


def _runner_text(name: str) -> str:
    return (_RUNNERS[0] / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("runner", ["stress_runner.sh", "upgrade_runner.sh"])
def test_runner_captures_and_reports_through_the_library(runner):
    """Both runners must wrap their script and append their row through the shared
    helpers. A runner that grew its own copy again would drift from the other, which
    is what the duplicated block did before."""
    text = _runner_text(runner)
    assert text.count("run_capturing_output ") == 1, runner
    assert text.count("append_script_result_row ") == 1, runner
    # No leftover copy of the block the helpers replaced.
    for gone in (
        "tail -n +1 -f --pid=",
        "drain_capture ",
        "finalize_capture ",
        "exec 3>&1",
    ):
        assert gone not in text, (runner, gone)


@pytest.mark.parametrize("runner", ["stress_runner.sh", "upgrade_runner.sh"])
def test_runner_reads_the_status_and_restores_errexit(runner):
    """`run_capturing_output` returns with errexit off, so the runner must read `$?`
    on the next line and turn `set -e` back on itself. Reading anything else would
    report every stress run as a pass, and leaving errexit off would let the rest of
    the runner ignore failures."""
    lines = [l.strip() for l in _runner_text(runner).splitlines()]
    i = next(i for i, l in enumerate(lines) if l.startswith("run_capturing_output "))
    # The wrapped command continues onto the next line, hence the offsets.
    assert lines[i].endswith("\\"), lines[i]
    assert lines[i + 2] == "stress_script_exit_code=$?", lines[i + 2]
    assert lines[i + 3] == "set -e", lines[i + 3]


@pytest.mark.parametrize("runner", ["stress_runner.sh", "upgrade_runner.sh"])
def test_runner_passes_the_same_capture_path_to_both_helpers(runner):
    """The wrapper writes the capture and the row reads it: a mismatch would silently
    report every failure as having produced no output."""
    text = _runner_text(runner)
    wrapper_path = re.search(r"run_capturing_output (\S+)", text).group(1)
    row = re.search(r"append_script_result_row (\S+) (\S+) \\\n\s*(\S+)", text)
    assert row, runner
    results, status, row_path = row.groups()
    assert row_path == wrapper_path, (runner, wrapper_path, row_path)
    assert status == '"$stress_script_exit_code"', status
    # The row goes to the file the report is parsed from, and the capture stays out
    # of the wholesale-uploaded directory.
    assert results == "/test_output/test_results.tsv", results
    assert not wrapper_path.startswith("/test_output/"), wrapper_path


def test_the_two_runners_report_identically():
    """The reporting sequence is the same in both runners and must stay that way:
    the only thing that legitimately differs is the command being wrapped."""

    def sequence(name: str):
        lines = [l.strip() for l in _runner_text(name).splitlines()]
        i = next(
            i for i, l in enumerate(lines) if l.startswith("run_capturing_output ")
        )
        # Drop line i+1, the wrapped command, which is what differs by design.
        return [lines[i], *lines[i + 2 : i + 6]]

    assert sequence("stress_runner.sh") == sequence("upgrade_runner.sh")
