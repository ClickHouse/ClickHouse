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


def _run(body: str, tmp_path: Path):
    """Source the shipped library and run `body`, with `/test_output` redirected."""
    test_output = tmp_path / "test_output"
    test_output.mkdir(exist_ok=True)
    lib = (tmp_path / "stress_tests.lib")
    lib.write_text(
        _LIB.read_text(encoding="utf-8").replace("/test_output", str(test_output)),
        encoding="utf-8",
    )
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
( sleep 3; printf 'late-diagnostic\\n' >> '{capture}' ) &
writer=$!
drain_capture '{capture}' 8
cp '{capture}' snapshot.log
wait "$writer"
""",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    assert "late-diagnostic" in (tmp_path / "snapshot.log").read_text(encoding="utf-8")


def test_drain_capture_is_bounded_when_a_writer_never_stops(tmp_path):
    """An unbounded wait here would spend the job's whole time limit: the earlier
    pipe form of this wrapper cost a 2h09m `Stress test (amd_tsan)` timeout."""
    capture = tmp_path / "cap.log"
    capture.write_text("", encoding="utf-8")
    proc = _run(
        f"""
( for _ in $(seq 100); do printf 'x\\n' >> '{capture}'; sleep 0.5; done ) &
writer=$!
started=$SECONDS
drain_capture '{capture}'
echo "elapsed=$(( SECONDS - started ))" > report
kill "$writer" 2>/dev/null ||:
""",
        tmp_path,
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


# `FAILURE_DRAIN_TIMEOUT` plus one poll interval and a little scheduling slack.
FAILURE_DRAIN_TIMEOUT_CEILING = 45


def test_the_timeout_ceiling_matches_the_library(tmp_path):
    """Pins the constants the bounds above are written against, so raising one in
    the library without revisiting them fails here rather than silently widening
    the job's exposure."""
    proc = _run(
        "echo \"$FAILURE_DRAIN_TIMEOUT $FAILURE_DRAIN_INTERVAL"
        " $FAILURE_TAIL_LINES $FAILURE_TAIL_MAX_BYTES\" > report",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    timeout, interval, tail_lines, tail_bytes = (
        int(x) for x in (tmp_path / "report").read_text().split()
    )
    assert timeout + interval <= FAILURE_DRAIN_TIMEOUT_CEILING
    assert (tail_lines, tail_bytes) == (30, 3000)


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


def _wrap(tmp_path, exit_code: int, late: int, stdout_is_file: bool):
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
        proc = _run(body, tmp_path)
        return proc, proc.stdout
    # `exec >`, not a `{ ... } > file` group: a group's redirection is undone before the
    # shell's EXIT trap runs, so the trap's flush would miss the file and this would read
    # as a product defect. The runners are redirected the same way, by the job.
    console = tmp_path / "console.log"
    proc = _run(f"exec > '{console}' 2>/dev/null\n{body}", tmp_path)
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
    proc, out = _wrap(tmp_path, exit_code=1, late=3, stdout_is_file=stdout_is_file)
    assert proc.returncode == 0, proc.stderr
    for token in ("line one", "traceback: RuntimeError boom"):
        assert _count(out, token) == 2, (token, out)
    assert "STATUS=1" in out


def test_wrapper_puts_late_output_on_the_console_and_in_the_row(tmp_path):
    """A descendant inherits the command's stdout and can append after it exits.
    Those bytes must reach the console and the failure row, not just the file.

    The delay sits between `FAILURE_DRAIN_GRACE_OK` and `FAILURE_DRAIN_GRACE`, so
    only a failing run's full settle wait covers it: this is what pins the
    exit-code comparison that selects between the two graces. A shorter delay
    would be inside both and could not tell them apart."""
    proc, out = _wrap(tmp_path, exit_code=1, late=9, stdout_is_file=False)
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
    proc, out = _wrap(tmp_path, exit_code=exit_code, late=1, stdout_is_file=False)
    assert proc.returncode == 0, proc.stderr
    assert f"STATUS={exit_code}" in out, out
    artifact = tmp_path / "test_output" / "stress_script.log"
    assert artifact.exists() is (exit_code != 0)


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
    )
    assert proc.returncode == 0, proc.stderr
    assert "REACHED status=3" in proc.stdout, proc.stdout
    assert "e" in re.search(r"opts=(\S+)", proc.stdout).group(1)


def test_wrapper_recovers_output_that_arrives_after_an_early_exit(tmp_path):
    """Both runners exit early after this point -- `start_server` failing, the
    upgrade allow-list `exit 0`. The EXIT trap is what preserves output that
    arrived after the settle wait on exactly those runs, and it must not replace
    the status the job reports.

    The write is scheduled past `FAILURE_DRAIN_TIMEOUT`, so the drain provably
    cannot have caught it: a writer inside the drain window would be recovered
    whether the trap existed or not, and this would assert nothing."""
    stub = tmp_path / "s.sh"
    stub.write_text(
        "#!/bin/bash\necho early\n"
        "( sleep 35; echo APPEARS-AFTER-SETTLE ) &\nexit 1\n",
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
sleep 40
exit 1
""",
        tmp_path,
    )
    # The simulated early exit is the harness's own status.
    assert proc.returncode == 1, (proc.returncode, proc.stderr)
    out = console.read_text(encoding="utf-8", errors="replace")
    assert "APPEARS-AFTER-SETTLE" in out, out
    artifact = tmp_path / "test_output" / "stress_script.log"
    assert "APPEARS-AFTER-SETTLE" in artifact.read_text(encoding="utf-8")
