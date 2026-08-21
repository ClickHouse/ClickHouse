"""
Contract tests for the three pure capture helpers in
`tests/docker_scripts/stress_tests.lib`: `script_failure_info`, `flush_capture`
and `drain_capture`.

They are file in, string out: no server, no docker, and no sourcing of a runner.
The encode, offset and truncation arithmetic they carry is what decides whether a
`Test script failed` row names the reason or just an exit code, and it is the part
that breaks silently -- inverting the exit-code comparison in the wrapper, or
pointing its mirror at `/dev/null`, leaves the whole stress suite green.

The library is sourced as shipped, with only `/test_output` redirected into the
test's temporary directory, so these tests cannot drift from the code they guard.

See https://github.com/ClickHouse/ClickHouse/pull/114029
"""

import os
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
    still on disk when the row and the artifact are composed."""
    capture = tmp_path / "cap.log"
    capture.write_text("early\n", encoding="utf-8")
    proc = _run(
        f"""
( sleep 6; printf 'late-diagnostic\\n' >> '{capture}' ) &
drain_capture '{capture}' 2
""",
        tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    assert "late-diagnostic" in capture.read_text(encoding="utf-8")


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
