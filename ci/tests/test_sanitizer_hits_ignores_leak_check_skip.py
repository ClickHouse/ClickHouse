"""The `sanitizer_hits` detector must ignore safeExit()'s leak-check-skip notice.

`ClickHouseProc.check_fatal_messages_in_logs` scans the server's stderr for
sanitizer output: it opens an *open-ended* `sed` range at the first line
containing `anitizer` and prints everything from there to EOF, then removes a
closed allow-list of benign lines. Whatever survives `head -n 1` becomes a
`BLOCKER` job failure.

`safeExit()` writes `Not running the leak check: other threads are still
running.` when a forced shutdown skips the LeakSanitizer check. That notice
records a check that was *skipped*, so it can never itself be a report - but it
is not sanitizer output either, and once the range is open it survives the
allow-list and gets blamed as a sanitizer hit.

The range does open benignly: the `__asan_handle_no_return` block's
`For details see https://github.com/google/sanitizers/issues/189` line matches
`anitizer` and is removed only *after* the range has opened.

The arms below drive the real pipeline, extracted from the module's source so
that editing the pipeline breaks this test rather than silently bypassing it.
Arms 2 and 5b are the ones that matter most: the filter must not swallow a
genuine report, neither on its own line nor sharing a line with the notice.
Arm 4 covers the multi-server layout, where `stderr*.log` is several files that
`sed` concatenates into one stream, so the range can open in one file and blame
a line in the next. Arm 5 pins the filter to our exact literal, so it cannot
grow into a blanket silencer.
"""

import ast
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PROC_PY = REPO_ROOT / "ci" / "jobs" / "scripts" / "clickhouse_proc.py"

SKIP_NOTICE = "Not running the leak check: other threads are still running."

# The `__asan_handle_no_return` block, verbatim. Its 4th line is what opens the
# `sed` range; all four are on the detector's allow-list.
ASAN_STACK_SIZE_BLOCK = [
    "==1==WARNING: ASan is ignoring requested __asan_handle_no_return: stack top: 0x7f0000000000; bottom 0x7ffd00000000; size: 0x000300000000 (12884901888)",
    "False positive error reports may follow",
    "For details see https://github.com/google/sanitizers/issues/189",
    "==1==WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!",
]

REAL_REPORT = "==1==ERROR: LeakSanitizer: detected memory leaks"


def _extract_sanitizer_hits_pipeline() -> str:
    """The `sanitizer_hits` shell pipeline, taken from the module's own source.

    Read rather than imported: importing `clickhouse_proc` pulls in the whole
    praktika stack. The f-string is reassembled from the AST so a paraphrase
    cannot creep in, and `{self.log_dir}` becomes `${LOG_DIR}`.
    """
    tree = ast.parse(PROC_PY.read_text())
    assigns = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "sanitizer_hits"
            for target in node.targets
        )
    ]
    assert (
        len(assigns) == 1
    ), f"expected one sanitizer_hits assignment, got {len(assigns)}"
    call = assigns[0].value
    assert isinstance(
        call, ast.Call
    ), "sanitizer_hits is no longer a Shell.get_output() call"
    joined = call.args[0]
    assert isinstance(
        joined, ast.JoinedStr
    ), "sanitizer_hits argument is no longer an f-string"

    parts = []
    for value in joined.values:
        if isinstance(value, ast.Constant):
            parts.append(value.value)
        else:
            expr = ast.unparse(value.value)
            assert (
                expr == "self.log_dir"
            ), f"unexpected interpolation {expr!r} in the pipeline"
            parts.append("${LOG_DIR}")
    return "".join(parts)


PIPELINE = _extract_sanitizer_hits_pipeline()


def _detect(log_dir: Path) -> str:
    """Run the detector over `log_dir` and return what it would blame."""
    completed = subprocess.run(
        ["bash", "-c", f"LOG_DIR={log_dir}; " + PIPELINE],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _write(log_dir: Path, **files) -> None:
    for name, lines in files.items():
        (log_dir / name).write_text("".join(line + "\n" for line in lines))


def test_pipeline_filters_the_skip_notice(tmp_path):
    """Arm 1: the notice alone, behind a benignly opened range, is not a hit."""
    _write(tmp_path, **{"stderr.log": ASAN_STACK_SIZE_BLOCK + [SKIP_NOTICE]})
    assert _detect(tmp_path) == ""


def test_pipeline_still_reports_a_real_leak(tmp_path):
    """Arm 2: a genuine report is still blamed.

    This is the arm that proves the filter is not a blanket silencer, so it is
    written in the ordering CI actually produces - `safeExit()` writes the
    notice immediately before `_exit()`, so a real report always precedes it -
    and it must hold both with and without the filter.
    """
    _write(
        tmp_path,
        **{"stderr.log": ASAN_STACK_SIZE_BLOCK + [REAL_REPORT, SKIP_NOTICE]},
    )
    assert _detect(tmp_path) == REAL_REPORT


def test_pipeline_reports_a_real_leak_the_notice_would_shadow(tmp_path):
    """Arm 2b: a report *after* the notice is blamed, rather than the notice.

    No single process emits this ordering, but the detector globs `stderr*.log`
    and `sed` runs without `-s`, so several servers' files are one stream (the
    layout arm 4 covers): server A's notice can precede server B's report.
    The detector's output is order-sensitive (`head -n 1`), so without the
    filter the notice would be blamed and the leak hidden behind it.
    """
    _write(
        tmp_path,
        **{"stderr.log": ASAN_STACK_SIZE_BLOCK + [SKIP_NOTICE, REAL_REPORT]},
    )
    assert _detect(tmp_path) == REAL_REPORT


def test_pipeline_ignores_the_asan_stack_size_block_alone(tmp_path):
    """Arm 3: the allow-listed block on its own is not a hit (unchanged behaviour)."""
    _write(tmp_path, **{"stderr.log": ASAN_STACK_SIZE_BLOCK})
    assert _detect(tmp_path) == ""


def test_pipeline_filters_the_skip_notice_across_stderr_files(tmp_path):
    """Arm 4: multi-server layout - the range opens in one file, the notice is in the next.

    `stderr*.log` globs to several files on a multi-server run (e.g.
    DatabaseReplicated) and `sed` runs without `-s`, so the open range carries
    over from `stderr.log` into `stderr1.log`. `stderr1.log` holds nothing but
    the notice: any other line there would be blamed first (nothing else the
    server prints is allow-listed) and would mask what this arm measures.
    """
    _write(
        tmp_path,
        **{
            "stderr.log": ASAN_STACK_SIZE_BLOCK,
            "stderr1.log": [SKIP_NOTICE],
        },
    )
    assert _detect(tmp_path) == ""


def test_pipeline_does_not_over_match_the_skip_notice(tmp_path):
    """Arm 5: a lookalike differing *before* the notice's text is not filtered."""
    lookalike = f"Not running the leak check for real: {REAL_REPORT}"
    _write(tmp_path, **{"stderr.log": ASAN_STACK_SIZE_BLOCK + [lookalike]})
    assert _detect(tmp_path) == lookalike


def test_pipeline_reports_a_leak_sharing_a_line_with_the_notice(tmp_path):
    """Arm 5b: a report that *contains* the notice is still blamed.

    A substring filter drops any line merely containing the notice, which
    silently loses this report - the whole-line match is what keeps it. The
    notice can only ever be a complete line (`safeExit()` writes one fixed
    buffer, newline included, in a single `write(2)`), so matching the whole
    line costs no coverage.
    """
    shared = f"{REAL_REPORT} {SKIP_NOTICE}"
    _write(tmp_path, **{"stderr.log": ASAN_STACK_SIZE_BLOCK + [shared]})
    assert _detect(tmp_path) == shared
