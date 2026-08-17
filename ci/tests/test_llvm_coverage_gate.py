"""
Regression tests for the LLVM Coverage diff gate tolerance check.

A drop exactly equal to the 0.3 pp tolerance must pass, as the gate's own
message states. `coverage_drop` rounds the difference so the binary-float
representation of a decimal subtraction cannot push it over the threshold.
"""

import os
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.llvm_coverage_job import (
    COVERAGE_DROP_TOLERANCE,
    coverage_degraded,
    coverage_drop,
)
from ci.praktika.result import Result

_JOB = os.path.join(os.path.dirname(__file__), "..", "jobs", "llvm_coverage_job.py")
_DIFF_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "scripts", "generate_diff_coverage_report.sh"
)


def _degraded(baseline: float, current: float) -> bool:
    """The gate's verdict, driving both production helpers rather than a copy."""
    return coverage_degraded(coverage_drop(baseline, current))


def _gate_snippet() -> str:
    """The verdict block from llvm_coverage_job.py, verbatim.

    The gate lives inside `if __name__ == "__main__":`, so it cannot be imported;
    exec'ing its own source keeps this test honest about what the job really does.
    """
    lines = open(_JOB, encoding="utf-8").read().splitlines(True)
    start = next(i for i, l in enumerate(lines) if "_drop = coverage_drop(" in l)
    end = next(i for i, l in enumerate(lines) if "diff_res.set_failed()" in l)
    return textwrap.dedent("".join(lines[start : end + 1]))


class _ResultStub:
    """Captures the three side effects the gate has on its Result."""

    def __init__(self):
        self.info = None
        self.comment = None
        self.failed = False

    def set_comment(self, msg):
        self.comment = msg

    def set_failed(self):
        self.failed = True


def _run_gate(baseline: float, current: float) -> _ResultStub:
    """Execute the job's own verdict block and report what it did to the Result."""
    res = _ResultStub()
    ns = {
        "coverage_drop": coverage_drop,
        "coverage_degraded": coverage_degraded,
        "COVERAGE_DROP_TOLERANCE": COVERAGE_DROP_TOLERANCE,
        "b_line_cov": baseline,
        "c_line_cov": current,
        "diff_res": res,
        "print": lambda *a, **k: None,
    }
    exec(_gate_snippet(), ns)  # noqa: S102 - trusted first-party source
    return res


def test_gate_snippet_is_the_real_verdict_block():
    # Without this the extraction could silently degenerate and make every
    # _run_gate assertion below vacuous.
    src = _gate_snippet()
    assert "coverage_drop(" in src
    assert "coverage_degraded(" in src
    assert "diff_res.set_failed()" in src


def test_tolerance_is_unchanged():
    assert COVERAGE_DROP_TOLERANCE == 0.3


def test_drop_equal_to_tolerance_passes():
    # The only two value pairs behind all 21 observed failures.
    assert not _degraded(84.4, 84.1)
    assert not _degraded(85.4, 85.1)


def test_old_expression_did_fire_on_those_pairs():
    # Without this the suite cannot tell the fixed and broken versions apart.
    assert 84.4 - 84.1 > COVERAGE_DROP_TOLERANCE
    assert 85.4 - 85.1 > COVERAGE_DROP_TOLERANCE


def test_drop_above_tolerance_still_fails():
    assert _degraded(86.30, 85.99)
    assert _degraded(86.3, 85.8)


def test_large_drop_still_fails():
    # The shape reported on PR #105684; the gate must not be disabled.
    assert _degraded(86.20, 28.60)


def test_coverage_increase_passes():
    assert not _degraded(86.53, 86.54)


def test_contract_over_full_range():
    # For every one-decimal baseline, the verdict must be `drop > 0.3`.
    mismatches = []
    for step in range(0, 1001):
        baseline = step / 10.0
        for drop in ("0.29", "0.30", "0.31", "0.35", "0.40"):
            current = round(baseline - float(drop), 2)
            if current < 0:
                continue
            expected = float(drop) > COVERAGE_DROP_TOLERANCE
            if _degraded(baseline, current) != expected:
                mismatches.append((baseline, current, drop))
    assert mismatches == [], f"{len(mismatches)} mismatches, first: {mismatches[:5]}"


def test_message_reports_the_value_it_compared():
    baseline, current = 86.30, 85.99
    drop = coverage_drop(baseline, current)
    assert _degraded(baseline, current)
    # The gate interpolates this same value, so the printed number cannot
    # disagree with the number judged.
    assert f"{drop:.2f}" == "0.31"


def test_gate_passes_a_drop_equal_to_tolerance():
    # Drives the production call site, not just the helpers it wires together.
    assert _run_gate(84.4, 84.1).failed is False
    assert _run_gate(85.4, 85.1).failed is False


def test_gate_fails_a_drop_above_tolerance_with_the_value_it_judged():
    res = _run_gate(86.30, 85.99)
    assert res.failed is True
    assert res.comment == (
        "Coverage degraded: master 86.30% \u2192 PR 85.99%"
        " (dropped 0.31 pp, tolerance 0.3 pp)"
    )
    assert res.info == res.comment


def test_gate_still_fails_the_large_drop():
    res = _run_gate(86.20, 28.60)
    assert res.failed is True
    assert "dropped 57.60 pp" in res.comment


# ---------------------------------------------------------------------------
# The diff step has four outcomes; the job must report the one that happened.
#
# generate_diff_coverage_report.sh exits 0 in three distinct states and dies
# under `set -euo pipefail` in a fourth. All four leave no report directory, so
# a job that infers the reason from that directory reports three of them as
# "No C/C++ source files changed" - false on two states that are green today.
# ---------------------------------------------------------------------------

_NO_CPP_CLAIM = "No C/C++ source files changed"

# The marker tokens are the wire format between the script and the job, so they
# are written literally here rather than imported: a rename must break a test.
_MARKER_NO_CPP = "no_cpp_changes"
_MARKER_NO_DATA = "no_coverage_data"
_MARKER_EMPTY = "current_coverage_empty"
_MARKER_REPORT = "report_generated"

_EXIT_0_MARKERS = (_MARKER_NO_CPP, _MARKER_NO_DATA, _MARKER_EMPTY)


def _job_source() -> str:
    return open(_JOB, encoding="utf-8").read()


def _script_source() -> str:
    return open(_DIFF_SCRIPT, encoding="utf-8").read()


def _else_body_ending_at(needle: str) -> str:
    """The body of the `else:` branch that contains `needle`, from the job source.

    Extracting the job's own source keeps these tests honest: a copy of the
    reporting logic would keep passing after the job stopped matching it.
    """
    lines = _job_source().splitlines(True)
    end = next(i for i, l in enumerate(lines) if needle in l)
    start = next(i for i in range(end, -1, -1) if lines[i].rstrip() == "        else:")
    body = lines[start + 1 : end + 1]
    assert body, needle
    return textwrap.dedent("".join(body))


def _diff_report_snippet() -> str:
    """Where the job says why no differential report exists."""
    return _else_body_ending_at("diff_res.info = _diff_msg")


def _print_uncovered_snippet() -> str:
    """Where the job builds the `Print Uncovered Code` sub-result without input."""
    return _else_body_ending_at("print_res.set_comment(msg)")


def _coverage_comment_snippet() -> str:
    """Where the job says why it posts no coverage comment."""
    lines = _job_source().splitlines(True)
    start = next(i for i, l in enumerate(lines) if "if not _has_coverage_data:" in l)
    end = next(i for i in range(start + 1, len(lines)) if "print(" in lines[i])
    return textwrap.dedent("".join(lines[start : end + 1]))


class _DiffResultStub:
    """The two things the reporting block may do to a failed/ok diff result."""

    def __init__(self, ok: bool):
        self._ok = ok
        self.info = None
        self.comment = None
        self.failed = not ok
        self.files = []
        self.assets = []

    def is_ok(self):
        return self._ok

    def set_comment(self, msg):
        self.comment = msg

    def set_failed(self):
        self.failed = True

    def set_success(self):
        self.failed = False
        self._ok = True


def _outcome(script_ok: bool, marker: str, tmp_path) -> object:
    """The job's own verdict on a real-world diff-step state.

    Drives the production marker reader and classifier through a real marker
    file, so the file protocol is under test too. Returns None on a job that has
    no outcome model yet - its reporting block does not consult one.
    """
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    classify = getattr(job, "classify_diff_outcome", None)
    if classify is None:
        return None
    if marker:
        (tmp_path / "diff_outcome.txt").write_text(marker + "\n", encoding="utf-8")
    return classify(
        script_ok=script_ok,
        marker=job.read_diff_outcome_marker(str(tmp_path)),
        report_ready=(marker == _MARKER_REPORT),
    )


def _run_snippet(snippet: str, **overrides):
    """Execute a production reporting block and collect what it printed."""
    printed = []
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    ns = {k: v for k, v in vars(job).items() if not k.startswith("__")}
    ns.update(
        {
            "Result": Result,
            "print": lambda *a, **k: printed.append(" ".join(str(x) for x in a)),
        }
    )
    ns.update(overrides)
    exec(snippet, ns)  # noqa: S102 - trusted first-party source
    return "\n".join(printed), ns


def _reported_reasons(script_ok: bool, marker: str, tmp_path) -> str:
    """Every reason the job gives the reader for the absence of a report."""
    outcome = _outcome(script_ok, marker, tmp_path)
    diff_res = _DiffResultStub(ok=script_ok)
    out = []
    for snippet, extra in (
        (_diff_report_snippet(), {"diff_res": diff_res}),
        (_print_uncovered_snippet(), {}),
        (_coverage_comment_snippet(), {"_has_coverage_data": False}),
    ):
        text, _ = _run_snippet(
            snippet, _diff_outcome=outcome, _diff_ran=False, **extra
        )
        out.append(text)
    return "\n".join(out)


def test_reporting_snippets_are_the_real_production_blocks():
    # Without this every assertion below could go vacuous through a silently
    # degenerate extraction.
    assert "diff_res" in _diff_report_snippet()
    assert "Print Uncovered Code" in _print_uncovered_snippet()
    assert "Result.create_from" in _print_uncovered_snippet()
    assert "_has_coverage_data" in _coverage_comment_snippet()


def test_tool_failure_is_not_reported_as_no_cpp_changes(tmp_path):
    # Outcome 4: the script died, so nothing is known about the changed files.
    text = _reported_reasons(script_ok=False, marker="", tmp_path=tmp_path)
    assert _NO_CPP_CLAIM not in text, text
    assert "generate_diff_coverage_report.sh" in text, text


def test_tool_failure_keeps_print_uncovered_code_not_ok(tmp_path):
    # An analysis that never ran is not a success.
    outcome = _outcome(script_ok=False, marker="", tmp_path=tmp_path)
    _, ns = _run_snippet(
        _print_uncovered_snippet(), _diff_outcome=outcome, _diff_ran=False
    )
    assert ns["print_res"].status != Result.Status.OK


def test_genuine_no_cpp_changes_still_says_so(tmp_path):
    # Outcome 1 is the one state the existing wording is true for; the fix must
    # not remove it. This is the arm that stops it over-firing.
    text = _reported_reasons(script_ok=True, marker=_MARKER_NO_CPP, tmp_path=tmp_path)
    assert _NO_CPP_CLAIM in text, text


def test_no_coverage_data_is_not_called_no_cpp_changes(tmp_path):
    # Outcome 2: the C++ files were found and their patterns extracted; lcov
    # simply had no records for them.
    text = _reported_reasons(script_ok=True, marker=_MARKER_NO_DATA, tmp_path=tmp_path)
    assert _NO_CPP_CLAIM not in text, text
    assert "coverage data" in text, text


def test_current_coverage_empty_is_not_called_no_cpp_changes(tmp_path):
    # Outcome 3: same, but only the current side is empty.
    text = _reported_reasons(script_ok=True, marker=_MARKER_EMPTY, tmp_path=tmp_path)
    assert _NO_CPP_CLAIM not in text, text
    assert "empty" in text.lower(), text


@pytest.mark.parametrize("marker", _EXIT_0_MARKERS)
def test_the_three_exit_0_outcomes_stay_green(marker, tmp_path):
    # None of the script's exit-0 states may become a new red.
    outcome = _outcome(script_ok=True, marker=marker, tmp_path=tmp_path)
    diff_res = _DiffResultStub(ok=True)
    _run_snippet(
        _diff_report_snippet(),
        _diff_outcome=outcome,
        _diff_ran=False,
        diff_res=diff_res,
    )
    assert diff_res.failed is False
    _, ns = _run_snippet(
        _print_uncovered_snippet(), _diff_outcome=outcome, _diff_ran=False
    )
    assert ns["print_res"].status == Result.Status.OK


def test_tool_failure_still_fails_the_job(tmp_path):
    # The pinned contract: a `set -euo pipefail` failure of the script stays RED.
    # This changes the label, never the verdict.
    outcome = _outcome(script_ok=False, marker="", tmp_path=tmp_path)
    diff_res = _DiffResultStub(ok=False)
    _run_snippet(
        _diff_report_snippet(),
        _diff_outcome=outcome,
        _diff_ran=False,
        diff_res=diff_res,
    )
    assert diff_res.failed is True


def test_current_coverage_empty_does_not_promise_lbc_analysis():
    # print_uncovered_code.py reads only current.changed.info, the file that is
    # empty in this state, so the promised analysis produces nothing.
    assert "LBC analysis will run separately" not in _script_source()


def test_script_names_its_outcome_at_every_exit_0():
    src = _script_source()
    # A stale marker from an earlier step must not be readable as this run's.
    assert 'rm -f "$OUTCOME_MARKER"' in src
    for token in _EXIT_0_MARKERS + (_MARKER_REPORT,):
        assert f'echo "{token}" > "$OUTCOME_MARKER"' in src, token
    # One marker per exit-0 path plus the success path, and no silent fifth one.
    assert src.count('> "$OUTCOME_MARKER"') == 4
    assert src.count("exit 0") == 3


def test_gh_api_stderr_is_not_redirected():
    # Shell.run feeds err_output from stderr and classifies retries on it, so a
    # redirection here would silently disable that for every future caller.
    for line in _script_source().splitlines():
        if "gh api" in line or "repos/ClickHouse/ClickHouse/compare" in line:
            assert "2>&1" not in line, line
            assert "2>" not in line, line


def test_gh_api_calls_name_the_endpoint_they_request():
    # `gh` reports a failure as a bare "gh: Not Found (HTTP 404)" naming no
    # resource, so the URL has to be in the log independently.
    src = _script_source()
    assert "Fetching diff: repos/ClickHouse/ClickHouse/compare/" in src
    assert "Fetching changed files: repos/ClickHouse/ClickHouse/compare/" in src
