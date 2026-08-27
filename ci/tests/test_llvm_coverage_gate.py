"""
Regression tests for the LLVM Coverage diff gate tolerance check.

A drop exactly equal to the 0.3 pp tolerance must pass, as the gate's own
message states. `coverage_drop` rounds the difference so the binary-float
representation of a decimal subtraction cannot push it over the threshold.
"""

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

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


# --- Completeness: the shard-profile presence check --------------------------
#
# A verdict may only be derived from a complete measurement. The job compares
# the profiles on disk against the expected set derived from the coverage
# artifact manifest; these tests pin the set logic the SKIPPED decision runs on.

from ci.defs.defs import LLVM_ARTIFACTS_LIST
from ci.jobs.llvm_coverage_job import (
    expected_profile_files,
    missing_profile_files,
    present_profile_files,
)


def test_expected_profiles_cover_every_coverage_artifact():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    assert len(expected) == len(LLVM_ARTIFACTS_LIST)
    assert all(f.endswith(".profdata") for f in expected)
    # One profile per artifact: a duplicate artifact name would silently weaken
    # the completeness check to fewer files than shards.
    assert len(set(expected)) == len(expected)


def test_all_profiles_present_means_nothing_missing():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    assert missing_profile_files(expected, list(expected)) == []


def test_one_absent_shard_is_reported_by_name():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    present = [f for f in expected if f != expected[3]]
    assert missing_profile_files(expected, present) == [expected[3]]


def test_extra_files_are_not_an_error_and_do_not_mask_a_missing_shard():
    # A stale merged.profdata (or a foreign leftover) must neither redden the
    # run nor hide that an expected shard is absent.
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    present = [f for f in expected if f != expected[0]] + ["merged.profdata"]
    assert missing_profile_files(expected, present) == [expected[0]]


def test_present_profiles_lists_only_profdata_files(tmp_path):
    (tmp_path / "a.profdata").write_bytes(b"x")
    (tmp_path / "b.profraw").write_bytes(b"x")
    (tmp_path / "clickhouse").write_bytes(b"x")
    (tmp_path / "sub").mkdir()
    assert present_profile_files(str(tmp_path)) == ["a.profdata"]


def test_present_profiles_of_missing_directory_is_empty():
    assert present_profile_files("/nonexistent/definitely/not/here") == []
# ---------------------------------------------------------------------------
# The diff step reaches four report-less states, and the job must report the one
# that happened. generate_diff_coverage_report.sh exits 0 in three of them and
# dies under `set -euo pipefail` in the fourth; none leaves a report directory,
# so that directory alone cannot tell them apart.
# ---------------------------------------------------------------------------

# Specific to outcome 1. The other reasons also mention "C/C++ source files", so a
# shorter pin would make every "the claim is absent" arm below vacuous.
_NO_CPP_CLAIM = "No coverable C/C++ source files changed"

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


def _wiring_snippet() -> str:
    """Where the job turns the diff step's real on-disk state into an outcome.

    The classifier is only as good as the arguments the job hands it, so this
    block is executed rather than reconstructed: a call site that stops reading
    the marker, or derives `_diff_ran` some other way, has to show up here.
    """
    lines = _job_source().splitlines(True)
    start = next(i for i, l in enumerate(lines) if "_diff_report_dir = Path(TEMP_DIR)" in l)
    end = next(i for i in range(start + 1, len(lines)) if "_diff_ran =" in lines[i])
    return textwrap.dedent("".join(lines[start : end + 1]))


def _diff_inputs_snippet() -> str:
    """Where the job decides whether print_uncovered_code.py has this run's data."""
    lines = _job_source().splitlines(True)
    start = next(i for i, l in enumerate(lines) if "_diff_inputs_exist = (" in l)
    end = next(i for i in range(start + 1, len(lines)) if lines[i].rstrip() == "        )")
    return textwrap.dedent("".join(lines[start : end + 1]))


def _line_below(anchor: str) -> int:
    """Index of the first non-blank line below the single line holding `anchor`.

    The job has four textually identical `if _diff_ran:` lines and two
    `if _diff_inputs_exist:` lines, so a guard cannot be located by its own text:
    a search would land on whichever one comes first, and mutating the intended
    guard makes such a search slide silently to the next copy. Position relative
    to a unique neighbour is the only stable handle, and what it selects is
    asserted separately.
    """
    lines = _job_source().splitlines(True)
    matches = [i for i, l in enumerate(lines) if anchor in l]
    assert len(matches) == 1, (anchor, len(matches))
    return next(i for i in range(matches[0] + 1, len(lines)) if lines[i].strip())


# Last line of the zeroed-counters block, so the guard is the next one.
_REPORT_GUARD_ANCHOR = "b_branch_hit = b_branch_total = c_branch_hit"
_ANALYSIS_GUARD_ANCHOR = "_print_log = f\"{TEMP_DIR}"


def _report_guard_body() -> str:
    """What the guarded coverage-report branch does, for identifying the site."""
    lines = _job_source().splitlines(True)
    start = _line_below(_REPORT_GUARD_ANCHOR)
    return "".join(lines[start : start + 30])


def _report_guard_snippet() -> str:
    """The `if _diff_ran:` guard admitting the coverage-report branch.

    It is the line below the zeroed coverage counters; the other three guard the
    log tail, the S3 link list and the archive list.
    """
    lines = _job_source().splitlines(True)
    idx = _line_below(_REPORT_GUARD_ANCHOR)
    return textwrap.dedent(lines[idx]) + "    _took_report_branch = True\n"


def _analysis_dispatch_snippet() -> str:
    """The `if _diff_inputs_exist:` block that runs print_uncovered_code.py.

    It is the first guard below the log path the analysis writes to; the later
    one only attaches that log to an already-built result.
    """
    lines = _job_source().splitlines(True)
    start = next(
        i
        for i in range(_line_below(_ANALYSIS_GUARD_ANCHOR), len(lines))
        if lines[i].lstrip().startswith("if ")
    )
    end = next(
        i for i in range(start + 1, len(lines)) if "print_res.set_comment(msg)" in lines[i]
    )
    return textwrap.dedent("".join(lines[start : end + 1]))


def _comment_dispatch_snippet() -> str:
    """The block deciding whether a coverage comment is written for this run."""
    lines = _job_source().splitlines(True)
    start = next(
        i for i, l in enumerate(lines) if "_has_coverage_data = _diff_ran" in l
    )
    end = next(
        i
        for i in range(start + 1, len(lines))
        if lines[i].rstrip() == "            else:"
    )
    # Dedent before appending the sentinel: a shallower line in the input would
    # shrink dedent's common prefix and leave the block itself indented.
    return textwrap.dedent("".join(lines[start : end + 1])) + "    _made_comment = True\n"


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


def _outcome(script_ok: bool, marker: str, tmp_path, report_ready=None) -> object:
    """The job's own verdict on a real-world diff-step state.

    Drives the production marker reader and classifier through a real marker
    file, so the file protocol is under test too. `report_ready` is an
    independent input and must stay passable on its own: tying it to `marker`
    makes the state where the two contradict unreachable. Returns None on a job
    that has no outcome model yet - its reporting block does not consult one.
    """
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    classify = getattr(job, "classify_diff_outcome", None)
    if classify is None:
        return None
    if marker:
        (tmp_path / "diff_outcome.txt").write_text(marker + "\n", encoding="utf-8")
    if report_ready is None:
        report_ready = marker == _MARKER_REPORT
    return classify(
        script_ok=script_ok,
        marker=job.read_diff_outcome_marker(str(tmp_path)),
        report_ready=report_ready,
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


def _seed_diff_state(tmp_path, marker: str, report_ready: bool):
    """Write the files the job reads: the real marker and the real report index."""
    if marker:
        (tmp_path / "diff_outcome.txt").write_text(marker + "\n", encoding="utf-8")
    if report_ready:
        report_dir = tmp_path / "llvm_coverage_diff_html_report"
        report_dir.mkdir(exist_ok=True)
        (report_dir / "index.html").write_text("<html></html>", encoding="utf-8")


def _wired_outcome(tmp_path, marker: str, report_ready: bool, script_ok: bool):
    """Run the job's own call site over real files. Returns (outcome, diff_ran)."""
    _seed_diff_state(tmp_path, marker, report_ready)
    # TEMP_DIR ends in a separator in production and some blocks concatenate it
    # as a string, so a value without one probes a different path.
    _, ns = _run_snippet(
        _wiring_snippet(),
        TEMP_DIR=str(tmp_path) + os.sep,
        diff_res=_DiffResultStub(ok=script_ok),
        Path=Path,
    )
    return ns["_diff_outcome"], ns["_diff_ran"]


def _wired_inputs_exist(tmp_path, marker: str, report_ready: bool, script_ok: bool):
    """Whether the job would run print_uncovered_code.py over this state."""
    outcome, _ = _wired_outcome(tmp_path, marker, report_ready, script_ok)
    _, ns = _run_snippet(
        _diff_inputs_snippet(),
        TEMP_DIR=str(tmp_path) + os.sep,
        _diff_outcome=outcome,
        Path=Path,
    )
    return ns["_diff_inputs_exist"]


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
    assert "classify_diff_outcome" in _wiring_snippet()
    assert "_diff_ran" in _wiring_snippet()
    assert "changes.diff" in _diff_inputs_snippet()
    assert "_diff_inputs_exist" in _diff_inputs_snippet()


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


def test_each_endpoint_echo_names_its_own_anchor():
    # The two ranges are deliberately different (the script documents why), so a
    # diagnostic that prints the other call's range misattributes the 404.
    src = _script_source()
    assert (
        'echo "Fetching diff: repos/ClickHouse/ClickHouse/compare/'
        '${FIRST_BASE_COMMIT}...${CURRENT_COMMIT}"' in src
    )
    assert (
        'echo "Fetching changed files: repos/ClickHouse/ClickHouse/compare/'
        '${BASE_COMMIT}...${CURRENT_COMMIT}"' in src
    )


def _named_outcomes() -> list:
    """Every outcome token DiffOutcome names, derived from the class itself."""
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    cls = job.DiffOutcome
    names = [
        getattr(cls, a)
        for a in vars(cls)
        if a.isupper() and isinstance(getattr(cls, a), str)
    ]
    assert len(names) >= 6, names
    return names


def test_every_outcome_has_a_reason():
    # The message helpers index the reason table directly, so an outcome missing
    # from it kills the job with a KeyError instead of reporting anything.
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    for outcome in _named_outcomes():
        for helper in (
            job.diff_report_message,
            job.uncovered_code_message,
            job.coverage_comment_message,
            job.coverage_marker_reason,
        ):
            assert helper(outcome).strip(), (outcome, helper.__name__)


@pytest.mark.parametrize("marker", _EXIT_0_MARKERS)
def test_this_runs_marker_outranks_a_leftover_report_directory(marker, tmp_path):
    # A report directory is never removed by either side, so a previous run's can
    # outlive it. The state this run declared is the one that must be reported.
    assert _outcome(True, marker, tmp_path, report_ready=True) == marker


def test_a_marker_less_script_that_generated_a_report_still_reports_one(tmp_path):
    assert _outcome(True, "", tmp_path, report_ready=True) == _MARKER_REPORT


def test_a_marker_less_script_with_no_report_reports_no_outcome(tmp_path):
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    outcome = _outcome(True, "", tmp_path, report_ready=False)
    assert outcome == job.DiffOutcome.UNKNOWN
    text = _reported_reasons(script_ok=True, marker="", tmp_path=tmp_path)
    assert _NO_CPP_CLAIM not in text, text
    assert "reported no outcome" in text, text


def test_a_reported_report_runs_the_diff_branch(tmp_path):
    assert _outcome(True, _MARKER_REPORT, tmp_path, report_ready=False) == _MARKER_REPORT


# ---------------------------------------------------------------------------
# The production call site, driven over real files.
#
# Everything above proves things about classify_diff_outcome. These prove the
# job calls it with the state it is looking at, and derives _diff_ran from the
# answer -- otherwise a correct classifier can sit next to a call site that
# ignores it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "marker,report_ready,script_ok,want_outcome,want_diff_ran",
    [
        # A marker on disk outranks a report directory an earlier run left.
        (_MARKER_NO_CPP, True, True, _MARKER_NO_CPP, False),
        # _diff_ran follows the outcome, not the directory listing.
        (_MARKER_REPORT, False, True, _MARKER_REPORT, True),
        # No marker and no report: the job knows it knows nothing.
        ("", False, True, "unknown", False),
        # A non-zero exit outranks every artifact on disk.
        (_MARKER_REPORT, True, False, "failed", False),
    ],
)
def test_the_job_reads_the_state_it_is_looking_at(
    marker, report_ready, script_ok, want_outcome, want_diff_ran, tmp_path
):
    outcome, diff_ran = _wired_outcome(tmp_path, marker, report_ready, script_ok)
    assert (outcome, diff_ran) == (want_outcome, want_diff_ran)


@pytest.mark.parametrize(
    "marker,inputs_expected",
    [
        # The only outcome whose slice is this run's own and holds records.
        (_MARKER_REPORT, True),
        # This run's slice, but with no records in it at all.
        (_MARKER_EMPTY, False),
        # No slice of this run's, so a file present here is an earlier run's.
        (_MARKER_NO_CPP, False),
        (_MARKER_NO_DATA, False),
    ],
)
def test_uncovered_code_analysis_only_reads_this_runs_data(
    marker, inputs_expected, tmp_path
):
    # Both files present on disk, as a repeated run in one directory leaves them.
    (tmp_path / "changes.diff").write_text("diff --git a/x b/x\n", encoding="utf-8")
    (tmp_path / "current.changed.info").write_text(
        "SF:/src/Foo.cpp\nDA:1,1\nend_of_record\n", encoding="utf-8"
    )
    assert (
        _wired_inputs_exist(tmp_path, marker, report_ready=False, script_ok=True)
        is inputs_expected
    )


def test_a_failed_script_never_reads_leftover_coverage_data(tmp_path):
    # The 404 path: the script died between writing changes.diff and
    # current.changed.info, so a present pair is not this run's.
    (tmp_path / "changes.diff").write_text("diff --git a/x b/x\n", encoding="utf-8")
    (tmp_path / "current.changed.info").write_text(
        "SF:/src/Foo.cpp\nDA:1,1\nend_of_record\n", encoding="utf-8"
    )
    assert (
        _wired_inputs_exist(tmp_path, "", report_ready=False, script_ok=False) is False
    )


def test_the_report_outcome_still_reads_its_own_inputs(tmp_path):
    # The gate must not cost the one outcome that does have data.
    (tmp_path / "changes.diff").write_text("diff --git a/x b/x\n", encoding="utf-8")
    (tmp_path / "current.changed.info").write_text(
        "SF:/src/Foo.cpp\nDA:1,1\nend_of_record\n", encoding="utf-8"
    )
    assert (
        _wired_inputs_exist(tmp_path, _MARKER_REPORT, report_ready=True, script_ok=True)
        is True
    )


def test_a_missing_input_file_still_blocks_the_analysis(tmp_path):
    # The outcome says the run should have produced them; the file checks confirm
    # it did, so both halves of the predicate stay load-bearing.
    (tmp_path / "changes.diff").write_text("diff --git a/x b/x\n", encoding="utf-8")
    assert (
        _wired_inputs_exist(tmp_path, _MARKER_REPORT, report_ready=True, script_ok=True)
        is False
    )


# ---------------------------------------------------------------------------
# The marker protocol, driven through the real script.
#
# The assertions above read the script as text, which cannot tell whether a
# branch writes the token belonging to a different branch. These run it with
# stubbed gh/wget/lcov/genhtml and assert the token each outcome produces.
# ---------------------------------------------------------------------------

# The diff fetch is anchored at the baseline commit and the changed-file fetch at
# the PR merge base. These three values must stay pairwise distinct, or an
# assertion on one endpoint would also accept the other one's range.
_FIRST_BASE = "fbc111"
_BASE = "bc999"
_CURRENT = "cur777"

_STUBS = {
    # `wget --spider` is grepped for '200 OK'; the download must land a file.
    "wget": """#!/bin/bash
for a in "$@"; do [ "$a" = "--spider" ] && echo "200 OK" && exit 0; done
out=""; prev=""
for a in "$@"; do [ "$prev" = "-O" ] && out="$a"; prev="$a"; done
[ -n "$out" ] && echo "TN:" > "$out"
exit 0
""",
    # The two compare calls are told apart by --jq, so either can be failed on
    # its own: STUB_GH_RC the diff fetch, STUB_GH_RC_JQ the changed-file fetch.
    "gh": """#!/bin/bash
case "$*" in
  *--jq*)
    if [ "${STUB_GH_RC_JQ:-0}" != "0" ]; then
      echo "gh: Not Found (HTTP 404)" >&2
      exit "$STUB_GH_RC_JQ"
    fi
    printf '%s\\n' ${STUB_CHANGED_FILES}
    ;;
  *)
    if [ "${STUB_GH_RC:-0}" != "0" ]; then
      echo "gh: Not Found (HTTP 404)" >&2
      exit "$STUB_GH_RC"
    fi
    echo "diff --git a/x b/x"
    ;;
esac
exit 0
""",
    # Writes -o with or without an SF: record, per side.
    "lcov": """#!/bin/bash
src=""; out=""; prev=""
for a in "$@"; do
  [ "$prev" = "--extract" ] && src="$a"
  [ "$prev" = "-o" ] && out="$a"
  prev="$a"
done
want=SF
case "$src" in
  llvm_coverage.info) [ "${STUB_CURRENT_SF:-1}" = "1" ] || want="" ;;
  *) [ "${STUB_BASELINE_SF:-1}" = "1" ] || want="" ;;
esac
: > "$out"
[ -n "$want" ] && printf 'SF:/src/Foo.cpp\\nDA:1,1\\nend_of_record\\n' > "$out"
exit 0
""",
    "genhtml": """#!/bin/bash
out=""; prev=""
for a in "$@"; do [ "$prev" = "--output-directory" ] && out="$a"; prev="$a"; done
mkdir -p "$out" && echo "<html></html>" > "$out/index.html"
exit 0
""",
}


def _run_diff_script(tmp_path, changed_files: str, **stub_env):
    """Run the real diff script in a sandbox. Returns (rc, marker, report, log)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True)
    for name, body in _STUBS.items():
        stub = bin_dir / name
        stub.write_text(body, encoding="utf-8")
        stub.chmod(0o755)

    workspace = tmp_path / "ws"
    ci_tmp = workspace / "ci" / "tmp"
    ci_tmp.mkdir(parents=True)
    (ci_tmp / "llvm_coverage.info").write_text("TN:\n", encoding="utf-8")
    # Every case starts from a stale marker, so a branch that writes none is
    # distinguishable from one that leaves a previous run's token behind.
    (ci_tmp / "diff_outcome.txt").write_text(_MARKER_REPORT + "\n", encoding="utf-8")

    env = dict(os.environ)
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PREV_30_COMMITS": f"{_FIRST_BASE},{_FIRST_BASE}222",
            "CURRENT_COMMIT": _CURRENT,
            "BASE_COMMIT": _BASE,
            "BRANCH": "topic",
            "BASE_BRANCH": "master",
            "WORKSPACE_PATH": str(workspace),
            "PR_NUMBER": "",
            "REPO_NAME": "ClickHouse",
            "STUB_CHANGED_FILES": changed_files,
        }
    )
    env.update({k: str(v) for k, v in stub_env.items()})

    proc = subprocess.run(
        ["bash", os.path.abspath(_DIFF_SCRIPT)],
        cwd=str(workspace),
        env=env,
        capture_output=True,
        text=True,
    )
    marker_file = ci_tmp / "diff_outcome.txt"
    marker = (
        marker_file.read_text(encoding="utf-8").strip() if marker_file.exists() else ""
    )
    report = (ci_tmp / "llvm_coverage_diff_html_report" / "index.html").exists()
    return proc.returncode, marker, report, proc.stdout + proc.stderr


def _stubs_are_executable(tmp_path) -> bool:
    """Whether a stub written here can be run, e.g. not a noexec mount."""
    probe = tmp_path / "probe.sh"
    probe.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    probe.chmod(0o755)
    try:
        return subprocess.run([str(probe)], capture_output=True).returncode == 0
    except OSError:
        return False


@pytest.fixture(scope="module")
def _script_runs(tmp_path_factory):
    if not shutil.which("bash"):
        pytest.skip("bash is required to drive the diff script")
    root = tmp_path_factory.mktemp("diffscript")
    if not _stubs_are_executable(root):
        pytest.skip("cannot execute a stub from the temporary directory")
    cases = {
        "no_cpp": ("docs/readme.md ci/foo.py", {}),
        "no_data": ("src/Foo.cpp", {"STUB_CURRENT_SF": 0, "STUB_BASELINE_SF": 0}),
        "empty": ("src/Foo.cpp", {"STUB_CURRENT_SF": 0, "STUB_BASELINE_SF": 1}),
        "gh_404": ("src/Foo.cpp", {"STUB_GH_RC": 1}),
        # The second compare call fails on its own, so its endpoint line is the
        # only one that can attribute the 404 to a range.
        "gh_404_files": ("src/Foo.cpp", {"STUB_GH_RC_JQ": 1}),
        "report": ("src/Foo.cpp", {}),
    }
    return {
        name: _run_diff_script(root / name, files, **env)
        for name, (files, env) in cases.items()
    }


@pytest.mark.parametrize(
    "case,marker",
    [
        ("no_cpp", _MARKER_NO_CPP),
        ("no_data", _MARKER_NO_DATA),
        ("empty", _MARKER_EMPTY),
        ("report", _MARKER_REPORT),
    ],
)
def test_the_script_writes_the_marker_belonging_to_its_outcome(
    case, marker, _script_runs
):
    rc, written, _, log = _script_runs[case]
    assert rc == 0, log
    assert written == marker, log


def test_the_script_generates_a_report_only_in_the_report_outcome(_script_runs):
    for case, (_, _, report, log) in _script_runs.items():
        assert report is (case == "report"), (case, log)


def test_the_fixture_anchors_are_pairwise_distinct():
    # Every endpoint assertion below can only discriminate while these differ; if
    # a future edit collapses them the arms would silently accept either range.
    assert len({_FIRST_BASE, _BASE, _CURRENT}) == 3


def test_a_failing_gh_api_leaves_no_marker_and_names_its_endpoint(_script_runs):
    rc, marker, _, log = _script_runs["gh_404"]
    assert rc != 0, log
    # The stale marker must not be readable as this run's outcome.
    assert marker == "", log
    assert (
        f"Fetching diff: repos/ClickHouse/ClickHouse/compare/"
        f"{_FIRST_BASE}...{_CURRENT}" in log
    ), log
    # gh's own stderr has to survive to the log for Shell.run's classification.
    assert "gh: Not Found (HTTP 404)" in log, log


def test_a_failing_changed_files_call_names_its_own_range(_script_runs):
    # The second call is anchored at the PR merge base, not the baseline commit,
    # so its endpoint line is the only one that attributes this 404 correctly.
    rc, marker, _, log = _script_runs["gh_404_files"]
    assert rc != 0, log
    assert marker == "", log
    assert (
        f"Fetching changed files: repos/ClickHouse/ClickHouse/compare/"
        f"{_BASE}...{_CURRENT}" in log
    ), log
    assert "gh: Not Found (HTTP 404)" in log, log


def test_the_report_run_names_both_ranges_with_their_own_anchors(_script_runs):
    # A successful run reaches both echoes, which is where the two ranges can be
    # seen to differ; the 404 cases each reach only one.
    _, _, _, log = _script_runs["report"]
    assert (
        f"Fetching diff: repos/ClickHouse/ClickHouse/compare/"
        f"{_FIRST_BASE}...{_CURRENT}" in log
    ), log
    assert (
        f"Fetching changed files: repos/ClickHouse/ClickHouse/compare/"
        f"{_BASE}...{_CURRENT}" in log
    ), log


def test_the_job_classifies_every_real_script_run(_script_runs, tmp_path):
    # Closes the loop: the tokens the script actually writes are the ones the
    # job's classifier accepts.
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    expected = {
        "no_cpp": _MARKER_NO_CPP,
        "no_data": _MARKER_NO_DATA,
        "empty": _MARKER_EMPTY,
        "report": _MARKER_REPORT,
        "gh_404": job.DiffOutcome.FAILED,
        "gh_404_files": job.DiffOutcome.FAILED,
    }
    for case, (rc, marker, report, log) in _script_runs.items():
        case_dir = tmp_path / case
        case_dir.mkdir()
        if marker:
            (case_dir / "diff_outcome.txt").write_text(marker, encoding="utf-8")
        outcome = job.classify_diff_outcome(
            script_ok=(rc == 0),
            marker=job.read_diff_outcome_marker(str(case_dir)),
            report_ready=report,
        )
        assert outcome == expected[case], (case, log)


@pytest.mark.parametrize(
    "case,want_outcome,want_diff_ran",
    [
        ("no_cpp", _MARKER_NO_CPP, False),
        ("no_data", _MARKER_NO_DATA, False),
        ("empty", _MARKER_EMPTY, False),
        ("report", _MARKER_REPORT, True),
        ("gh_404", "failed", False),
        ("gh_404_files", "failed", False),
    ],
)
def test_the_job_reports_what_the_real_script_did(
    case, want_outcome, want_diff_ran, _script_runs, tmp_path
):
    # End to end over the real files: the script writes a token, the job's own
    # call site reads it, and the outcome is what the reader is told.
    rc, marker, report, log = _script_runs[case]
    outcome, diff_ran = _wired_outcome(
        tmp_path, marker, report_ready=report, script_ok=(rc == 0)
    )
    assert (outcome, diff_ran) == (want_outcome, want_diff_ran), (case, log)


# ---------------------------------------------------------------------------
# The three dispatches the outcome selects.
#
# Everything above proves what the job computes. These prove what it then DOES:
# whether the coverage-report branch runs, whether print_uncovered_code.py is
# executed, and whether a coverage comment is written. A correct outcome can sit
# next to a guard that consumes it backwards, and only these can see that.
# ---------------------------------------------------------------------------


class _ResultFsStub(Result):
    """A Result whose from_fs does not need print_uncovered_code.py's output file."""

    @classmethod
    def from_fs(cls, name):
        return cls.create_from(name=name, status=cls.Status.OK, info="stub")


class _ShellSpy:
    """Records whether the job asked the shell to run the analysis."""

    def __init__(self):
        self.commands = []

    def run(self, command, *a, **k):
        self.commands.append(command)
        return 0


def _report_branch_taken(outcome: str) -> bool:
    ns = {"_diff_ran": outcome == _MARKER_REPORT, "_took_report_branch": False}
    exec(_report_guard_snippet(), ns)  # noqa: S102 - trusted first-party source
    return ns["_took_report_branch"]


def _analysis_dispatch(outcome: str, tmp_path, files: dict):
    """Run the real analysis dispatch. Returns (analysis_ran, print_res_status)."""
    for name, present in files.items():
        if present:
            (tmp_path / name).write_text("x", encoding="utf-8")
    _, inputs_ns = _run_snippet(
        _diff_inputs_snippet(),
        TEMP_DIR=str(tmp_path) + os.sep,
        _diff_outcome=outcome,
        Path=Path,
    )
    spy = _ShellSpy()
    _, ns = _run_snippet(
        _analysis_dispatch_snippet(),
        TEMP_DIR=str(tmp_path) + os.sep,
        _diff_outcome=outcome,
        _diff_inputs_exist=inputs_ns["_diff_inputs_exist"],
        _print_log=str(tmp_path / "print.log"),
        Result=_ResultFsStub,
        Shell=spy,
        Path=Path,
    )
    return bool(spy.commands), ns["print_res"].status


def _comment_made(outcome: str, tmp_path) -> bool:
    # TEMP_DIR must be overridden: _run_snippet seeds the namespace from the
    # job module, whose TEMP_DIR is the real ci/tmp, and the marker-write path
    # would otherwise create files there.
    _, ns = _run_snippet(
        _comment_dispatch_snippet(),
        _diff_ran=outcome == _MARKER_REPORT,
        _diff_outcome=outcome,
        _made_comment=False,
        TEMP_DIR=str(tmp_path),
        current_commit_sha="0123abcd" + "0" * 32,
    )
    return ns["_made_comment"]


# What the real script leaves on disk per outcome, measured by running it: the
# diff is fetched before every exit-0 path, and the coverage slice is written
# before the no-data and empty exits but after the 404 dies.
_FILES_PRESENT = {
    _MARKER_NO_CPP: {"changes.diff": True, "current.changed.info": False},
    _MARKER_NO_DATA: {"changes.diff": True, "current.changed.info": True},
    _MARKER_EMPTY: {"changes.diff": True, "current.changed.info": True},
    _MARKER_REPORT: {"changes.diff": True, "current.changed.info": True},
    "failed": {"changes.diff": True, "current.changed.info": False},
    "unknown": {"changes.diff": False, "current.changed.info": False},
}


def test_dispatch_snippets_are_the_real_production_blocks():
    # Without these the dispatch assertions below could go vacuous through an
    # extraction that slid onto one of the job's other identical guard lines.
    report = _report_guard_snippet()
    assert "_diff_ran" in report
    assert "_took_report_branch" in report
    # The four `if _diff_ran:` lines are textually identical, so the site is
    # identified by what it guards: only this one reads the coverage summaries.
    assert "get_lcov_summary(" in _report_guard_body()
    assert "base_llvm_coverage.info" in _report_guard_body()
    analysis = _analysis_dispatch_snippet()
    assert "if _diff_inputs_exist:" in analysis
    assert "print_uncovered_code.py" in analysis
    assert "Result.create_from" in analysis
    comment = _comment_dispatch_snippet()
    assert "_has_coverage_data = _diff_ran" in comment
    assert "coverage_comment_message" in comment
    assert "_made_comment" in comment
    assert "skipped_reason" in comment
    assert "coverage_marker_reason" in comment


@pytest.mark.parametrize(
    "outcome,want_report,want_analysis,want_comment",
    [
        # The only outcome with numbers: report, analysis and comment all happen.
        (_MARKER_REPORT, True, True, True),
        # This run's slice holds no records, so there is nothing to analyse.
        (_MARKER_EMPTY, False, False, False),
        # No slice of this run's own, so the analysis must not read a stale one.
        (_MARKER_NO_CPP, False, False, False),
        (_MARKER_NO_DATA, False, False, False),
        # Nothing is known about the changed files in either of these.
        ("failed", False, False, False),
        ("unknown", False, False, False),
    ],
)
def test_each_outcome_selects_the_dispatches_it_should(
    outcome, want_report, want_analysis, want_comment, tmp_path
):
    analysis_ran, _ = _analysis_dispatch(outcome, tmp_path, _FILES_PRESENT[outcome])
    assert (
        _report_branch_taken(outcome),
        analysis_ran,
        _comment_made(outcome, tmp_path),
    ) == (want_report, want_analysis, want_comment)


def test_a_reportless_outcome_never_enters_the_coverage_report_branch():
    # That branch calls get_lcov_summary on absent files and compresses a
    # directory that does not exist, so entering it is a crash, not a mislabel.
    for outcome in (_MARKER_NO_CPP, _MARKER_NO_DATA, _MARKER_EMPTY, "failed", "unknown"):
        assert _report_branch_taken(outcome) is False, outcome


def test_a_failed_script_reports_the_analysis_as_not_ok(tmp_path):
    # The dispatch must reach the branch that fails the sub-result, not merely
    # compute an outcome that would.
    analysis_ran, status = _analysis_dispatch(
        "failed", tmp_path, _FILES_PRESENT["failed"]
    )
    assert analysis_ran is False
    assert status != Result.Status.OK


def test_a_record_less_slice_is_reported_as_empty_not_as_nothing_coverable(tmp_path):
    # `current_coverage_empty` is reached only when the baseline slice does have
    # records, so the changed files are known to be coverable. Running the
    # analyser over the record-less current slice would report the opposite:
    # `0/0 (nothing coverable)` at OK, contradicting the outcome alongside it.
    analysis_ran, status = _analysis_dispatch(
        _MARKER_EMPTY, tmp_path, _FILES_PRESENT[_MARKER_EMPTY]
    )
    assert analysis_ran is False
    assert status == Result.Status.OK
    reasons_dir = tmp_path / "reasons"
    reasons_dir.mkdir()
    text = _reported_reasons(script_ok=True, marker=_MARKER_EMPTY, tmp_path=reasons_dir)
    assert "nothing coverable" not in text, text
    assert "empty" in text.lower(), text


# --- Producer gates and the stale-comment marker ------------------------------
#
# A shard whose run was incomplete must publish no profile, and a report-less
# aggregate outcome must leave a stale-numbers marker for the comment hook.

import json as _json

_NON_REPORT_OUTCOMES = (
    _MARKER_NO_CPP,
    _MARKER_NO_DATA,
    _MARKER_EMPTY,
    "failed",
    "unknown",
)


@pytest.mark.parametrize("outcome", _NON_REPORT_OUTCOMES)
def test_reportless_outcomes_write_the_stale_comment_marker(outcome, tmp_path):
    job = sys.modules["ci.jobs.llvm_coverage_job"]
    assert _comment_made(outcome, tmp_path) is False
    marker = tmp_path / "coverage_comment.json"
    assert marker.exists(), outcome
    d = _json.loads(marker.read_text(encoding="utf-8"))
    assert set(d) == {"skipped_reason", "commit_sha"}
    assert d["skipped_reason"] == job.coverage_marker_reason(outcome)
    # The hook prepends "No coverage measurement for commit <sha>: ", so the
    # reason must read as a lowercase sentence tail.
    assert d["skipped_reason"][:1].islower(), d["skipped_reason"]


def test_the_report_outcome_writes_no_stale_marker(tmp_path):
    assert _comment_made(_MARKER_REPORT, tmp_path) is True
    assert not (tmp_path / "coverage_comment.json").exists()


class _JobConfigInfoStub:
    def __init__(self, provides):
        self.job_config = {"provides": provides}


def test_it_merge_publishes_nothing_for_an_incomplete_run(monkeypatch, tmp_path):
    import ci.jobs.integration_test_job as it

    monkeypatch.chdir(tmp_path)
    (tmp_path / "x.profraw").write_bytes(b"x")
    (tmp_path / "cov_it.profdata").write_bytes(b"stale")
    monkeypatch.setattr(it, "Info", lambda: _JobConfigInfoStub(["cov_it"]))

    assert it.merge_profraw_files("llvm-profdata", run_complete=False) is None
    # The stale target is removed (the ./*.profdata artifact glob would publish
    # it), while the .profraw inputs stay on disk for inspection.
    assert not (tmp_path / "cov_it.profdata").exists()
    assert (tmp_path / "x.profraw").exists()


def _producer_gate_snippet(path: str, anchor: str) -> str:
    lines = open(path, encoding="utf-8").read().splitlines(True)
    start = next(i for i, l in enumerate(lines) if anchor in l)
    end = next(
        i for i in range(start + 1, len(lines)) if "profraw_files = []" in lines[i]
    )
    return textwrap.dedent("".join(lines[start : end + 1]))


class _ErrorableResultStub:
    def __init__(self, error: bool):
        self._error = error

    def is_error(self):
        return self._error


def _run_producer_gate(snippet: str, **ns):
    printed = []
    ns["print"] = lambda *a, **k: printed.append(" ".join(str(x) for x in a))
    exec(snippet, ns)  # noqa: S102 - trusted first-party source
    return "\n".join(printed), ns


def test_ft_gate_suppresses_the_profile_on_incomplete_runs():
    snippet = _producer_gate_snippet(
        "ci/jobs/functional_tests.py",
        "if test_result is None or test_result.is_error():",
    )
    assert "publishing no profile" in snippet

    text, ns = _run_producer_gate(
        snippet, test_result=None, profraw_files=["a.profraw"]
    )
    assert "the test stage did not run" in text
    assert ns["profraw_files"] == []

    text, ns = _run_producer_gate(
        snippet, test_result=_ErrorableResultStub(True), profraw_files=["a.profraw"]
    )
    assert "terminated unexpectedly" in text
    assert ns["profraw_files"] == []

    text, ns = _run_producer_gate(
        snippet, test_result=_ErrorableResultStub(False), profraw_files=["a.profraw"]
    )
    assert ns["profraw_files"] == ["a.profraw"]


def test_ut_gate_suppresses_the_profile_when_the_binary_died():
    snippet = _producer_gate_snippet(
        "ci/jobs/unit_tests_job.py", "if R.is_error():"
    )
    assert "publishing no profile" in snippet

    text, ns = _run_producer_gate(
        snippet, R=_ErrorableResultStub(True), profraw_files=["a.profraw"]
    )
    assert "did not run to completion" in text
    assert ns["profraw_files"] == []

    text, ns = _run_producer_gate(
        snippet, R=_ErrorableResultStub(False), profraw_files=["a.profraw"]
    )
    assert ns["profraw_files"] == ["a.profraw"]


def test_ut_coverage_job_uploads_its_profile_despite_failing_tests():
    # complete_job exits 1 on FAIL; the runner uploads `provides` artifacts on a
    # failed job only when do_not_block_pipeline_on_failure is set, so without it
    # a failing-but-complete unit run would strand its merged profile locally.
    src = open("ci/jobs/unit_tests_job.py", encoding="utf-8").read()
    assert (
        'R.complete_job(do_not_block_pipeline_on_failure="llvm_coverage" in job_name)'
        in src
    )


def test_paginated_gh_output_is_flattened_across_pages():
    from ci.jobs.scripts.job_hooks.llvm_coverage_hook import parse_paginated_arrays

    # gh api --paginate emits one JSON array per page, concatenated.
    assert parse_paginated_arrays('["a", "b"]') == ["a", "b"]
    assert parse_paginated_arrays('["a"]\n["b", "c"]') == ["a", "b", "c"]
    assert parse_paginated_arrays('["a"]["b"]') == ["a", "b"]
    assert parse_paginated_arrays("") == []
    assert parse_paginated_arrays(None) == []
    with pytest.raises(ValueError):
        parse_paginated_arrays("not json")
