"""
Tests for GH.convert_to_gh_status mapping.

Adding a new Result.Status value requires updating GH._STATUS_TO_GH.
These tests verify that every status is mapped and the mapping is correct.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.gh import GH
from ci.tests.test_result import ALL_STATUSES, _get_class_constants


def test_all_statuses_mapped():
    """Every Result.Status value must have a GH mapping."""
    for status in ALL_STATUSES:
        gh = GH.convert_to_gh_status(status)
        assert gh, f"No GH mapping for {status}"


def test_mapping_values_are_valid_gh_statuses():
    """GH API only accepts these four strings."""
    valid = _get_class_constants(Result.GHStatus)
    for status in ALL_STATUSES:
        gh = GH.convert_to_gh_status(status)
        assert gh in valid, f"GH mapping for {status} is {gh!r}, not a valid GH status"


def test_expected_mappings():
    assert GH.convert_to_gh_status(Result.Status.OK) == "success"
    assert GH.convert_to_gh_status(Result.Status.FAIL) == "failure"
    assert GH.convert_to_gh_status(Result.Status.ERROR) == "error"
    assert GH.convert_to_gh_status(Result.Status.SKIPPED) == "success"
    assert GH.convert_to_gh_status(Result.Status.PENDING) == "pending"
    assert GH.convert_to_gh_status(Result.Status.RUNNING) == "pending"
    assert GH.convert_to_gh_status(Result.Status.DROPPED) == "error"
    assert GH.convert_to_gh_status(Result.Status.UNKNOWN) == "failure"
    assert GH.convert_to_gh_status(Result.Status.XFAIL) == "success"
    assert GH.convert_to_gh_status(Result.Status.XPASS) == "failure"


def test_idempotent():
    """Passing an already-converted GH string should return it unchanged."""
    for gh_val in _get_class_constants(Result.GHStatus):
        assert GH.convert_to_gh_status(gh_val) == gh_val


def test_invalid_status_asserts():
    """Unknown strings must raise."""
    try:
        GH.convert_to_gh_status("bogus")
        assert False, "Should have raised"
    except AssertionError:
        pass


def test_repo_name_from_git_remote_url():
    cases = {
        "https://github.com/ClickHouse/ClickHouse.git": "ClickHouse/ClickHouse",
        "https://github.com/ClickHouse/ClickHouse": "ClickHouse/ClickHouse",
        "git@github.com:ClickHouse/ClickHouse.git": "ClickHouse/ClickHouse",
        "ssh://git@github.com/ClickHouse/ClickHouse.git": "ClickHouse/ClickHouse",
        "https://github.com/ClickHouse/ClickHouse.git/": "ClickHouse/ClickHouse",
    }
    for repo_url, repo_name in cases.items():
        assert GH._repo_name_from_git_remote_url(repo_url) == repo_name


def test_repo_name_from_git_remote_url_rejects_unexpected_format():
    assert GH._repo_name_from_git_remote_url("/home/user/ClickHouse") == ""


def _stub_report_url(monkeypatch):
    """Replace `Info.get_specific_report_url_static` so `to_markdown` can run
    without the praktika Settings/env being configured for these unit tests."""
    from ci.praktika.info import Info

    monkeypatch.setattr(
        Info,
        "get_specific_report_url_static",
        staticmethod(
            lambda pr_number, branch, sha, job_name, workflow_name:
                f"https://x/json.html?PR={pr_number}&job={job_name}"
        ),
    )


def _make_summary(extras, n_fails):
    """Build a `ResultSummaryForGH` with `n_fails` synthetic failed jobs."""
    S = GH.ResultSummaryForGH
    fails = [S(name=f"Job {i}", status=Result.Status.FAIL) for i in range(n_fails)]
    return S(
        name="PR",
        status=Result.Status.FAIL if n_fails else Result.Status.OK,
        extra_links=list(extras),
        failed_results=fails,
    )


def _assert_table_renders_at_top_level(md):
    """Verify the failure table is recognisable as a top-level GFM table.

    Markdown renderers require the table header to be immediately preceded
    by a blank line or the document start — otherwise the header line gets
    merged into the previous paragraph or list item and the delimiter row
    is rendered as plain text.
    """
    lines = md.split("\n")
    header_idx = next(
        (i for i, ln in enumerate(lines) if ln.startswith("|job_name|")),
        None,
    )
    assert header_idx is not None, f"failure-table header missing in:\n{md}"
    prev = lines[header_idx - 1] if header_idx > 0 else ""
    assert prev == "", (
        f"failure-table header must be preceded by a blank line so it is "
        f"parsed as a top-level table; previous line was: {prev!r}\n\n{md}"
    )
    delim = lines[header_idx + 1]
    assert delim.startswith("|:--"), (
        f"line after header must be the delimiter row; got: {delim!r}"
    )


def test_to_markdown_extras_plus_truncated_failures_render_correctly(monkeypatch):
    """Regression: when both `extra_links` bullets and more than 15 failed
    jobs are present, the truncation note must not be indented (otherwise
    it becomes an indented code block) and the table must stay outside the
    bullet (otherwise it renders as plain text inside the last bullet).

    This is the bug from `https://github.com/ClickHouse/ClickHouse/pull/106008`.
    """
    _stub_report_url(monkeypatch)
    extras = [("Performance Comparison", "[Performance dashboard](https://x/runs?q=1)")]
    md = _make_summary(extras, n_fails=16).to_markdown(
        pr_number=1, sha="deadbeef", workflow_name="PR", branch="b"
    )

    lines = md.split("\n")

    bullet_idx = next(i for i, ln in enumerate(lines) if ln.startswith("- "))
    assert lines[bullet_idx + 1] == "", (
        f"bullet list must be terminated with a blank line; got: {lines[bullet_idx + 1]!r}"
    )

    trunc_idx = next(
        i for i, ln in enumerate(lines) if "failures out of" in ln
    )
    assert lines[trunc_idx] == "*15 failures out of 16 shown*:", (
        f"truncation note must be unindented; got: {lines[trunc_idx]!r}"
    )
    assert lines[trunc_idx + 1] == "", (
        f"truncation note must be followed by a blank line; got: {lines[trunc_idx + 1]!r}"
    )

    _assert_table_renders_at_top_level(md)
    assert "|[Job 0](" in md
    assert "|[Job 14](" in md
    assert "|[Job 15](" not in md, "rows must be truncated to first 15"


def test_to_markdown_extras_plus_small_failure_table(monkeypatch):
    """With extras and ≤15 failures the table follows the bullet with one
    blank line separator — no truncation note."""
    _stub_report_url(monkeypatch)
    extras = [("Performance Comparison", "[Performance dashboard](https://x/runs?q=1)")]
    md = _make_summary(extras, n_fails=3).to_markdown(
        pr_number=1, sha="deadbeef", workflow_name="PR", branch="b"
    )
    assert "failures out of" not in md
    _assert_table_renders_at_top_level(md)


def test_to_markdown_no_extras_truncated(monkeypatch):
    """With no extras but >15 failures the truncation note still stands as
    its own paragraph, with blank lines around it."""
    _stub_report_url(monkeypatch)
    md = _make_summary(extras=[], n_fails=20).to_markdown(
        pr_number=1, sha="deadbeef", workflow_name="PR", branch="b"
    )
    lines = md.split("\n")
    trunc_idx = next(i for i, ln in enumerate(lines) if "failures out of" in ln)
    assert lines[trunc_idx] == "*15 failures out of 20 shown*:"
    assert lines[trunc_idx - 1] == "", (
        f"truncation paragraph must be preceded by a blank line; "
        f"got: {lines[trunc_idx - 1]!r}"
    )
    assert lines[trunc_idx + 1] == ""
    _assert_table_renders_at_top_level(md)


def test_to_markdown_no_failures(monkeypatch):
    """With no failures `to_markdown` must not emit a table at all."""
    _stub_report_url(monkeypatch)
    extras = [("Performance Comparison", "[Performance dashboard](https://x/runs?q=1)")]
    md = _make_summary(extras, n_fails=0).to_markdown(
        pr_number=1, sha="deadbeef", workflow_name="PR", branch="b"
    )
    assert "|job_name|" not in md
    assert "failures out of" not in md
    # The bullet is still present.
    assert "- Performance Comparison:" in md


def test_post_commit_status_converts_transparently(monkeypatch):
    """post_commit_status must accept Result.Status values and convert them."""
    captured = {}

    def fake_do_command(cmd):
        captured["cmd"] = cmd
        return True

    monkeypatch.setattr(GH, "do_command_with_retries", staticmethod(fake_do_command))
    monkeypatch.setenv("GITHUB_REPOSITORY", "test/repo")
    monkeypatch.setenv("SHA", "abc123")

    GH.post_commit_status(
        name="test", status=Result.Status.OK, description="ok", url="http://x",
        sha="abc123", repo="test/repo",
    )
    assert "state=success" in captured["cmd"], f"Expected state=success in command, got: {captured['cmd']}"
