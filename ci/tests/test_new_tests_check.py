"""
Tests for the `new_tests_check.py` workflow post-hook (the merge-blocking
aggregate for Bugfix Validation, see PR #103541).

The hook gates Bug-Fix PRs on whether at least one per-arch Bugfix Validation
job (`OK`/`XFAIL`) confirmed the bug reproduces on master HEAD and is fixed
on the PR. Earlier iterations of this branch had two bypasses in `check`
(`has_unit` shortcut, CI-report-link fallback) that let an unvalidated FT/IT
regression test through - both were caught only by review. These tests pin
the truth table so future changes cannot reintroduce a bypass.

Invariant under test: when a Bug-Fix PR adds new functional or integration
regression tests (`has_ft or has_it`), `check()` returns True iff at least
one per-arch Bugfix Validation job is strict-success (`OK` or `XFAIL`).
Neither the unit-test presence shortcut nor the CI-report-link fallback may
override this; only the per-arch jobs decide.

See ClickHouse/ClickHouse#103541 (bot review, 2026-06-06).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on
# the path for `import praktika` to resolve to `ci/praktika`. CI runs configure
# this via the praktika runner; we replicate it here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.defs import JobNames
from ci.jobs.scripts.workflow_hooks import new_tests_check
from ci.praktika.result import Result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _per_arch_job(name, status):
    """A workflow sub-result for a single per-arch Bugfix Validation job."""
    return Result(name=name, status=status)


def _workflow_result(per_arch_jobs):
    """A fake workflow-level `Result` with the supplied per-arch jobs."""
    return Result(
        name="PR",
        status=Result.Status.OK,
        results=list(per_arch_jobs),
    )


class _StubInfo:
    """Replace `ci.praktika.info.Info` so `check()` does not need the praktika
    runtime to be initialized. Only the surface that `check` and
    `any_bugfix_validation_passed` touch is implemented.
    """

    def __init__(self, *, workflow_name="PR", pr_body="", changed_files=None, labels=None):
        self.workflow_name = workflow_name
        self.pr_body = pr_body
        self._changed_files = list(changed_files or [])
        self.pr_labels = list(labels or [])

    def get_changed_files(self):
        return list(self._changed_files)


def _install_stubs(
    monkeypatch,
    *,
    pr_body,
    changed_files,
    workflow_subresults,
    title="A bug fix",
    labels=None,
):
    """Patch `Info`, `GH.get_pr_title_body_labels`, and `Result.from_fs` so
    `check()` and `any_bugfix_validation_passed()` run without external state.

    `changed_files` is a list of paths relative to the repo root. They are
    not required to exist on disk: `has_new_*_tests` use `Path.is_file`, so
    the helpers in `new_tests_check` are also stubbed to return whether any
    matching path was provided.

    `labels` drives the bug-fix gate, which `check()` keys on the
    `pr-bugfix` / `pr-critical-bugfix` labels (set by the
    `pr_labels_and_category.py` pre-hook), NOT the PR body text. When not
    given explicitly, the bug-fix label is inferred from the legacy
    "Bug Fix" changelog marker in `pr_body` so the truth-table cases below
    stay terse; pass `labels=[...]` to exercise the label gate directly.
    """
    if labels is None:
        labels = ["pr-bugfix"] if " Bug Fix" in pr_body else []
    stub_info = _StubInfo(
        workflow_name="PR",
        pr_body=pr_body,
        changed_files=list(changed_files),
        labels=labels,
    )
    monkeypatch.setattr(new_tests_check, "Info", lambda: stub_info)

    monkeypatch.setattr(
        new_tests_check.GH,
        "get_pr_title_body_labels",
        staticmethod(lambda: (title, pr_body, list(labels or []))),
    )

    workflow_result = _workflow_result(workflow_subresults)
    monkeypatch.setattr(
        new_tests_check.Result,
        "from_fs",
        staticmethod(lambda name: workflow_result),
    )

    # `has_new_functional_tests` / `has_new_integration_tests` /
    # `has_new_unit_tests` use `Path.is_file()` to ignore deleted files. Our
    # synthetic paths are not on disk; stub the file-presence checks so the
    # tests are about path classification only.
    monkeypatch.setattr(
        new_tests_check, "has_new_functional_tests",
        lambda files: any(
            f.lstrip("./").startswith("tests/queries/0_stateless") for f in files
        ),
    )
    monkeypatch.setattr(
        new_tests_check, "has_new_integration_tests",
        lambda files: any(
            f.lstrip("./").startswith("tests/integration/test_")
            and not f.lstrip("./").startswith("tests/integration/test_e2e_")
            and f.endswith(".py")
            for f in files
        ),
    )
    monkeypatch.setattr(
        new_tests_check, "has_new_unit_tests",
        lambda files: any(
            f.lstrip("./").startswith("src") and "/tests/" in f for f in files
        ),
    )


# ---------------------------------------------------------------------------
# Sanity: `any_bugfix_validation_passed` itself
# ---------------------------------------------------------------------------


def test_any_bugfix_validation_passed_only_strict_success_counts(monkeypatch):
    """`SKIPPED` per-arch jobs must NOT count as validation. Only `OK` or
    `XFAIL` reflect "the bug reproduced on master HEAD and is fixed on PR".
    """
    _install_stubs(
        monkeypatch,
        pr_body="",
        changed_files=[],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.any_bugfix_validation_passed() is False


def test_any_bugfix_validation_passed_one_ok_is_enough(monkeypatch):
    _install_stubs(
        monkeypatch,
        pr_body="",
        changed_files=[],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.OK),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.any_bugfix_validation_passed() is True


def test_any_bugfix_validation_passed_xfail_counts(monkeypatch):
    """`XFAIL` is treated as success by `Result.is_success`."""
    _install_stubs(
        monkeypatch,
        pr_body="",
        changed_files=[],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.XFAIL),
        ],
    )
    assert new_tests_check.any_bugfix_validation_passed() is True


def test_any_bugfix_validation_passed_error_does_not_count(monkeypatch):
    """An infrastructure `ERROR` per-arch result must not be counted as a
    validation: we have no reliable signal about whether the bug reproduces.
    """
    _install_stubs(
        monkeypatch,
        pr_body="",
        changed_files=[],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.ERROR),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.any_bugfix_validation_passed() is False


def test_any_bugfix_validation_passed_fail_does_not_count(monkeypatch):
    """A `FAIL` per-arch result must not count as validated.

    Per-arch runners propagate `SKIPPED` for the no-repro case rather than
    `FAIL`, so a `FAIL` here would be a runner regression - the post-hook
    must not silently accept it.
    """
    _install_stubs(
        monkeypatch,
        pr_body="",
        changed_files=[],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.FAIL),
        ],
    )
    assert new_tests_check.any_bugfix_validation_passed() is False


# ---------------------------------------------------------------------------
# `check()` truth table: FT/IT present
# ---------------------------------------------------------------------------


def test_ft_present_all_per_arch_skipped_fails_check(monkeypatch):
    """All per-arch jobs `SKIPPED` (bug reproduces on no arch) -> reject."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(n, Result.Status.SKIPPED)
            for n in (
                JobNames.BUGFIX_VALIDATE_FT_AMD,
                JobNames.BUGFIX_VALIDATE_FT_ARM,
                JobNames.BUGFIX_VALIDATE_IT_AMD,
                JobNames.BUGFIX_VALIDATE_IT_ARM,
            )
        ],
    )
    assert new_tests_check.check() is False


def test_ft_present_all_per_arch_fail_fails_check(monkeypatch):
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(n, Result.Status.FAIL)
            for n in (
                JobNames.BUGFIX_VALIDATE_FT_AMD,
                JobNames.BUGFIX_VALIDATE_FT_ARM,
            )
        ],
    )
    assert new_tests_check.check() is False


def test_ft_present_one_per_arch_ok_passes_check(monkeypatch):
    """At least one per-arch job validated the bug -> pass."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.OK),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.check() is True


def test_it_present_one_per_arch_ok_passes_check(monkeypatch):
    """Same contract for integration tests (independent of FT)."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["tests/integration/test_my_thing/test.py"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_AMD, Result.Status.OK),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_IT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.check() is True


# ---------------------------------------------------------------------------
# `check()` truth table: bypass concerns from the bot reviews of PR #103541
# ---------------------------------------------------------------------------


def test_ft_plus_unit_does_not_let_unit_bypass_per_arch(monkeypatch):
    """Concern #5 - unit-only shortcut cannot bypass FT/IT validation.

    A PR that adds BOTH a unit test AND a functional regression test must
    still validate via per-arch Bugfix Validation. The unit-test presence
    shortcut applies ONLY when no FT/IT regression test was added.
    """
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=[
            "tests/queries/0_stateless/04500_my_regression.sql",
            "src/Functions/tests/gtest_my_unit.cpp",
        ],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.check() is False


def test_ft_plus_ci_report_link_does_not_bypass_per_arch(monkeypatch):
    """Concern #6 - the CI-report-link fallback cannot bypass FT validation.

    A Bug-Fix PR that adds an FT regression test AND links a CI report in
    the PR body must still validate via per-arch Bugfix Validation. The
    CI-report-link fallback applies ONLY to PRs that add NO new tests at
    all (the operator-supplied-evidence path).
    """
    pr_body = (
        "Fix something.\n\n"
        "- [x]  Bug Fix\n\n"
        "See https://s3.amazonaws.com/clickhouse-test-reports/PRs/12345/ for"
        " evidence.\n"
    )
    _install_stubs(
        monkeypatch,
        pr_body=pr_body,
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.check() is False


def test_unit_only_passes_without_per_arch(monkeypatch):
    """Unit-only PR: no FT/IT machinery, the unit-test presence is enough."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["src/Functions/tests/gtest_my_unit.cpp"],
        workflow_subresults=[],
    )
    assert new_tests_check.check() is True


def test_no_test_with_ci_report_link_passes(monkeypatch):
    """No new tests + CI-report-link in PR body -> operator-supplied
    evidence path passes.
    """
    pr_body = (
        "Fix something.\n\n"
        "- [x]  Bug Fix\n\n"
        "See https://s3.amazonaws.com/clickhouse-test-reports/PRs/12345/ for"
        " evidence.\n"
    )
    _install_stubs(
        monkeypatch,
        pr_body=pr_body,
        changed_files=["src/Functions/UnrelatedSourceFile.cpp"],
        workflow_subresults=[],
    )
    assert new_tests_check.check() is True


def test_no_test_without_ci_report_link_fails(monkeypatch):
    """No new tests and no CI-report-link -> reject."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["src/Functions/UnrelatedSourceFile.cpp"],
        workflow_subresults=[],
    )
    assert new_tests_check.check() is False


def test_not_a_bug_fix_pr_skips(monkeypatch):
    """Non-bug-fix PRs are not gated by this hook at all."""
    _install_stubs(
        monkeypatch,
        pr_body="An improvement.\n\n- [x]  Improvement\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.SKIPPED),
        ],
    )
    assert new_tests_check.check() is True


def test_ft_present_with_xfail_per_arch_passes(monkeypatch):
    """`XFAIL` is treated as success by `is_success`."""
    _install_stubs(
        monkeypatch,
        pr_body="A change.\n\n- [x]  Bug Fix\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.XFAIL),
        ],
    )
    assert new_tests_check.check() is True


# ---------------------------------------------------------------------------
# Bug-fix gate keys on labels, not PR body text
# ---------------------------------------------------------------------------


def test_ci_only_pr_body_mentioning_bug_fix_is_not_gated(monkeypatch):
    """A CI-only PR (labeled `pr-ci`, no `pr-bugfix`) whose body merely
    mentions "Bug Fix" in prose must NOT be treated as a Bug-Fix PR. Gating
    on the body substring used to self-trigger this hook on exactly such PRs
    (PR #103541 bot review, 2026-06-13).
    """
    _install_stubs(
        monkeypatch,
        pr_body="CI change.\n\nThis hook should skip unless Bug Fix PR.\n",
        changed_files=["src/Functions/UnrelatedSourceFile.cpp"],
        workflow_subresults=[],
        labels=["pr-ci"],
    )
    assert new_tests_check.check() is True


def test_bugfix_label_gates_even_without_body_marker(monkeypatch):
    """The `pr-bugfix` label alone enables the gate, regardless of body text:
    a labeled Bug-Fix PR with an FT regression test and no per-arch validation
    is rejected.
    """
    _install_stubs(
        monkeypatch,
        pr_body="A fix with no changelog marker in the body.\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.SKIPPED),
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_ARM, Result.Status.SKIPPED),
        ],
        labels=["pr-bugfix"],
    )
    assert new_tests_check.check() is False


def test_critical_bugfix_label_also_gates(monkeypatch):
    """`pr-critical-bugfix` enables the gate the same way as `pr-bugfix`."""
    _install_stubs(
        monkeypatch,
        pr_body="A critical fix.\n",
        changed_files=["tests/queries/0_stateless/04500_my_regression.sql"],
        workflow_subresults=[
            _per_arch_job(JobNames.BUGFIX_VALIDATE_FT_AMD, Result.Status.OK),
        ],
        labels=["pr-critical-bugfix"],
    )
    assert new_tests_check.check() is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
