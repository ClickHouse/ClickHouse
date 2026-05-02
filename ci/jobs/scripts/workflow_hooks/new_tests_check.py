import sys
from pathlib import Path

from ci.defs.defs import JobNames
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result


def has_new_functional_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if file.startswith("tests/queries/0_stateless"):
            return True
    return False


def has_new_integration_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if (
            file.startswith("tests/integration/test_")
            and Path(file).name.startswith("test")
            and file.endswith(".py")
        ):
            return True
    return False


def has_new_unit_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if file.startswith("src") and "/tests/" in file:
            return True
    return False


def has_ci_report_link(pr_body):
    return "s3.amazonaws.com/clickhouse-test-reports" in pr_body


# Per-arch Bugfix Validation job names that this post-hook OR's together.
# Each per-arch job runs the new/modified test on master HEAD and on the PR;
# the per-arch runner inverts the test status (XFAIL): the job reports OK
# iff the bug reproduced on master HEAD AND was fixed on the PR for that
# arch. Per-arch jobs are configured with `allow_failure=True` so an
# individual FAIL doesn't block PR merge — instead, this post-hook is
# responsible for the merge-blocking decision: validation passes iff at
# least ONE per-arch job is OK (or new unit tests were added; unit tests
# can't be auto-validated by re-running master HEAD).
_BUGFIX_VALIDATE_PER_ARCH_JOB_NAMES = (
    JobNames.BUGFIX_VALIDATE_FT_AMD,
    JobNames.BUGFIX_VALIDATE_FT_ARM,
    JobNames.BUGFIX_VALIDATE_IT_AMD,
    JobNames.BUGFIX_VALIDATE_IT_ARM,
    # Legacy monolithic names — kept here so cached/historical workflows
    # (which may still emit these names) still validate correctly.
    JobNames.BUGFIX_VALIDATE_FT,
    JobNames.BUGFIX_VALIDATE_IT,
)


def any_bugfix_validation_passed():
    """Return True iff at least one per-arch Bugfix Validation job in the
    current workflow result reported a success status (OK / XFAIL).

    SKIPPED jobs do NOT count as a pass: a job that filter_job skipped
    because the corresponding test type wasn't changed has no opinion on
    whether the bug was validated. We use `is_success()` (strict — OK or
    XFAIL only) rather than `is_ok()` (which treats SKIPPED as OK).
    """
    info = Info()
    try:
        workflow_result = Result.from_fs(info.workflow_name)
    except Exception as e:
        # Defensive: if the workflow result file isn't on disk yet (very
        # early in a workflow), don't crash — the post-hook will be retried.
        print(
            f"WARNING: failed to read workflow result for "
            f"[{info.workflow_name}]: {e}"
        )
        return False
    matched = []
    passed = []
    for sub in workflow_result.results:
        if sub.name in _BUGFIX_VALIDATE_PER_ARCH_JOB_NAMES:
            matched.append((sub.name, sub.status))
            if sub.is_success():
                passed.append(sub.name)
    if matched:
        print(
            f"Bugfix validation per-arch jobs: {matched} — "
            f"passed: {passed if passed else 'none'}"
        )
    else:
        print(
            "WARNING: no Bugfix Validation per-arch jobs found in workflow "
            "result — were they all filtered/skipped?"
        )
    return bool(passed)


def check():
    # read actual PR body from GH API - fallback to workflow context if failed
    title, body, labels = GH.get_pr_title_body_labels()
    if body:
        pr_body = body
    else:
        print("WARNING: Failed to get PR body from GH API - using workflow context")
        pr_body = Info().pr_body

    if not " Bug Fix" in pr_body:
        print("Not a bug fix PR - skip")
        return True

    changed_files = Info().get_changed_files()
    has_unit = has_new_unit_tests(changed_files)
    has_ft = has_new_functional_tests(changed_files)
    has_it = has_new_integration_tests(changed_files)

    if not has_unit and not has_ft and not has_it:
        if has_ci_report_link(pr_body):
            print(
                "No new tests have been added, but the PR description has a link to a CI report - pass"
            )
            return True
        print(f"No new tests have been added")
        return False

    # Unit tests can't be auto-validated by re-running master HEAD (they
    # live in compiled C++ and the validation framework can't selectively
    # run them against the master binary). Adding new unit tests is
    # accepted as proof on its own — same as before.
    if has_unit:
        print("New unit tests added - pass")
        return True

    # New functional or integration tests exist. The per-arch Bugfix
    # Validation jobs ran them against master HEAD and the PR. We pass iff
    # at least one per-arch job reported the bug as validated.
    if any_bugfix_validation_passed():
        print(
            "At least one per-arch Bugfix Validation job validated the bug - pass"
        )
        return True

    if has_ci_report_link(pr_body):
        print(
            "No per-arch Bugfix Validation job validated the bug, but the "
            "PR description has a link to a CI report - pass"
        )
        return True

    print(
        "No per-arch Bugfix Validation job validated the bug — the test "
        "either passes on master HEAD on every arch (so it's not actually "
        "a regression test for the fix) or every arch errored out. See "
        "the per-arch Bugfix validation jobs in the report."
    )
    return False


if __name__ == "__main__":
    if not check():
        sys.exit(1)
