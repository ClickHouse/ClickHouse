import sys
from pathlib import Path

from ci.praktika.gh import GH
from ci.praktika.info import Info


def has_new_functional_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        # Deleted files are still listed in changed_files; skip them so a PR that
        # only removes tests does not trigger flaky/bugfix checks that would then
        # find nothing to run.
        if file.startswith("tests/queries/0_stateless") and Path(file).is_file():
            return True
    return False


def has_new_integration_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if (
            file.startswith("tests/integration/test_")
            # e2e tests (`tests/integration/test_e2e_*`) require external
            # credentials/backends and are excluded from the default pytest run
            # via the `e2e` marker, so the flaky/bugfix checks cannot execute
            # them. Skip them here so a PR that only touches e2e tests does not
            # trigger a check that would have nothing to run.
            and not file.startswith("tests/integration/test_e2e_")
            and Path(file).name.startswith("test")
            and file.endswith(".py")
            and Path(file).is_file()
        ):
            return True
    return False


def has_new_unit_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if file.startswith("src") and "/tests/" in file and Path(file).is_file():
            return True
    return False


def has_ci_report_link(pr_body):
    return "s3.amazonaws.com/clickhouse-test-reports" in pr_body


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
    if (
        not has_new_unit_tests(changed_files)
        and not has_new_functional_tests(changed_files)
        and not has_new_integration_tests(changed_files)
    ):
        if has_ci_report_link(pr_body):
            print(
                "No new tests have been added, but the PR description has a link to a CI report - pass"
            )
            return True
        print(f"No new tests have been added")
        return False
    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
