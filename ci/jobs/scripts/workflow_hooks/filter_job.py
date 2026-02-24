import re

from ci.defs.defs import JobNames
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.new_tests_check import (
    has_new_functional_tests,
    has_new_integration_tests,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info


def only_docs(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if (
            file.startswith("docs/")
            or file.startswith("docker/docs")
            or file.endswith(".md")
            or "aspell-dict.txt" in file
        ):
            continue
        else:
            return False
    return True


DO_NOT_TEST_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.DOCKER_BUILDS_ARM,
    JobNames.DOCKER_BUILDS_AMD,
]

PRELIMINARY_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    "Build (amd_tidy)",
    "Build (arm_tidy)",
]

INTEGRATION_TEST_FLAKY_CHECK_JOBS = [
    "Build (amd_asan)",
    "Integration tests (amd_asan, flaky)",
]

FUNCTIONAL_TEST_FLAKY_CHECK_JOBS = [
    "Build (amd_asan)",
    "Build (amd_tsan)",
    "Build (amd_msan)",
    "Build (amd_ubsan)",
    "Build (amd_debug)",
    "Build (amd_binary)",
    "Stateless tests (amd_asan, flaky check)",
    "Stateless tests (amd_tsan, flaky check)",
    "Stateless tests (amd_msan, flaky check)",
    "Stateless tests (amd_ubsan, flaky check)",
    "Stateless tests (amd_debug, flaky check)",
    "Stateless tests (amd_binary, flaky check)",
]


_info_cache = None


def should_skip_job(job_name):
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()
        print(f"INFO: PR labels: {_info_cache.pr_labels}")

    changed_files = _info_cache.get_kv_data("changed_files")
    if not changed_files:
        print("WARNING: no changed files found for PR - do not filter jobs")
        return False, ""

    if job_name == JobNames.PR_BODY:
        # Run the job if AI assistant is explicitly enabled in the PR body
        if "disable ai pr formatting assistant: true" in _info_cache.pr_body.lower():
            return True, "AI PR assistant is explicitly disabled in the PR body"
        if "Reverts ClickHouse/" in _info_cache.pr_body:
            return True, "Skipped for revert PRs"
        return False, ""

    if (
        Labels.CI_BUILD in _info_cache.pr_labels
        and "build" not in job_name.lower()
        and job_name not in PRELIMINARY_JOBS
    ):
        return True, f"Skipped, labeled with '{Labels.CI_BUILD}'"

    if Labels.DO_NOT_TEST in _info_cache.pr_labels and job_name not in DO_NOT_TEST_JOBS:
        return True, f"Skipped, labeled with '{Labels.DO_NOT_TEST}'"

    if Labels.NO_FAST_TESTS in _info_cache.pr_labels and job_name in PRELIMINARY_JOBS:
        return True, f"Skipped, labeled with '{Labels.NO_FAST_TESTS}'"

    if (
        job_name == JobNames.SMOKE_TEST_MACOS
        and _info_cache.pr_number
        and Labels.CI_MACOS not in _info_cache.pr_labels
    ):
        return True, f"Skipped, not labeled with '{Labels.CI_MACOS}'"

    if (
        JobNames.BUILD_TOOLCHAIN in job_name
        and _info_cache.pr_number
        and Labels.CI_TOOLCHAIN not in _info_cache.pr_labels
    ):
        return True, f"Skipped, not labeled with '{Labels.CI_TOOLCHAIN}'"

    if (
        Labels.CI_INTEGRATION_FLAKY in _info_cache.pr_labels
        and job_name not in INTEGRATION_TEST_FLAKY_CHECK_JOBS
    ):
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_INTEGRATION_FLAKY}' - run integration test flaky check job only",
        )

    if (
        Labels.CI_FUNCTIONAL_FLAKY in _info_cache.pr_labels
        and job_name not in FUNCTIONAL_TEST_FLAKY_CHECK_JOBS
    ):
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_FUNCTIONAL_FLAKY}' - run stateless test jobs only",
        )

    if Labels.CI_INTEGRATION in _info_cache.pr_labels and not (
        job_name.startswith(JobNames.INTEGRATION)
        or job_name in JobConfigs.builds_for_tests
    ):
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_INTEGRATION}' - run integration test jobs only",
        )

    if Labels.CI_FUNCTIONAL in _info_cache.pr_labels and not (
        job_name.startswith(JobNames.STATELESS)
        or job_name.startswith(JobNames.STATEFUL)
        or job_name in JobConfigs.builds_for_tests
        or "functional" in job_name.lower()  # Bugfix validation (functional tests)
    ):
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_FUNCTIONAL}' - run stateless test jobs only",
        )

    if Labels.CI_PERFORMANCE in _info_cache.pr_labels and (
        "performance" not in job_name.lower()
        and job_name
        not in (
            "Build (amd_release)",
            "Build (arm_release)",
            JobNames.DOCKER_BUILDS_ARM,
            JobNames.DOCKER_BUILDS_AMD,
        )
    ):
        return (
            True,
            "Skipped, labeled with 'ci-performance' - run performance jobs only",
        )

    if " Bug Fix" not in _info_cache.pr_body and "Bugfix" in job_name:
        # Don't skip if the corresponding test job file was changed
        skip = True
        if job_name == JobNames.BUGFIX_VALIDATE_FT and any(
            f.endswith("jobs/functional_tests.py") for f in changed_files
        ):
            skip = False
        elif job_name == JobNames.BUGFIX_VALIDATE_IT and any(
            f.endswith("jobs/integration_test_job.py") for f in changed_files
        ):
            skip = False

        if skip:
            return True, "Skipped, not a bug-fix PR"

    if "flaky" in job_name.lower():
        changed_files = _info_cache.get_changed_files()
        if "stateless" in job_name.lower() and not has_new_functional_tests(
            changed_files
        ):
            return True, "Skipped, no functional tests updates"
        if "integration" in job_name.lower() and not has_new_integration_tests(
            changed_files
        ):
            return True, "Skipped, no integration tests updates"

    # Skip bug fix validation jobs even for bufgfix prs if no corresponding updates are found.
    #  ci/jobs/scripts/workflow_hooks/new_tests_check.py hook validates whether at list one type of tests has updates
    if (
        " Bug Fix" in _info_cache.pr_body
        and job_name == JobNames.BUGFIX_VALIDATE_FT
        and not has_new_functional_tests(_info_cache.get_changed_files())
    ):
        return True, "Skipped, no functional tests updates"

    if (
        " Bug Fix" in _info_cache.pr_body
        and job_name == JobNames.BUGFIX_VALIDATE_IT
        and not has_new_integration_tests(_info_cache.get_changed_files())
    ):
        return True, "Skipped, no integration tests updates"

    # skip ARM perf tests for non-performance update
    if (
        "- Performance Improvement" not in _info_cache.pr_body
        and Labels.CI_PERFORMANCE not in _info_cache.pr_labels
        and JobNames.PERFORMANCE in job_name
        and "arm" in job_name
        and _info_cache.pr_number  # run all performance jobs on master
    ):
        return True, "Skipped, not labeled with 'pr-performance'"

    # If only the functional tests script changed, run only the first batch of stateless tests
    if changed_files and all(
        f.startswith("ci/") and f.endswith(".py") for f in changed_files
    ):
        if JobNames.STATELESS in job_name:
            match = re.search(r"(\d)/\d", job_name)
            if match and match.group(1) != "1" or "sequential" in job_name:
                return True, "Skipped, only job script changed - run first batch only"

        if JobNames.INTEGRATION in job_name:
            match = re.search(r"(\d)/\d", job_name)
            if (
                match
                and match.group(1) != "1"
                or "sequential" in job_name
                or "_asan" not in job_name
            ):
                return True, "Skipped, only job script changed - run first batch only"

    return False, ""
