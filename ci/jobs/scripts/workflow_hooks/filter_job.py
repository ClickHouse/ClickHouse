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
    "Integration tests (amd_asan, flaky check)",
]

FUNCTIONAL_TEST_FLAKY_CHECK_JOBS = [
    "Build (amd_asan)",
    "Stateless tests (amd_asan, flaky check)",
]

_info_cache = None


def should_skip_job(job_name):
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()

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
    ):
        if "release_base" in job_name and not _info_cache.pr_number:
            # comparison with the latest release merge base - do not skip on master
            return False, ""
        return True, "Skipped, not labeled with 'pr-performance'"

    return False, ""
