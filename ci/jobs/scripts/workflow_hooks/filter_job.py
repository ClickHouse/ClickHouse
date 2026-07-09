import re

from ci.defs.defs import JobNames
from ci.defs.job_configs import JobConfigs, build_digest_config
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

BUILDS_FOR_TESTS = [
    j.name
    for j in JobConfigs.build_jobs
    + JobConfigs.coverage_build_jobs
    + JobConfigs.release_build_jobs
]

INTEGRATION_TEST_FLAKY_CHECK_JOBS = [
    "Build (amd_asan_ubsan)",
    "Integration tests (amd_asan_ubsan, flaky)",
]

FUNCTIONAL_TEST_FLAKY_CHECK_JOBS = [
    "Build (amd_asan_ubsan)",
    "Build (amd_tsan)",
    "Build (amd_msan)",
    "Build (amd_debug)",
    "Build (amd_binary)",
    "Stateless tests (amd_asan_ubsan, flaky check)",
    "Stateless tests (amd_tsan, flaky check)",
    "Stateless tests (amd_msan, flaky check)",
    "Stateless tests (amd_debug, flaky check)",
    "Stateless tests (amd_binary, flaky check)",
]

# Must match ci.workflows.pull_request.KEEPER_STRESS_PR_NAME
KEEPER_STRESS_PR_NAME = "Keeper Stress Tests (PR)"


def _has_keeper_stress_changes(changed_files):
    """True if any changed file is under src/Coordination, tests/stress/keeper, programs/keeper-bench, or ci/jobs/keeper_stress_job.py."""
    for f in changed_files:
        p = f.removeprefix(".").removeprefix("/")
        if (
            p.startswith("src/Coordination")
            or p.startswith("tests/stress/keeper")
            or p.startswith("programs/keeper-bench")
            or p == "ci/jobs/keeper_stress_job.py"
        ):
            return True
    return False


def _has_build_digest_changes(changed_files):
    """True if any changed file may affect the compiled ClickHouse binary,
    per `build_digest_config.include_paths`/`exclude_paths` - the same paths
    that gate the build job's cache digest in `ci/defs/job_configs.py`.
    """
    include = [p.removeprefix("./") for p in build_digest_config.include_paths]
    exclude = [p.removeprefix("./") for p in build_digest_config.exclude_paths]
    for f in changed_files:
        p = f.removeprefix(".").removeprefix("/")
        if any(p.startswith(inc) for inc in include) and not any(
            p.startswith(exc) for exc in exclude
        ):
            return True
    return False


_info_cache = None

# Labels that mark a PR as a bug fix (set by the `pr_labels_and_category.py`
# pre-hook from the changelog category). Gating Bugfix Validation on labels
# rather than a free-text scan of the PR body avoids accidentally enabling or
# failing the check on ordinary PR text that merely mentions "Bug Fix".
_BUGFIX_LABELS = (Labels.PR_BUGFIX, Labels.PR_CRITICAL_BUGFIX)


def _is_bugfix_pr():
    return any(lb in _info_cache.pr_labels for lb in _BUGFIX_LABELS)


def _classify_changed_tests(changed_files):
    """Return (stateless, integration, unit, binary_unchanged).

    binary_unchanged is True only when every changed path is non-binary
    (tests/, ci/, docs/, top-level metadata). If any source/cmake/contrib
    file changed the binary is potentially different.
    """
    NON_BINARY_TOP = frozenset({
        "README.md", "LICENSE", "CHANGELOG.md", "CONTRIBUTING.md",
        "NOTICE", "AUTHORS", "CODE_OF_CONDUCT.md", "SECURITY.md",
        ".gitignore", ".gitattributes", ".editorconfig",
        ".dockerignore", ".clang-format", ".clang-tidy",
    })

    def _non_binary(p):
        p = p.removeprefix(".").removeprefix("/")
        return (
            p.startswith("tests/")
            or p.startswith("ci/")
            or p.startswith(".github/")
            or p.startswith("docs/")
            or (p.startswith("src/") and "/tests/" in p)
            or ("/" not in p and p in NON_BINARY_TOP)
        )

    stateless = any(
        f.removeprefix(".").removeprefix("/").startswith("tests/queries/")
        for f in changed_files
    )
    integration = any(
        f.removeprefix(".").removeprefix("/").startswith("tests/integration/test_")
        for f in changed_files
    )
    unit = any(
        f.removeprefix(".").removeprefix("/").startswith("src/")
        and "/tests/" in f
        for f in changed_files
    )
    binary_unchanged = bool(changed_files) and all(_non_binary(f) for f in changed_files)

    # Do not classify as single-type if any changed path is inside a test-type
    # directory but is NOT a runnable test definition (e.g. tests/integration/helpers/,
    # tests/integration/runner, or tests/integration/conftest.py). Such paths are
    # binary-safe but affect test behaviour across types — skipping integration
    # coverage because helpers/ changed alongside one stateless test is wrong.
    def _is_integration_infra(p):
        p = p.removeprefix(".").removeprefix("/")
        return p.startswith("tests/integration/") and not p.startswith("tests/integration/test_")

    if integration or any(_is_integration_infra(f) for f in changed_files):
        # Treat any integration-directory change (helper, runner, conftest, …) as
        # an integration change so integration coverage is never skipped.
        integration = integration or any(_is_integration_infra(f) for f in changed_files)

    return stateless, integration, unit, binary_unchanged


def should_skip_job(job_name):
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()
        print(f"INFO: PR labels: {_info_cache.pr_labels}")

    # There is no way to prevent GitHub Actions from running the PR workflow on
    # release branches, so we skip all jobs here. The ReleaseCI workflow is used
    # for testing on release branches instead.
    if (
        Labels.RELEASE in _info_cache.pr_labels
        or Labels.RELEASE_LTS in _info_cache.pr_labels
    ):
        return True, "Skipped for release PR"

    changed_files = _info_cache.get_kv_data("changed_files")
    if not changed_files:
        print("WARNING: no changed files found for PR - do not filter jobs")
        return False, ""

    # Run Keeper Stress jobs only when there are changes in src/Coordination,
    # tests/stress/keeper, or ci/jobs/keeper_stress_job.py
    if job_name == KEEPER_STRESS_PR_NAME:
        if not _has_keeper_stress_changes(changed_files):
            return (
                True,
                "Skipped, no changes in src/Coordination, tests/stress/keeper, or keeper_stress_job.py",
            )
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
        or job_name in BUILDS_FOR_TESTS
    ):
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_INTEGRATION}' - run integration test jobs only",
        )

    if Labels.CI_FUNCTIONAL in _info_cache.pr_labels and not (
        job_name.startswith(JobNames.STATELESS)
        or job_name.startswith(JobNames.STATEFUL)
        or job_name in BUILDS_FOR_TESTS
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

    # Skip the whole coverage family together: the coverage build, the amd_llvm_coverage test shards, the excluded_from_llvm jobs
    # (they only run the tests the coverage shards skip, so they are pointless without them), and the final "LLVM Coverage" merge job.
    if Labels.CI_NO_COVERAGE in _info_cache.pr_labels and (
        "llvm_coverage" in job_name
        or "excluded_from_llvm" in job_name
        or job_name == JobNames.LLVM_COVERAGE
    ):
        return True, f"Skipped, labeled with '{Labels.CI_NO_COVERAGE}'"

    if not _is_bugfix_pr() and "Bugfix" in job_name:
        # Don't skip if the corresponding test job file was changed
        skip = True
        if job_name in (
            JobNames.BUGFIX_VALIDATE_FT_AMD,
            JobNames.BUGFIX_VALIDATE_FT_ARM,
        ) and any(f.endswith("jobs/functional_tests.py") for f in changed_files):
            skip = False
        elif job_name in (
            JobNames.BUGFIX_VALIDATE_IT_AMD,
            JobNames.BUGFIX_VALIDATE_IT_ARM,
        ) and any(
            f.endswith("jobs/integration_test_job.py") for f in changed_files
        ):
            skip = False

        if skip:
            return True, "Skipped, not a bug-fix PR"

    if "flaky" in job_name.lower():
        from ci.jobs.scripts.find_tests import Targeting

        targeter = Targeting(info=_info_cache)
        # _info_cache.job_name is the hook runner job, not the flaky check job.
        # Set job_type explicitly from the job_name argument so CIDB queries use
        # the correct check_name prefix (e.g. 'Stateless%' instead of None).
        if "stateless" in job_name.lower():
            targeter.job_type = Targeting.STATELESS_JOB_TYPE
        elif "integration" in job_name.lower():
            targeter.job_type = Targeting.INTEGRATION_JOB_TYPE
        changed_files = _info_cache.get_changed_files()
        if "stateless" in job_name.lower():
            changed_tests = targeter.get_changed_tests()
            try:
                previously_failed = targeter.get_previously_failed_tests()
            except Exception as e:
                print(f"Warning: failed to fetch previously-failed tests: {e}")
                previously_failed = []
            if not changed_tests and not previously_failed:
                return True, "Skipped, no tests to run"
        if "integration" in job_name.lower() and not has_new_integration_tests(
            changed_files
        ):
            return True, "Skipped, no integration tests updates"

    # Skip bug fix validation jobs even for bugfix PRs if no corresponding updates are found.
    #  ci/jobs/scripts/workflow_hooks/new_tests_check.py hook validates whether at least one type of tests has updates
    #
    # On a Bug-Fix PR that only touches integration tests, the per-arch
    # functional-test jobs would otherwise run anyway, find nothing to validate
    # against, and report FAIL even though they should not have been running.
    if (
        _is_bugfix_pr()
        and job_name in (
            JobNames.BUGFIX_VALIDATE_FT_AMD,
            JobNames.BUGFIX_VALIDATE_FT_ARM,
        )
        and not has_new_functional_tests(_info_cache.get_changed_files())
    ):
        return True, "Skipped, no functional tests updates"

    if (
        _is_bugfix_pr()
        and job_name in (
            JobNames.BUGFIX_VALIDATE_IT_AMD,
            JobNames.BUGFIX_VALIDATE_IT_ARM,
        )
        and not has_new_integration_tests(_info_cache.get_changed_files())
    ):
        return True, "Skipped, no integration tests updates"

    # skip AMD perf tests for non-performance update (ARM runs by default)
    if (
        " Performance Improvement" not in _info_cache.pr_body
        and Labels.CI_PERFORMANCE not in _info_cache.pr_labels
        and Labels.PR_PERFORMANCE not in _info_cache.pr_labels
        and JobNames.PERFORMANCE in job_name
        and "amd" in job_name
        and _info_cache.pr_number  # run all performance jobs on master
    ):
        return True, "Skipped, not labeled with 'pr-performance'"

    # --- Coverage sub-job skipping based on changed test type (PR only) ---
    # When only one class of tests changed and the binary is unchanged, skip
    # the coverage sub-jobs for the other test types. The llvm_coverage_job
    # (final merge) supplements the missing profdata from master.
    # Only applies to PRs: master coverage runs must always publish a complete
    # llvm_coverage.info. If master skipped IT/unit, the published baseline
    # would be partial, and later PRs comparing against it would see a huge
    # artificial "newly covered" spike from all the lines missing in the baseline.
    # Only skip when the binary is provably unchanged; any source/cmake change
    # keeps all coverage jobs active.
    _is_coverage_ft  = "amd_llvm_coverage" in job_name and JobNames.STATELESS in job_name
    _is_coverage_it  = "amd_llvm_coverage" in job_name and JobNames.INTEGRATION in job_name
    _is_coverage_ut  = "amd_llvm_coverage" in job_name and JobNames.UNITTEST in job_name
    if (_is_coverage_ft or _is_coverage_it or _is_coverage_ut) and _info_cache.pr_number > 0:
        cf = _info_cache.get_changed_files() or []
        _sl, _it, _ut, _bin_unch = _classify_changed_tests(cf)
        if _bin_unch and (_sl or _it or _ut):
            # Only one type changed — skip the other two.
            if _sl and not _it and not _ut:
                if _is_coverage_it:
                    return True, "Skipped: only stateless tests changed, integration coverage not needed"
                if _is_coverage_ut:
                    return True, "Skipped: only stateless tests changed, unit coverage not needed"
            elif _it and not _sl and not _ut:
                if _is_coverage_ft:
                    return True, "Skipped: only integration tests changed, stateless coverage not needed"
                if _is_coverage_ut:
                    return True, "Skipped: only integration tests changed, unit coverage not needed"
            elif _ut and not _sl and not _it:
                if _is_coverage_ft:
                    return True, "Skipped: only unit tests changed, stateless coverage not needed"
                if _is_coverage_it:
                    return True, "Skipped: only unit tests changed, integration coverage not needed"

    # Run the LLVM Coverage merge/report job only when the build itself may have
    # changed. Coverage numbers only move when the compiled binary changes; a
    # PR that only touches tests/docs/CI scripts would produce a merge report
    # identical to master, so skip it. PR-only, for the same reason as the
    # coverage sub-job skipping above: master coverage runs must always
    # publish a complete baseline.
    if (
        job_name == JobNames.LLVM_COVERAGE
        and _info_cache.pr_number > 0
        and not _has_build_digest_changes(_info_cache.get_changed_files() or [])
    ):
        return True, "No build-affecting changes"

    # If only CI scripts changed (no product code), run a minimal set of tests
    # to validate the CI pipeline: stateless batch 1 and amd_asan_ubsan integration batch 1.
    # Individual coverage test jobs run normally; the LLVM merge/report job is
    # already skipped above whenever the build is unaffected.
    if changed_files and all(
        f.startswith("ci/") and f.endswith(".py") for f in changed_files
    ):
        if JobNames.STATELESS in job_name:
            match = re.search(r"(\d)/\d", job_name)
            if match and match.group(1) != "1" or "sequential" in job_name:
                return True, "Skipped: only CI scripts changed; running stateless batch 1 only"

        if JobNames.INTEGRATION in job_name:
            match = re.search(r"(\d)/\d", job_name)
            if (
                match
                and match.group(1) != "1"
                or "sequential" in job_name
                or "_asan" not in job_name
            ):
                return True, "Skipped: only CI scripts changed; running amd_asan_ubsan integration batch 1 only"

    return False, ""
