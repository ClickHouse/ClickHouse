import re

from ci.defs.defs import JobNames
from ci.defs.job_configs import JobConfigs, build_digest_config
from ci.jobs.scripts.workflow_hooks.new_tests_check import (
    has_new_functional_tests,
    has_new_integration_tests,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.utils import Shell


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

# The Darwin (macOS) "Fast test" jobs, resolved to their parametrized names
# (e.g. "Fast test (arm_darwin)"). They run on scarce self-hosted macOS runners,
# so in PRs they are skipped unless the PR carries the `ci-macos` label.
DARWIN_FAST_TEST_JOBS = [j.name for j in JobConfigs.darwin_fast_test_jobs]

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


# Files whose content directly drives the LLVM coverage pipeline's own
# behaviour (test-shard execution, profdata merging, report/diff generation,
# and this job-filtering logic itself) - as opposed to files that merely add
# or edit a test case. `_has_build_digest_changes` only tracks whether the
# *compiled binary* can change, so a PR that only touches one of these would
# otherwise be auto-skipped as "tests-only" and could never exercise the
# coverage-specific code it just modified.
_COVERAGE_PIPELINE_PATHS = (
    "ci/jobs/llvm_coverage_job.py",
    "ci/jobs/functional_tests.py",
    "ci/jobs/integration_test_job.py",
    "ci/jobs/unit_tests_job.py",
    # LLVM_COVERAGE_SKIP_PREFIXES here decides which integration suites land in
    # amd_llvm_coverage vs excluded_from_llvm (integration_test_job.py:433-462).
    "ci/jobs/scripts/integration_tests_configs.py",
    "ci/jobs/scripts/merge_llvm_coverage.sh",
    "ci/jobs/scripts/generate_diff_coverage_report.sh",
    "ci/jobs/scripts/print_uncovered_code.py",
    "ci/jobs/scripts/dedup_lcov_instantiations.py",
    "ci/jobs/scripts/job_hooks/llvm_coverage_hook.py",
    "ci/jobs/scripts/workflow_hooks/filter_job.py",
    # Both set LLVM_PROFILE_FILE for the servers, i.e. whether their profiles
    # are continuous-mode kill-safe.
    "ci/jobs/scripts/clickhouse_proc.py",
    "tests/integration/helpers/cluster.py",
    "ci/defs/job_configs.py",
    "ci/defs/defs.py",
    "tests/clickhouse-test",
    "tests/config/",
)


def _has_coverage_pipeline_changes(changed_files):
    """True if any changed file could alter how the coverage pipeline itself
    behaves, independent of whether the compiled binary changed. See
    `_COVERAGE_PIPELINE_PATHS`.
    """
    for f in changed_files:
        p = f.removeprefix(".").removeprefix("/")
        if any(p.startswith(path) for path in _COVERAGE_PIPELINE_PATHS):
            return True
    return False


_info_cache = None
_pipeline_note_labels = set()

_PIPELINE_NOTES = {
    Labels.CI_BUILD: "Label `ci-build` runs build jobs and preliminary checks only.",
    Labels.DO_NOT_TEST: (
        "Label `do not test` runs only `STYLE_CHECK`, `DOCKER_BUILDS_ARM`, and "
        "`DOCKER_BUILDS_AMD`."
    ),
    Labels.NO_FAST_TESTS: (
        "Label `no-fast-tests` skips only `STYLE_CHECK` and `FAST_TEST`; merge is "
        "still allowed because the merge queue runs those checks."
    ),
    Labels.CI_INTEGRATION_FLAKY: (
        "Label `ci-integration-test-flaky` runs the integration flaky-check jobs only."
    ),
    Labels.CI_FUNCTIONAL_FLAKY: (
        "Label `ci-functional-test-flaky` runs the stateless flaky-check jobs only."
    ),
    Labels.CI_INTEGRATION: (
        "Label `ci-integration-test` runs integration test jobs only."
    ),
    Labels.CI_FUNCTIONAL: (
        "Label `ci-functional-test` runs stateless and stateful test jobs only."
    ),
    Labels.CI_PERFORMANCE: (
        "Label `ci-performance` runs performance jobs only."
    ),
    Labels.CI_NO_COVERAGE: (
        "Label `ci-no-coverage` skips coverage jobs and the `LLVM Coverage` merge job."
    ),
    Labels.CI_MACOS: (
        "Label `ci-macos` runs the Darwin (macOS) `Fast test` job, which is "
        "skipped by default in PRs."
    ),
}


def _add_pipeline_note(label):
    if _info_cache is None or label in _pipeline_note_labels:
        return
    message = _PIPELINE_NOTES.get(label)
    if not message:
        return
    _pipeline_note_labels.add(label)
    _info_cache.add_workflow_note(message)

# Labels that mark a PR as a bug fix (set by the `pr_labels_and_category.py`
# pre-hook from the changelog category). Gating Bugfix Validation on labels
# rather than a free-text scan of the PR body avoids accidentally enabling or
# failing the check on ordinary PR text that merely mentions "Bug Fix".
_BUGFIX_LABELS = (Labels.PR_BUGFIX, Labels.PR_CRITICAL_BUGFIX)


def _is_bugfix_pr():
    return any(lb in _info_cache.pr_labels for lb in _BUGFIX_LABELS)


def _is_empty_merge_commit(sha):
    """True if `sha` is a merge commit (>=2 parents) that introduced no changes -
    i.e. its diff against the first parent is empty.

    This is the commit produced by merging the base branch into the PR branch when
    the merge brings nothing new (e.g. the GitHub "Update branch" button on a branch
    that is already effectively up to date). The reviewed code is then identical to
    the previous head, so re-running the AI `Code Review` job would only repeat the
    previous review.

    Resolved via the GitHub API rather than local git: the CI checkout may be a
    shallow clone that lacks the merge commit's parents, and the commits endpoint
    reports `.files` for a merge commit relative to its first parent. Returns False
    on any uncertainty (not a merge, API error, unparseable output) so that we
    prefer to run the review rather than silently skip it.
    """
    out = Shell.get_output(
        f"gh api repos/{_info_cache.repo_name}/commits/{sha} "
        "--jq '\"\\(.parents | length) \\(.files | length)\"'",
        verbose=True,
        retries=3,
    ).split()
    if len(out) != 2 or not all(s.isdigit() for s in out):
        print(f"WARNING: could not determine parents/files for commit {sha}")
        return False
    num_parents, num_files = int(out[0]), int(out[1])
    return num_parents >= 2 and num_files == 0


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

    # The AI `Code Review` job reviews the PR's code. When the PR's latest commit is
    # an empty merge commit (base branch merged in with no net change - e.g. the
    # GitHub "Update branch" button), the code is identical to the previous head and
    # a fresh review would only repeat itself, so skip it.
    if (
        job_name == JobNames.CODE_REVIEW
        and _info_cache.pr_number > 0
        and _is_empty_merge_commit(_info_cache.sha)
    ):
        return True, "Skipped, PR latest commit is an empty merge commit"

    changed_files = _info_cache.get_kv_data("changed_files")
    if not changed_files:
        print("WARNING: no changed files found for PR - do not filter jobs")
        return False, ""

    if job_name == JobNames.BUILD_PROFILE_DIFF and only_docs(changed_files):
        return True, "Skipped, only documentation changed"

    # Run Keeper Stress jobs only when there are changes in src/Coordination,
    # tests/stress/keeper, or ci/jobs/keeper_stress_job.py
    if job_name == KEEPER_STRESS_PR_NAME:
        if not _has_keeper_stress_changes(changed_files):
            return (
                True,
                "Skipped, no changes in src/Coordination, tests/stress/keeper, or keeper_stress_job.py",
            )
        return False, ""

    # The Darwin (macOS) fast test runs on scarce self-hosted macOS runners, so
    # in PRs it runs only when explicitly requested via the `ci-macos` label.
    # Master has no such job, so this gate is a no-op there.
    if (
        job_name in DARWIN_FAST_TEST_JOBS
        and _info_cache.pr_number
        and Labels.CI_MACOS not in _info_cache.pr_labels
    ):
        return True, f"Skipped, not labeled with '{Labels.CI_MACOS}'"

    if (
        Labels.CI_BUILD in _info_cache.pr_labels
        and "build" not in job_name.lower()
        and job_name not in PRELIMINARY_JOBS
    ):
        _add_pipeline_note(Labels.CI_BUILD)
        return True, f"Skipped, labeled with '{Labels.CI_BUILD}'"

    if Labels.DO_NOT_TEST in _info_cache.pr_labels and job_name not in DO_NOT_TEST_JOBS:
        _add_pipeline_note(Labels.DO_NOT_TEST)
        return True, f"Skipped, labeled with '{Labels.DO_NOT_TEST}'"

    if Labels.NO_FAST_TESTS in _info_cache.pr_labels and job_name in PRELIMINARY_JOBS:
        _add_pipeline_note(Labels.NO_FAST_TESTS)
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
        _add_pipeline_note(Labels.CI_INTEGRATION_FLAKY)
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_INTEGRATION_FLAKY}' - run integration test flaky check job only",
        )

    if (
        Labels.CI_FUNCTIONAL_FLAKY in _info_cache.pr_labels
        and job_name not in FUNCTIONAL_TEST_FLAKY_CHECK_JOBS
    ):
        _add_pipeline_note(Labels.CI_FUNCTIONAL_FLAKY)
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_FUNCTIONAL_FLAKY}' - run stateless test jobs only",
        )

    if Labels.CI_INTEGRATION in _info_cache.pr_labels and not (
        job_name.startswith(JobNames.INTEGRATION)
        or job_name in BUILDS_FOR_TESTS
        or (
            job_name == JobNames.PROMQL_COMPLIANCE
            and Labels.COMP_PROMQL in _info_cache.pr_labels
        )
    ):
        _add_pipeline_note(Labels.CI_INTEGRATION)
        return (
            True,
            f"Skipped, labeled with '{Labels.CI_INTEGRATION}' - run integration test jobs only",
        )

    if (
        job_name == JobNames.PROMQL_COMPLIANCE
        and Labels.COMP_PROMQL not in _info_cache.pr_labels
    ):
        return (
            True,
            f"Skipped, PR not labeled '{Labels.COMP_PROMQL}' — PromQL compliance comment job only",
        )

    if Labels.CI_FUNCTIONAL in _info_cache.pr_labels and not (
        job_name.startswith(JobNames.STATELESS)
        or job_name.startswith(JobNames.STATEFUL)
        or job_name in BUILDS_FOR_TESTS
        or "functional" in job_name.lower()  # Bugfix validation (functional tests)
    ):
        _add_pipeline_note(Labels.CI_FUNCTIONAL)
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
        _add_pipeline_note(Labels.CI_PERFORMANCE)
        return (
            True,
            "Skipped, labeled with 'ci-performance' - run performance jobs only",
        )

    # Skip the whole coverage family together: the coverage build, the amd_llvm_coverage test shards, the excluded_from_llvm jobs
    # (they only run the tests the coverage shards skip, so they are pointless without them), and the final "LLVM Coverage" merge job.
    #
    # This also fires automatically, without the label, whenever a PR has no build-digest-affecting
    # changes (i.e. it only touches tests/docs/CI scripts) AND does not touch the coverage pipeline's
    # own code (`_has_coverage_pipeline_changes`) - a PR fixing a bug in llvm_coverage_job.py, this
    # hook, or the coverage-relevant parts of functional_tests.py/integration_test_job.py must still
    # be able to run the jobs it changed, even though it changes no compiled-binary path. Coverage
    # numbers only move when the compiled binary changes, so an ordinary tests-only PR would produce
    # coverage identical to master - running any part of the family just burns CI time on profdata that
    # the (also-skipped) merge job would never consume. Master itself is unaffected (pr_number gate):
    # its coverage runs must always publish a complete llvm_coverage.info for later PRs to compare against.
    if (
        "llvm_coverage" in job_name
        or "excluded_from_llvm" in job_name
        or job_name == JobNames.LLVM_COVERAGE
    ) and (
        Labels.CI_NO_COVERAGE in _info_cache.pr_labels
        or (
            _info_cache.pr_number > 0
            and not _has_build_digest_changes(_info_cache.get_changed_files() or [])
            and not _has_coverage_pipeline_changes(_info_cache.get_changed_files() or [])
        )
    ):
        if Labels.CI_NO_COVERAGE in _info_cache.pr_labels:
            _add_pipeline_note(Labels.CI_NO_COVERAGE)
            return True, f"Skipped, labeled with '{Labels.CI_NO_COVERAGE}'"
        return True, "Skipped: no build-affecting changes; coverage would be identical to master"

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
        changed_files = _info_cache.get_changed_files()
        if "stateless" in job_name.lower():
            from ci.jobs.scripts.find_tests import Targeting

            # Mirrors the in-job selection in `functional_tests.py`. Runs inside
            # `Config Workflow`, so it must issue no CIDB query.
            if not Targeting(info=_info_cache).get_changed_tests():
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

    # If only CI scripts changed (no product code), run a minimal set of tests
    # to validate the CI pipeline: stateless batch 1 and amd_asan_ubsan integration batch 1.
    # The whole coverage family is already skipped above whenever the build is
    # unaffected, so this only narrows down the plain (non-coverage) test jobs.
    if changed_files and all(
        f.startswith("ci/") and f.endswith(".py") for f in changed_files
    ):
        if JobNames.STATELESS in job_name:
            match = re.search(r"(\d)/\d", job_name)
            if (
                (match and match.group(1) != "1")
                or ("sequential" in job_name and "selected tests" not in job_name)
            ):
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


def should_skip_merge_queue_job(job_name):
    """Config-time filter for the `MergeQueueCI` workflow.

    The merge queue runs a small, fixed set of jobs (style check, fast test, the
    `amd_binary` build, and the stateless flaky check). Only the flaky check is
    conditional: it reruns the PR's new/changed stateless tests as a drift guard,
    so a PR that changes no stateless tests has nothing for it to do. Filter it
    out here, at config time, so such a PR does not schedule the runner, restore
    `CH_AMD_BINARY`, and enter the test container only to exit `SKIPPED`. This is
    the merge-queue counterpart to the `flaky` branch of `should_skip_job`, kept
    deliberately minimal so it cannot skip the build/style/fast-test jobs the
    queue always needs. The skip condition matches the in-job selection in
    `functional_tests.py` (both rely on `Targeting.get_changed_tests`), so the
    early exit and the config-time skip never disagree. `get_changed_tests`
    resolves data fixtures (a `.parquet`/`.tsv` under `tests/queries/0_stateless/`,
    even one nested in a subdirectory) back to the tests that consume them, so a
    fixture-only PR still reruns the affected test surface instead of being
    skipped here as "no changed tests".
    """
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()

    if "flaky" not in job_name.lower() or "stateless" not in job_name.lower():
        return False, ""

    from ci.jobs.scripts.find_tests import Targeting

    targeter = Targeting(info=_info_cache)
    targeter.job_type = Targeting.STATELESS_JOB_TYPE
    if not targeter.get_changed_tests():
        return True, "Skipped, no new/changed stateless tests to rerun in the merge queue"
    return False, ""
