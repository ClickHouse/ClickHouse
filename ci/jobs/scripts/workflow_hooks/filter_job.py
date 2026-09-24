import re
from pathlib import PurePosixPath

from ci.defs.defs import JobNames
from ci.defs.job_configs import JobConfigs, build_digest_config
from ci.jobs.scripts.workflow_hooks.new_tests_check import (
    has_new_functional_tests,
    has_new_integration_tests,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.jobs.scripts.workflow_hooks.review_threads import (
    KV_OVERRIDE,
    KV_UNRESOLVED_COUNT,
    should_limit_pipeline,
)
from ci.jobs.scripts.workflow_hooks.store_data import PRODUCT_CODE_PATHS
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

# Keep this in sync with the build jobs configured in
# ci/workflows/pull_request.py. Unlike a name-based check, this excludes jobs
# such as `Build profile diff` that are not builds despite their names.
REVIEW_THREADS_BUILD_JOBS = [
    j.name
    for j in JobConfigs.tidy_build_arm_jobs
    + JobConfigs.build_jobs
    + JobConfigs.extra_validation_build_jobs
    + JobConfigs.release_build_jobs_with_examples
    + JobConfigs.special_build_jobs
    + JobConfigs.build_llvm_coverage_job
    + JobConfigs.toolchain_build_jobs
    + JobConfigs.wasm_parser_build_jobs
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
    "ci/jobs/scripts/newly_covered_lines.py",
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


# Stress tests, fuzzers and the SQL conformance suites (`SQLLogic test`,
# `SQLStorm test`) are skipped in a PR that changes fewer than this many lines
# (additions + deletions) of product code - counted by the `store_data.py`
# pre-hook as `product_changed_lines` over `PRODUCT_CODE_PATHS`, the part of the
# build digest that ends up in the built server. Tests, docs and CI scripts do
# not count. The `ci-force-all` label (`Labels.CI_FORCE_ALL`) bypasses every
# filter hook, including this one, so it is the way to run these jobs on a small
# PR.
SMALL_PR_CHANGED_LINES = 100

# Only the main PR workflow skips these jobs. `BackportPR` is a `pull_request`
# workflow using this same hook, and a backport has to be validated in full
# whatever its size: it lands in a release branch, which `ClickGap` (which fuzzes
# every commit merged to master) never fuzzes afterwards.
# Must match the workflow name in ci.workflows.pull_request.
SMALL_PR_WORKFLOW = "PR"

# The `targeted` AST fuzzer variants fuzz the tests that exercise the PR's changed
# symbols, i.e. they are designed for exactly the small PRs this rule skips the
# untargeted fuzzers on, so they keep running. `SQLLogic test` and `SQLStorm test`
# run fixed third-party suites unrelated to the change: over the 30 days before
# 2026-09-19 their only PR failures were infrastructure (`Start ClickHouse`,
# `Download dataset`), at 108 and 19 minutes per run.
_STRESS_AND_FUZZER_JOB_PREFIXES = (
    JobNames.STRESS,
    JobNames.ASTFUZZER,
    JobNames.BUZZHOUSE,
    JobNames.SQL_LOGIC_TEST,
    JobNames.SQL_STORM_TEST,
)

# Digest inputs of the skippable jobs that must not switch the skip off: a fifth
# of all commits touches the stateless suite, so exempting it would make the rule
# never fire. It is the only path of that frequency, and the other test inputs of
# these jobs are deliberately absent: `tests/config`, from which the runners
# install their server configuration (`run-fuzzer.sh` copies `listen.xml`,
# `ssl_certs.xml`, `server.crt` and friends, `stress.py` installs
# `cannot_allocate_thread_injection.xml`), and `tests/*.txt`, the blacklists
# `tests/clickhouse-test` reads to decide which tests run. Both change in well
# under 1% of commits.
_COMMON_TEST_PATHS = ("tests/queries/0_stateless/",)

# Machinery of these jobs that is not a digest input of theirs, but still decides
# what they do or whether they run at all.
_EXTRA_STRESS_AND_FUZZER_PATHS = (
    # The fuzzers themselves live in the server code.
    "src/Client/BuzzHouse/",
    "src/Common/QueryFuzzer*",
    # This rule, and the pre-hook computing the line count it reads.
    "ci/jobs/scripts/workflow_hooks/filter_job.py",
    "ci/jobs/scripts/workflow_hooks/store_data.py",
    # What defines these jobs and puts them into the workflow: their commands,
    # parameters, runners, timeouts and digests. A PR that rewrites the job
    # definition and touches a few lines of `src/` on top would otherwise skip the
    # very jobs it redefined. Same reasoning as `_COVERAGE_PIPELINE_PATHS` above.
    "ci/defs/job_configs.py",
    "ci/defs/defs.py",
    "ci/workflows/pull_request.py",
    # And praktika itself, which decides how any of it is scheduled and run.
    "ci/praktika/",
)


def _stress_and_fuzzer_paths():
    """Paths whose change makes a PR run the stress tests, fuzzers and SQL suites whatever its
    size. Derived from the digest `include_paths` of the very jobs this rule can
    skip, so an input added to one of them keeps its exemption here without a
    second edit, minus `_COMMON_TEST_PATHS` and plus
    `_EXTRA_STRESS_AND_FUZZER_PATHS`.
    """
    paths = set(_EXTRA_STRESS_AND_FUZZER_PATHS)
    for job in (
        *JobConfigs.stress_test_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        JobConfigs.sqllogic_test_master_job,
        JobConfigs.sqlstorm_test_job,
    ):
        for path in job.digest_config.include_paths:
            path = path.removeprefix("./")
            if path not in _COMMON_TEST_PATHS:
                paths.add(path)
    return tuple(sorted(paths))


_STRESS_AND_FUZZER_PATHS = _stress_and_fuzzer_paths()


def _uncounted_build_paths():
    """Build-digest inputs whose changed lines `store_data.py` does not count - it
    counts `PRODUCT_CODE_PATHS` only. Their diff size says nothing about the size
    of the change to the binary: a bumped gitlink under `contrib/` is two lines and
    an arbitrary amount of new third-party code, and a one-line compiler flag in
    `ci/jobs/build_clickhouse.py` rebuilds everything. A PR touching one of them is
    therefore never small, which keeps the invariant that every input of the build
    digest either counts towards the threshold or takes the PR out of the rule.
    """
    counted = {path.rstrip("/") for path in PRODUCT_CODE_PATHS}
    return tuple(
        sorted(
            path
            for path in (p.removeprefix("./") for p in build_digest_config.include_paths)
            if path.rstrip("/") not in counted
        )
    )


_UNCOUNTED_BUILD_PATHS = _uncounted_build_paths()
_BUILD_DIGEST_EXCLUDES = tuple(
    p.removeprefix("./") for p in build_digest_config.exclude_paths
)


def _is_stress_or_fuzzer_job(job_name):
    return job_name.startswith(_STRESS_AND_FUZZER_JOB_PREFIXES) and "targeted" not in job_name


def _matches_digest_path(path, patterns):
    """Whether `path` is covered by one of `patterns`, matched the way praktika
    matches a job's digest `include_paths` in `Job.is_affected_by`: a pattern is a
    directory prefix, an exact path, or a glob. Prefix matching alone would silently
    ignore the glob entries - `tests/*.txt` holds the blacklists `clickhouse-test`
    reads, and no file name starts with that string.

    `path` must already have its `./` prefix stripped. That is done by the caller,
    with `removeprefix("./")` rather than the `.`-then-`/` idiom of the older helpers
    in this file, because some of these paths are root dotfiles (`.gitmodules`).
    """
    for pattern in patterns:
        pattern = pattern.rstrip("/")
        if PurePosixPath("/" + path).match("/" + pattern) or path.startswith(
            pattern + "/"
        ):
            return True
    return False


def _has_stress_or_fuzzer_changes(changed_files):
    return any(
        _matches_digest_path(f.removeprefix("./"), _STRESS_AND_FUZZER_PATHS)
        for f in changed_files
    )


def _has_uncounted_build_changes(changed_files):
    """True if the PR changes the built binary in a way the line count does not see
    - see `_uncounted_build_paths`."""
    for f in changed_files:
        p = f.removeprefix("./")
        if _matches_digest_path(p, _UNCOUNTED_BUILD_PATHS) and not _matches_digest_path(
            p, _BUILD_DIGEST_EXCLUDES
        ):
            return True
    return False


def _is_small_pr(info):
    """True if the PR changes fewer than `SMALL_PR_CHANGED_LINES` lines of product
    code. False when the count is unknown (the pre-hook failed to fetch it), so an
    API hiccup runs the jobs instead of skipping them, and false outside the main
    PR workflow - see `SMALL_PR_WORKFLOW`."""
    if info.pr_number <= 0 or info.workflow_name != SMALL_PR_WORKFLOW:
        return False
    product_changed_lines = info.get_kv_data("product_changed_lines")
    if not isinstance(product_changed_lines, int):
        print("WARNING: product_changed_lines is not stored - do not skip stress tests, fuzzers and SQL suites")
        return False
    return product_changed_lines < SMALL_PR_CHANGED_LINES


_info_cache = None
_pipeline_note_labels = set()

# A revert pull request is recognized by its canonical title shape only - the
# one `git revert` and the GitHub "Revert" button produce, not a prose mention
# of a revert: `Revert "<title of the reverted change>"`. Reverting a revert
# nests the wrappers (`Revert "Revert "X""`), so the nesting depth gives the net
# effect: an odd depth is a real revert (it restores a state of `master` that CI
# has already validated), while an even depth re-applies the original change and
# must be tested as usual.
_REVERT_TITLE_RE = re.compile(r'^Revert "(.*)"$', re.DOTALL)

# The per-job reason shown on the report page.
REVERT_PR_SKIP_REASON = (
    f"Skipped: revert PR, CI is bypassed unless labeled '{Labels.CI_FORCE_ALL}'"
)
REVERT_PR_NOTE = (
    "Revert PR: all CI jobs except the style check are skipped so that the revert "
    "can be merged as quickly as possible. Add the "
    f"`{Labels.CI_FORCE_ALL}` label to run the full CI."
)


def revert_depth(title):
    """Number of nested `Revert "..."` wrappers in the pull request title; see
    `_REVERT_TITLE_RE`. An odd depth is a net revert, an even depth re-applies
    the reverted change."""
    depth = 0
    t = (title or "").strip()
    while True:
        m = _REVERT_TITLE_RE.fullmatch(t)
        if not m:
            break
        depth += 1
        t = m.group(1).strip()
    return depth


def is_net_revert_pr(title):
    """True if the pull request is, on balance, a revert: its title is an
    odd-depth stack of `Revert "..."` wrappers. A revert of a revert (even
    depth) re-applies the original change and is tested as usual."""
    return revert_depth(title) % 2 == 1


_revert_note_added = False


def _add_revert_note():
    """Explain the green light once on the workflow report page."""
    global _revert_note_added
    if _revert_note_added or _info_cache is None:
        return
    _revert_note_added = True
    _info_cache.add_workflow_note(REVERT_PR_NOTE)

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


def _coverage_family_decision(job_name, notes=True):
    """Filter decision for the coverage job family, or `None` when the job is
    not part of it (or the family rules have nothing to say about it).

    Factored out of `should_skip_job` so the review-thread gate can consult the
    skip half of it before limiting the pipeline: the gate may only shrink the
    PR surface, so it must not start the coverage family when a full run would
    have skipped it. Pass `notes=False` to probe the decision without emitting
    the workflow note for a decision that the caller may discard.
    """
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
            if notes:
                _add_pipeline_note(Labels.CI_NO_COVERAGE)
            return True, f"Skipped, labeled with '{Labels.CI_NO_COVERAGE}'"
        return True, "Skipped: no build-affecting changes; coverage would be identical to master"

    return None


def should_skip_job(job_name):
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()
        print(f"INFO: PR labels: {_info_cache.pr_labels}")

    if Labels.CI_FORCE_ALL in _info_cache.pr_labels:
        return False, ""

    # There is no way to prevent GitHub Actions from running the PR workflow on
    # release branches, so we skip all jobs here. The ReleaseCI workflow is used
    # for testing on release branches instead.
    if (
        Labels.RELEASE in _info_cache.pr_labels
        or Labels.RELEASE_LTS in _info_cache.pr_labels
    ):
        return True, "Skipped for release PR"

    if (
        _info_cache.pr_number > 0
        and job_name != JobNames.STYLE_CHECK
        and is_net_revert_pr(_info_cache.pr_title)
    ):
        _add_revert_note()
        return True, REVERT_PR_SKIP_REASON

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

    # `Build Toolchain (PGO, BOLT)` is opt-in: it occupies a large runner for
    # hours. This check stays ahead of the review-thread gate below, which is
    # allowed to shrink the pipeline but never to widen it - a limited run must
    # not start toolchain builds that a full run would have skipped.
    if (
        JobNames.BUILD_TOOLCHAIN in job_name
        and _info_cache.pr_number
        and Labels.CI_TOOLCHAIN not in _info_cache.pr_labels
    ):
        return True, f"Skipped, not labeled with '{Labels.CI_TOOLCHAIN}'"

    # While the PR has unresolved review threads, run only builds and the
    # preliminary checks - the code is expected to change again, so the full
    # test suite would be wasted (https://github.com/ClickHouse/ClickHouse/issues/114724).
    # The `Code Review` job keeps running so the AI review re-checks the new
    # code and resolves its own addressed threads, which re-triggers the full
    # suite via rerun_on_review_threads.yml. The kv data is stored by the
    # review_threads.py pre-hook; when it is missing (e.g. the GitHub API was
    # unavailable), nothing is skipped.
    unresolved_threads = _info_cache.get_kv_data(KV_UNRESOLVED_COUNT) or 0
    limited_by_review_threads = should_limit_pipeline(
        unresolved_threads, bool(_info_cache.get_kv_data(KV_OVERRIDE))
    )
    if (
        limited_by_review_threads
        and job_name not in REVIEW_THREADS_BUILD_JOBS
        and job_name not in PRELIMINARY_JOBS
        and job_name != JobNames.CODE_REVIEW
    ):
        if "unresolved-review-threads" not in _pipeline_note_labels:
            _pipeline_note_labels.add("unresolved-review-threads")
            _info_cache.add_workflow_note(
                f"The PR has {unresolved_threads} unresolved review thread(s): only "
                "builds and preliminary checks run, and merge is blocked. Resolve the "
                "threads before this run finishes to re-run the full test suite automatically; "
                "otherwise re-run CI manually, or add the "
                f"`{Labels.IGNORE_UNRESOLVED_THREADS}` label to bypass the gate."
            )
        return True, f"Skipped, {unresolved_threads} unresolved review thread(s)"

    # The limited pipeline is a fixed allowlist. Do not let other labels turn
    # it into a smaller pipeline: it must retain the builds, preliminary jobs,
    # and `Code Review` needed to validate the gate and trigger its re-run.
    if limited_by_review_threads:
        # The gate may only shrink the pipeline, never widen it: the allowlisted
        # coverage build is as expensive as any other build, so it must keep the
        # skip a full run would give it. Only a skip decision is honoured: a
        # neutral answer must not turn into anything wider than the allowlist.
        coverage_decision = _coverage_family_decision(job_name)
        if coverage_decision is not None and coverage_decision[0]:
            return coverage_decision
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

    # Skip the stress tests, fuzzers and SQL conformance suites on small PRs. Each
    # of these jobs takes up to 1-3 hours and they rarely catch anything a change
    # of this size introduces;
    # the targeted AST fuzzer still runs, and ClickGap fuzzes every merged PR on
    # master once more. Bypass: the `ci-force-all` label.
    if (
        _is_stress_or_fuzzer_job(job_name)
        and _is_small_pr(_info_cache)
        and not _has_uncounted_build_changes(changed_files)
        and not _has_stress_or_fuzzer_changes(changed_files)
    ):
        return (
            True,
            f"Skipped, fewer than {SMALL_PR_CHANGED_LINES} lines of product code changed "
            f"(add the '{Labels.CI_FORCE_ALL}' label to run)",
        )

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

    coverage_decision = _coverage_family_decision(job_name)
    if coverage_decision is not None:
        return coverage_decision

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

    # When the PR carries a functional or integration test, `new_tests_check.check`
    # decides the bug fix on the per-arch validators for those and returns before it
    # reads the unit validator, so a merge-base unit build has no verdict to contribute.
    if (
        _is_bugfix_pr()
        and job_name == JobNames.BUGFIX_VALIDATE_UT
        and (
            has_new_functional_tests(_info_cache.get_changed_files())
            or has_new_integration_tests(_info_cache.get_changed_files())
        )
    ):
        return True, "Skipped, the functional/integration bugfix validation owns the verdict"

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
    `amd_binary` build, the stateless flaky check, and the docs examples). Only
    the flaky check is conditional: it reruns the PR's new/changed stateless
    tests as a drift guard, so a PR that changes no stateless tests has nothing
    for it to do. Filter it out here, at config time, so such a PR does not
    schedule the runner, restore `CH_AMD_BINARY`, and enter the test container
    only to exit `SKIPPED`. This is the merge-queue counterpart to the `flaky`
    branch of `should_skip_job`, kept deliberately minimal so it cannot skip the
    build/style/fast-test/docs-examples jobs the queue always needs. The skip
    condition matches the in-job selection in `functional_tests.py` (both rely
    on `Targeting.get_changed_tests`), so the early exit and the config-time
    skip never disagree. `get_changed_tests` resolves data fixtures (a
    `.parquet`/`.tsv` under `tests/queries/0_stateless/`, even one nested in a
    subdirectory) back to the tests that consume them, so a fixture-only PR
    still reruns the affected test surface instead of being skipped here as
    "no changed tests".
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
