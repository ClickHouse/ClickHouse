import argparse
import json
import os
import random
import subprocess
import zlib
from pathlib import Path

from ci.jobs.scripts.bugfix_validation import bugfix_build_types, find_master_builds
from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.functional_tests.export_coverage import CoverageExporter
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"

# Substrings identifying a sanitizer build in a build type ("amd_asan_ubsan"),
# a job parameter string ("amd_asan_ubsan, distributed plan, parallel"), or a
# job name. Sanitizer builds get the tighter server memory cap (see the
# `set_memory_ratio` logic in `main`).
SANITIZERS = ("asan", "tsan", "msan", "ubsan")


def stateless_memory_limit(source):
    """Per-test cgroup memory limit (`clickhouse-test --memory-limit`) for a run
    identified by `source` (a build type, job-parameter string, or job name).

    Sanitizer clients are memory-heavy (~500 MiB RSS each), so a test running
    ~10 concurrent clients needs 10 GiB or the per-test cgroup OOM-kills them
    mid-test. Every sanitizer build gets 10 GiB, others 5 GiB. Match via
    `SANITIZERS`, not the literal `asan_ubsan` substring: the `tsan`/`msan` lanes
    (and the private `amd_ubsan` lane, an ASan+UBSan binary) lack that substring.
    """
    return 10 * 2**30 if any(san in source for san in SANITIZERS) else 5 * 2**30


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"
    DIAGNOSTICS = "diagnostics"
    CHECK_ERRORS = "check_errors"
    COLLECT_LOGS = "collect_logs"
    COLLECT_COVERAGE = "collect_coverage"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run ClickHouse functional tests (CI job)"
    )
    parser.add_argument(
        "--options",
        help="Comma-separated options. Examples: parallel|sequential|BATCH_NUM/BATCH_TOT|s3 storage|DBReplicated|azure|AsyncInsert|BugfixValidation|coverage",
        default="",
    )
    parser.add_argument(
        "--param",
        help="Optional start stage: install|start|test|check_errors|collect_logs|collect_coverage",
        default=None,
    )
    parser.add_argument(
        "--test",
        help="Optional. Space-separated test name patterns",
        default=[],
        nargs="+",
        action="extend",
    )
    parser.add_argument(
        "--count",
        help="Optional. Number of times to repeat each test",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--path",
        help="Optional. Path to a directory that contains the clickhouse binary",
        type=str,
        default="",
    )
    parser.add_argument(
        "--workers",
        help="Optional. Number of parallel workers for the test runner. Default: automatically computed from CPU count and job type",
        default=None,
    )
    parser.add_argument(
        "--debug",
        help="Optional. Open clickhouse-client console after test run",
        default=False,
        action="store_true",
    )
    return parser.parse_args()


def run_tests(
    batch_num: int,
    batch_total: int,
    tests: list[str] | None = None,
    extra_args="",
    rerun_count=1,
    random_order=False,
    global_time_limit=0,
    build_type=None,
):
    test_output_file = f"{temp_dir}/test_result.txt"
    if batch_num and batch_total:
        extra_args += (
            f" --run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
        )
    if "--no-shard" not in extra_args:
        extra_args += " --shard"
    if "--no-zookeeper" not in extra_args:
        extra_args += " --zookeeper"
    # Remove --report-logs-stats, it hides sanitizer errors in def reportLogStats(args): clickhouse_execute(args, "SYSTEM FLUSH LOGS")
    # Bugfix validation runs several builds per job, so size the limit from the
    # tested `build_type` over the job name (else a master-side memory-limit
    # failure would be inverted as a reproduced bug).
    limit_source = build_type if build_type is not None else Info().job_name
    memory_limit = stateless_memory_limit(limit_source)
    # Hand the time budget to `clickhouse-test` itself via `--global_time_limit`
    # so it stops *gracefully* between tests and exits with
    # `GLOBAL_TIME_LIMIT_EXIT_CODE` - reported as a benign "time limit reached"
    # rather than "Server died". The external `Shell.run` timeout below is kept
    # only as a larger safety net: it sends SIGTERM to the whole process group
    # and so should fire only for a genuinely frozen process, not pre-empt the
    # graceful stop.
    global_time_limit_arg = (
        f" --global_time_limit {global_time_limit}" if global_time_limit > 0 else ""
    )
    # `set -o pipefail` is required so that the pipeline's exit code reflects
    # `clickhouse-test`'s exit code rather than `tee`'s. Without it, a non-zero
    # exit from `clickhouse-test` is silently swallowed by `tee` returning 0.
    command = f"set -o pipefail; clickhouse-test --testname --check-zookeeper-session --hung-check --memory-limit {memory_limit} --trace \
                --capture-client-stacktrace --queries ./tests/queries --test-runs {rerun_count}{global_time_limit_arg} \
                {extra_args} \
                --queries ./tests/queries {('--order=random' if random_order else '')} -- {' '.join(tests) if tests else ''} | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    # Allow a margin over the graceful budget for the run to wind down before the
    # external hard kill engages. The last in-flight test can be deep inside its
    # own per-test alarm window when the deadline is reached: `clickhouse-test`
    # arms that alarm as `int(args.timeout * 1.1) + 60` (720s with the default
    # `--timeout 600`), after which it stops gracefully. The margin must exceed
    # that bound (plus the worker shutdown wind-down) so the external SIGTERM
    # fires only for a genuinely frozen process and never pre-empts the graceful
    # `GLOBAL_TIME_LIMIT_EXIT_CODE` stop (which would be reported as "Server died").
    outer_timeout = global_time_limit + 900 if global_time_limit > 0 else None
    return Shell.run(command, verbose=True, timeout=outer_timeout)


OPTIONS_TO_INSTALL_ARGUMENTS = {
    "old analyzer": "--analyzer",
    "WasmEdge": "--wasm-engine wasmedge",
    "s3 storage": "--s3-storage",
    "DBReplicated": "--db-replicated",
    "DatabaseOrdinary": "--db-ordinary",
    "wide parts enabled": "--wide-parts",
    "ParallelReplicas": "--parallel-rep",
    "distributed plan": "--distributed-plan",
    "azure": "--azure",
    "AsyncInsert": " --async-insert",
    "BugfixValidation": " --bugfix-validation",
    "db disk": "--remote-database-disk",
}

OPTIONS_TO_TEST_RUNNER_ARGUMENTS = {
    "s3 storage": "--s3-storage --no-stateful",
    "ParallelReplicas": "--no-zookeeper --no-shard --no-parallel-replicas",
    "AsyncInsert": " --no-async-insert",
    "DBReplicated": " --no-stateful --replicated-database",
    "azure": " --azure-blob-storage --no-random-settings --no-random-merge-tree-settings",  # azurite is slow, with randomization it can be super slow
    "parallel": "--no-sequential",
    "sequential": "--no-parallel",
    "flaky check": "--flaky-check",
    "targeted": "--flaky-check --no-self-parallel",
}

# Job option that replaces the full test suite with the subset of tests selected
# for the change under test (see `Targeting`): the tests the pull request changes,
# the tests that already failed in this pull request, and the tests that cover the
# changed lines according to the coverage database.
#
# Unlike the `targeted` check - which reruns that same selection many times to hunt
# for flakiness - a `selected tests` run is an ordinary functional test run with a
# shorter list of tests. The pull request workflow uses it for the sanitizer
# flavors, where the builds with sanitizers are still exercised by the stress tests
# and the whole suite still runs in the master workflow.
SELECTED_TESTS_OPTION = "selected tests"


def filter_selected_tests_by_flavor(tests, keep_sequential):
    """Keep the selected tests that a `parallel`/`sequential` job flavor runs.

    `--no-sequential`/`--no-parallel` split the suite into two independently
    scheduled job flavors, so a test tagged `no-parallel`/`sequential` never runs
    under the `parallel` flavor and vice versa. Dropping the tests the flavor
    would not select anyway keeps the reported selection honest and lets the job
    skip early when nothing is left for it.

    Tests that cannot be resolved to a file in `tests/queries/0_stateless` (a
    stateful test, or one removed or renamed since the selection data was
    collected) are kept: `clickhouse-test` filters them out on its own.
    """
    res = []
    for test in tests:
        source_file = Targeting.functional_test_source_file(test)
        if source_file is None or (
            Targeting.is_sequential_functional_test(source_file) == keep_sequential
        ):
            res.append(test)
    return res


def allow_oversubscription(options, test_options, is_flaky_check, is_targeted_check):
    """Whether this job may run more test workers than the runner has cores.

    A plain (non-sanitizer) binary job runs the whole suite, where every worker
    picks a different test and most tests are light, so oversubscribing the
    runner shortens the job without making any single test noticeably slower.

    A flaky/targeted check is the opposite case: every worker runs the *same*
    changed test, so `--jobs N` multiplies that one test's resource use by `N`.
    For a heavy test (its own `max_threads`, large inserts, merges) that turns
    into self-contention, and the flaky check fails a test whose wall-clock time
    exceeds `TEST_MAX_RUN_TIME_IN_SECONDS` - so oversubscription decides the
    verdict. Keep those checks at the default concurrency, where per-iteration
    times are comparable to the other flaky-check jobs and the "too long"
    verdict reflects the test rather than how many copies of it were co-scheduled.
    """
    if is_flaky_check or is_targeted_check:
        return False
    return "binary" in options and len(test_options) < 3


def invert_bugfix_validation_status(test_result: Result) -> bool:
    """Invert FAIL/OK in `test_result.results` for bugfix validation.

    On master HEAD a regression test for the bug is expected to FAIL; the
    inverter flips that to OK so the job reads as "bug reproduced".

    Returns True iff the bug did not reproduce on this arch (no-repro). In
    that case the caller must propagate `SKIPPED` to the top-level result so
    the per-arch job exits 0 without being counted as a validation by the
    `new_tests_check.py` post-hook (which uses strict `is_success`). This is
    the per-arch contract: a regression test that passes on master HEAD on
    one arch (e.g. an x86-only fix validated on aarch64 where the bug never
    existed) must not block the PR - another arch can still validate it.

    When the run ended in `Result.Status.ERROR` (runner did not finish,
    e.g. server crash without proper exit code, Python exception,
    infrastructure outage) the per-test list is empty or partial and the
    pre-inversion `ERROR` already tells the truth. Preserve it instead of
    overwriting with a validation verdict - an infra-induced failure is
    never counted as a validation. See #105789. A server crash caused by
    the regression test itself does NOT hit this guard: in bugfix
    validation `FTResultsProcessor` keeps the aborted-run culprit as
    `FAIL` (instead of demoting it to `ERROR` as in normal runs), so the
    crash is counted as a reproduction below.

    The aggregate check is not enough: `FTResultsProcessor` can leave the
    top-level status `OK` while still emitting `ERROR` per-test rows
    (parser failure for a single test, unexpected runner termination
    propagated as a row). A mix of `FAIL` + `ERROR` rows would otherwise
    set `has_failure = True` from the `FAIL` rows and call `set_success`,
    masking the `ERROR` and flipping the job to green. Treat any per-row
    `ERROR` the same as an aggregate `ERROR`. Mirrors the `has_error`
    dominant guard in `integration_test_job.py`.

    Rows produced by `check_fatal_messages_in_logs` (labelled `LOG_CHECK`:
    "Lost s3 keys", "OOM in dmesg", "Exception in test runner", etc.) are
    server-log / runner health checks, not test cases. A LOG_CHECK *failure*
    on the validated binary is itself evidence the bug reproduced (a crash /
    sanitizer assert / lost key triggered by the regression test), so it is
    flipped like a test failure. But a *clean* LOG_CHECK must stay `OK`: the
    absence of a fatal is not "failed to reproduce", and flipping it to
    `FAIL` is what produced the spurious xfail rows.
    """
    if test_result.status == Result.Status.ERROR or any(
        r.status == Result.Status.ERROR for r in test_result.results
    ):
        for r in test_result.results:
            r.set_label(Result.Label.XFAIL)
        print(
            "Bugfix validation inconclusive: the test runner did not "
            "finish; preserving ERROR rather than reporting a validation "
            "verdict."
        )
        return False

    has_failure = False
    for r in test_result.results:
        if r.status == Result.Status.OK and r.has_label(Result.Label.LOG_CHECK):
            # A clean health check is not a test that "failed to reproduce";
            # leave it untouched so it does not become a spurious failure.
            continue
        r.set_label(Result.Label.XFAIL)
        if r.status == Result.Status.FAIL:
            # A failing test, or a fatal / sanitizer assert / lost key on the
            # validated binary, both mean the bug was reproduced.
            r.status = Result.Status.OK
            has_failure = True
        elif r.status == Result.Status.OK:
            r.status = Result.Status.FAIL
    if not has_failure:
        # The bug did not reproduce on this arch - every regression test case
        # still passed on master HEAD here. Report SKIPPED so the per-arch job
        # exits 0 (`Result.is_ok` includes SKIPPED) and the GitHub status is
        # not red. The post-hook in `new_tests_check.py` uses `is_success`
        # (strict - `OK`/`XFAIL` only), so a SKIPPED per-arch job does NOT
        # count as a validation, preserving the contract that at least one
        # arch must reproduce the bug. The caller propagates this SKIPPED to
        # the top-level `R` (see `bugfix_validation_no_repro`).
        print("Bug does not reproduce on this arch - bugfix validation N/A")
        test_result.set_status(Result.Status.SKIPPED).set_info(
            "Bug does not reproduce on this arch - bugfix validation N/A"
        )
        return True
    test_result.set_success()
    return False


def attach_post_verdict_artifacts(
    test_result: Result, artifacts: list, preserve_verdict: bool
) -> None:
    """Attach artifact-collection rows without letting them decide the status.

    `extend_sub_results` re-derives the parent status from its children, so rows
    appended once the run is over overwrite whatever the parent said. With
    `preserve_verdict` the parent's own status wins instead, while the rows stay
    visible in the report: on a bugfix-validation job that status is the
    validation verdict, which `new_tests_check.py` reads with strict
    `is_success` to decide whether any arch validated the bug.

    The captured status is restored rather than forced to `OK`, so every verdict
    the inverter can set survives: reproduction `OK`, no-repro `SKIPPED`,
    inconclusive `ERROR`.
    """
    verdict = test_result.status
    test_result.extend_sub_results(artifacts)
    if preserve_verdict:
        test_result.set_status(verdict)


def reconcile_bugfix_crash_repro(result: Result, fatals: list) -> bool:
    """Fold a build type's fatal-log rows into its per-test result for bugfix
    validation, treating a master-HEAD server crash as a reproduction.

    A sanitizer assert / fatal in the master-HEAD server log (the
    `BLOCKER`-labelled rows of `check_fatal_messages_in_logs`) means the server
    crashed while running only the changed tests. With
    `-fno-sanitize-recover=all` a reproduced UBSan bug kills the server
    outright, which aborts the runner (`StopTesting`, exit code 2) and poisons
    the per-test rows with `ERROR` - so a crash-manifesting bugfix could never
    validate. That abort IS the bug reproducing, not an infra failure:
    downgrade the runner-level `ERROR` and the per-row `ERROR`s it caused to
    `FAIL`, which the inverter then flips into a successful reproduction. A run
    that ends in `ERROR` without a fatal in the server logs (genuine infra
    failure) is preserved as inconclusive (#105789). OOM kills are excluded:
    the dmesg OOM row carries no `BLOCKER` label.

    Capture the runner-level `ERROR` before `extend_sub_results`, which
    recomputes the aggregate status from child rows only and would otherwise
    erase a runner-level `ERROR` set by `FTResultsProcessor` (e.g.
    `not s.success_finish`) when the parsed rows are all `OK`/`FAIL`; restore
    it so `invert_bugfix_validation_status` still sees the error and does not
    flip a harness-level termination into green.

    Returns whether a crash reproduction was detected.
    """
    runner_level_error = result.is_error()
    crash_repro = any(
        r.status == Result.Status.FAIL and r.has_label(Result.Label.BLOCKER)
        for r in fatals
    )
    if crash_repro:
        print(
            "The master-HEAD server crashed with a sanitizer/fatal failure "
            "while running the changed tests - treating the resulting runner "
            "abort / per-test errors as the bug reproducing."
        )
        for r in result.results:
            if r.status == Result.Status.ERROR:
                r.status = Result.Status.FAIL
    result.extend_sub_results(fatals)
    if runner_level_error and not crash_repro:
        result.status = Result.Status.ERROR
    return crash_repro


def main():
    args = parse_args()
    test_options = [to.strip() for to in args.options.split(",")]
    batch_num, total_batches = 0, 0
    config_installs_args = ""
    is_flaky_check = False
    is_targeted_check = False
    is_selected_tests_run = False
    is_bugfix_validation = False
    is_s3_storage = False
    is_azure_storage = False
    is_database_replicated = False
    is_shared_catalog = False
    is_encrypted_storage = random.choice([True, False])
    is_parallel_replicas = False
    is_llvm_coverage = False
    is_excluded_from_llvm = False
    is_per_test_coverage = False
    runner_options = ""
    # optimal value for most of the jobs
    nproc = int(Utils.cpu_count() * 0.6)
    info = Info()

    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif to in OPTIONS_TO_INSTALL_ARGUMENTS:
            pass
        elif to.startswith("amd_") or to.startswith("arm_"):
            pass
        elif to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            pass
        elif to == "per_test_coverage":
            pass
        elif to == SELECTED_TESTS_OPTION:
            pass
        else:
            assert False, f"Unknown option [{to}]"

        if to in OPTIONS_TO_INSTALL_ARGUMENTS:
            print(f"NOTE: Enabled config option [{OPTIONS_TO_INSTALL_ARGUMENTS[to]}]")
            config_installs_args += f" {OPTIONS_TO_INSTALL_ARGUMENTS[to]}"

        if to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            if to in ("parallel", "sequential") and args.test:
                # skip setting up parallel/sequential if specific tests are provided
                continue
            else:
                runner_options += f" {OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}"
                print(
                    f"NOTE: Enabled test runner option [{OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}]"
                )

        if to == SELECTED_TESTS_OPTION:
            is_selected_tests_run = True
        elif "targeted" in to:
            is_targeted_check = True
        elif "flaky" in to:
            is_flaky_check = True
        elif "BugfixValidation" in to:
            is_bugfix_validation = True
        elif to.startswith("amd_") and "coverage" in to:
            is_llvm_coverage = True
        if "excluded_from_llvm" in to:
            is_excluded_from_llvm = True
        if "per_test_coverage" in to:
            is_per_test_coverage = True
        if "s3 storage" in to:
            is_s3_storage = True
        if "azure" in to:
            is_azure_storage = True
        if "DBReplicated" in to:
            is_database_replicated = True
        if "SharedCatalog" in to:
            is_shared_catalog = True
        if "ParallelReplicas" in to:
            is_parallel_replicas = True

    # The xfail inversion (and therefore the "a crash on master HEAD is a
    # reproduction" reading of a server death) only applies when the PR is
    # labelled as a bugfix; an unlabelled run of this job executes the sanity
    # test instead, where a crash is an ordinary infra failure and must keep
    # its ERROR classification in `FTResultsProcessor`.
    is_labeled_bugfix_validation = is_bugfix_validation and (
        Labels.PR_BUGFIX in info.pr_labels
        or Labels.PR_CRITICAL_BUGFIX in info.pr_labels
    )

    # If this PR only touches test files (no production/config code changed),
    # this job only needs to run if one of the changed tests would even be
    # selected here - and, when the job is also hash-batched (N/M), only the
    # batch(es) containing a changed test need to run. Other jobs/batches
    # would produce results identical to master and can be skipped. Note:
    # "parallel"/"sequential" job flavors need no batch number of their own
    # (e.g. "amd_debug, parallel") - the flavor-applicability check below must
    # not be gated on batching being active.
    # A `selected tests` run is excluded too: it picks its own test list and
    # applies the same flavor-applicability check to it below.
    if (
        not is_flaky_check
        and not is_targeted_check
        and not is_selected_tests_run
        and not is_bugfix_validation
        and not is_llvm_coverage
        and not is_excluded_from_llvm
        and not is_per_test_coverage
        and not args.test
    ):
        changed_files = info.get_changed_files()
        if changed_files and all(
            Targeting.is_functional_test_file(f)
            or Targeting.is_integration_test_file(f)
            or Targeting.is_ci_job_script(f)
            for f in changed_files
        ):
            changed_functional_files = [
                f for f in changed_files if Targeting.is_functional_test_file(f)
            ]
            if not changed_functional_files:
                Result.create_from(
                    status=Result.Status.SKIPPED,
                    info="Only non-functional test files changed in this PR - nothing for this job to run",
                ).complete_job()
            # "parallel"/"sequential" is a second, independent sharding dimension:
            # each is hash-batched separately (--no-sequential/--no-parallel), and
            # a test tagged no-parallel/sequential never runs under the "parallel"
            # flavor (and vice versa) regardless of batch. Restrict to the changed
            # tests that this job flavor would even select before checking batches.
            is_parallel_flavor = "parallel" in test_options
            is_sequential_flavor = "sequential" in test_options
            hash_batch_files = []
            for f in changed_functional_files:
                hash_batch_file = Targeting.functional_test_hash_batch_file(f)
                if hash_batch_file is None:
                    # Could not resolve to a concrete test source file (e.g. an
                    # orphan data file) - be conservative and run the batch normally.
                    hash_batch_files = None
                    break
                is_sequential_test = Targeting.is_sequential_functional_test(
                    hash_batch_file
                )
                if is_parallel_flavor and is_sequential_test:
                    continue
                if is_sequential_flavor and not is_sequential_test:
                    continue
                hash_batch_files.append(hash_batch_file)
            if hash_batch_files is not None and not hash_batch_files:
                Result.create_from(
                    status=Result.Status.SKIPPED,
                    info="Only test files changed in this PR and none of the changed tests apply to this job's parallel/sequential flavor",
                ).complete_job()
            if (
                hash_batch_files is not None
                and batch_num
                and total_batches > 1
                and not any(
                    zlib.crc32(f.encode("utf-8")) % total_batches == batch_num - 1
                    for f in hash_batch_files
                )
            ):
                Result.create_from(
                    status=Result.Status.SKIPPED,
                    info="Only test files changed in this PR and none of the changed tests fall into this batch",
                ).complete_job()

    if is_llvm_coverage:
        # Pin random-by-default fault injection seeds server-side (in the default
        # profile) so coverage is deterministic, instead of injecting them as
        # per-query client settings (which broke tests that switch to readonly
        # mode mid-session). See tests/config/users.d/coverage_fault_injection_seeds.xml.
        config_installs_args += " --llvm-coverage"

    if is_shared_catalog or is_parallel_replicas:
        pass
    else:
        if allow_oversubscription(args.options, test_options, is_flaky_check, is_targeted_check):
            # Plain binary job runs fast; allow higher concurrency
            nproc = int(Utils.cpu_count() * 1.2)
        elif is_database_replicated:
            nproc = int(Utils.cpu_count() * 0.4)
        elif "msan" in args.options:
            # MSan is slow
            nproc = int(Utils.cpu_count() * 0.4)
        elif is_azure_storage:
            # azure FT runs only under ASan; concurrent heavy queries overrun the
            # shared server memory cap, so the OvercommitTracker kills queries across
            # all co-scheduled tests. Lower concurrency to keep peak total RSS under it.
            nproc = int(Utils.cpu_count() * 0.4)
        elif is_per_test_coverage:
            cidb_cluster = CIDBCluster()
            if not info.is_local_run:
                assert cidb_cluster.is_ready()
            nproc = 1
        else:
            pass

    workers = None
    if args.workers:
        print(f"Workers count set from --workers: {args.workers}")
        workers = args.workers
    elif is_flaky_check:
        workers = max(1, nproc - 1)
        print(f"Workers count set to nproc-1 for flaky check: {workers}")
    else:
        workers = max(1, nproc)
        print(f"Workers count set to optimal value: {workers}")

    runner_options += f" --jobs {workers}"

    if is_flaky_check or is_targeted_check:
        # Stop after 5 total failures across all parallel workers (fast feedback on broken PRs).
        runner_options += " --max-failures 5"

    if is_excluded_from_llvm:
        # Run only tests that are normally disabled under LLVM coverage
        runner_options += " --excluded-from-llvm"
    elif is_llvm_coverage:
        # Randomization makes coverage non-deterministic, long tests are slow to collect coverage
        runner_options += " --llvm-coverage"
        # %c enables continuous mode: counters are memory-mapped into the file,
        # so the profile is valid at every instant instead of being written only
        # by an interruptible exit-time dump (see integration_test_job.py).
        os.environ["LLVM_PROFILE_FILE"] = f"ft-{batch_num}-%c%2m.profraw"
        if is_per_test_coverage:
            runner_options += " --collect-per-test-coverage"
        else:
            runner_options += " --no-random-settings --no-random-merge-tree-settings  --no-long"

    diagnostics_dir = f"{temp_dir}/random-settings-diagnostics"
    runner_options += f" --random-settings-diagnostics-dir {diagnostics_dir}"

    # `--repeat-newly-modified-tests` ranks the tests it is given by name and
    # repeats the highest-numbered ones, which identifies the newly added tests
    # only when the runner is given the whole suite. For a `selected tests` run
    # the top of that ranking is just the newest test of the selection, so the
    # option would multiply the run time without repeating anything new.
    if (
        not is_flaky_check
        and not is_targeted_check
        and not is_selected_tests_run
        and not is_llvm_coverage
        and not is_bugfix_validation
        and not args.test
        and "--no-random-settings" not in runner_options
    ):
        runner_options += " --repeat-newly-modified-tests"

    rerun_count = 1
    if args.count:
        print(f"Rerun count set from --count: {args.count}")
        rerun_count = args.count
    elif is_flaky_check and info.is_merge_queue_event:
        # The merge-queue flaky check is a drift guard, not a full flakiness
        # hunt: the PR CI already ran the full flaky check, and this rerun only
        # needs to catch new tests broken by the current `master` state (e.g. a
        # setting randomization added to `tests/clickhouse-test` after the PR's
        # last CI run). A randomized setting drawn with probability 0.4 per run
        # escapes 20 iterations with probability 0.6^20 ~= 4e-5, so a reduced
        # count keeps merge-queue latency bounded without losing the signal.
        rerun_count = 20
    elif is_flaky_check:
        # Large repeat count so the 45-min global_time_limit is the effective stopping
        # condition, not the repeat count.  Tests run in parallel (--jobs N) with fresh
        # random settings per TestCase; --max-failures 5 stops early on broken PRs.
        rerun_count = 50
    elif is_targeted_check:
        rerun_count = 50

    if is_flaky_check:
        # Run no-parallel and no-flaky-check tests sequentially with fewer iterations.
        # Derived from rerun_count so the ratio stays stable as policy evolves.
        runner_options += f" --sequential-test-runs {rerun_count // 2}"

    if (is_azure_storage or is_s3_storage) and is_encrypted_storage:
        config_installs_args += " --encrypted-storage"
        runner_options += " --encrypted-storage"

    if is_bugfix_validation:
        os.environ["GLOBAL_TAGS"] = "no-random-settings"
        ch_path = temp_dir
        # Download the master-HEAD binaries matching this job's runner arch:
        # the aarch64 job runs on an ARM runner and must use the ARM builds.
        build_types = bugfix_build_types(info.job_name)
        bt_paths = {bt: f"{temp_dir}/clickhouse_{bt}" for bt in build_types}
        # In local runs, only reuse existing binaries; probing master commits in S3
        # depends on `master_commits` workflow data populated by CI workflow hooks
        # and is not available locally.
        if info.is_local_run:
            missing = [str(p) for p in bt_paths.values() if not Path(p).is_file()]
            assert not missing, (
                "Local bugfix validation requires all build-type binaries to be "
                f"present under {temp_dir}; missing: {missing}"
            )
            build_urls = None
        else:
            build_urls = find_master_builds(build_types)
            assert build_urls, "Could not find master builds in S3"
        if build_urls:
            for bt, url in build_urls.items():
                bt_path = bt_paths[bt]
                if not info.is_local_run or not Path(bt_path).is_file():
                    Shell.run(
                        f"wget -nv -O {bt_path} {url}", verbose=True, strict=True
                    )
                    Shell.run(f"chmod +x {bt_path}", verbose=True)
        Shell.run(
            f"cp {temp_dir}/clickhouse_{build_types[0]} {temp_dir}/clickhouse",
            verbose=True,
            strict=True,
        )
    elif args.path:
        assert Path(args.path).is_dir(), f"Path [{args.path}] is not a directory"
        ch_path = str(Path(args.path).absolute())
    else:
        paths_to_check = [
            f"{temp_dir}/clickhouse",  # it's set for CI runs, but we need to check it
            f"{Utils.cwd()}/build/programs/clickhouse",
            f"{Utils.cwd()}/clickhouse",
        ]
        for path in paths_to_check:
            if Path(path).is_file():
                ch_path = str(Path(path).parent.absolute())
                break
        else:
            raise FileNotFoundError(
                "ClickHouse binary not found in any of the paths: "
                + ", ".join(paths_to_check)
                + ". You can also specify path to binary via --path argument"
            )

    Shell.check(f"chmod +x {ch_path}/clickhouse")

    stop_watch = Utils.Stopwatch()

    res = True
    results = []
    debug_files = []

    stages = list(JobStages)
    if not is_per_test_coverage:
        stages.remove(JobStages.COLLECT_COVERAGE)
    else:
        stages.remove(JobStages.COLLECT_LOGS)
    if is_per_test_coverage or info.is_local_run or is_bugfix_validation:
        # For bugfix validation, fatal message checking is done per-build-type
        # inside the bugfix validation loop below, so skip the outer stage.
        stages.remove(JobStages.CHECK_ERRORS)
    if info.is_local_run:
        if JobStages.COLLECT_LOGS in stages:
            stages.remove(JobStages.COLLECT_LOGS)
        if JobStages.COLLECT_COVERAGE in stages:
            stages.remove(JobStages.COLLECT_COVERAGE)
    if (
        is_flaky_check
        or is_per_test_coverage
        or is_bugfix_validation
        or is_targeted_check
        or info.is_local_run
    ):
        stages.remove(JobStages.DIAGNOSTICS)

    tests = args.test

    # for local run check if stateful tests are present to skip prepare_stateful_data and start faster if not
    has_stateful_tests = True
    if tests and info.is_local_run:
        from glob import glob

        has_stateful = False
        for test_pattern in tests:
            test_pattern_clean = test_pattern.strip()
            matching_files = glob(
                f"tests/queries/**/*{test_pattern_clean}*.sql", recursive=True
            )
            matching_files += glob(
                f"tests/queries/**/*{test_pattern_clean}*.sh", recursive=True
            )
            for test_file in matching_files:
                try:
                    with open(test_file, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if "stateful" in content.lower():
                            has_stateful = True
                            break
                except Exception:
                    pass
            if has_stateful:
                break
        if not has_stateful:
            has_stateful_tests = False

    targeter = Targeting(info=info)
    if is_flaky_check or is_bugfix_validation:
        if info.is_local_run:
            assert (
                args.test
            ), "For running flaky or bugfix_validation check locally, test case name must be provided via --test"
        else:
            if is_bugfix_validation and not is_labeled_bugfix_validation:
                # Not a bugfix PR - run a simple sanity test
                tests = ["00001_select_1"]
            elif is_flaky_check:
                # Flaky check runs only changed/new test files in this PR.
                # Previously failed and coverage-relevant tests are handled
                # by the separate targeted check jobs.
                tests = targeter.get_changed_tests()
                tests_str = ", ".join(tests) if tests else "(none)"
                print(f"[flaky-check] Changed/new tests ({len(tests)}): {tests_str}")
            else:
                tests = targeter.get_changed_tests()

        if tests:
            print(f"Test list: [{tests}]")
        else:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED, info="No tests to run"
            ).complete_job()

    if is_targeted_check:
        assert not args.test, "--test not supposed to be used for targeted check"
        tests, results_with_info = targeter.get_all_relevant_tests_with_info()
        results.append(results_with_info)

        if not tests:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No failed tests found from previous runs",
            ).complete_job()

    if is_selected_tests_run:
        assert not args.test, "--test not supposed to be used for a selected tests run"
        try:
            tests, results_with_info = targeter.get_all_relevant_tests_with_info(
                include_changed_tests=True
            )
            results.append(results_with_info)
        except Exception as e:
            # Selecting the tests needs the pull request diff and the coverage
            # database. Do not silently run a weaker, unbatched version of the
            # former sanitizer configuration: its original shards and repeated
            # newly modified tests cannot be reconstructed from this job. Fail
            # the check so the selection service problem is visible and retried.
            Result.create_from(
                status=Result.Status.ERROR,
                info=f"Failed to select tests: {e}",
            ).complete_job()

    if is_selected_tests_run:
        if "parallel" in test_options or "sequential" in test_options:
            tests = filter_selected_tests_by_flavor(
                tests, keep_sequential="sequential" in test_options
            )
        print(f"[selected tests] {len(tests)} tests to run: {tests}")

        if not tests:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No tests selected for this change",
            ).complete_job()

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    Utils.add_to_PATH(f"{ch_path}:tests")
    CH = ClickHouseProc(
        is_db_replicated=is_database_replicated,
        is_shared_catalog=is_shared_catalog,
        is_per_test_coverage=is_per_test_coverage,
    )
    # `run_tests` runs `clickhouse-test` without changing directory, so clients
    # it spawns inherit the repository root and dump their cores there.
    # Declaring it lets `prepare_logs` retain the core of a client that died on a
    # fatal signal; without it such a crash leaves no core and no stack anywhere.
    CH.client_core_path = Utils.cwd()

    job_info = ""

    if res and JobStages.INSTALL_CLICKHOUSE in stages:

        def configure_log_export():
            if not info.is_local_run:
                print("prepare log export config")
                return CH.create_log_export_config()
            else:
                print("skip log export config for local run")

        commands = [
            "rm -rf /etc/clickhouse-client/* /etc/clickhouse-server/* /etc/clickhouse-server1/* /etc/clickhouse-server2/*",
            # google *.proto files
            "mkdir -p /usr/share/clickhouse/ && ln -sf /usr/local/include /usr/share/clickhouse/protos",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-compressor",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-disks",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-obfuscator",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-format",
            f"ln -sf {ch_path}/clickhouse {ch_path}/ch",
            f"ln -sf /usr/bin/clickhouse-odbc-bridge {ch_path}/clickhouse-odbc-bridge",
            "cp programs/server/config.xml programs/server/users.xml /etc/clickhouse-server/",
            f"./tests/config/install.sh /etc/clickhouse-server /etc/clickhouse-client {config_installs_args}",
            "clickhouse-server --version",
            f"sed -i 's|>/test/chroot|>{temp_dir}/chroot|' /etc/clickhouse-server**/config.d/*.xml",
            CH.set_random_timezone,
        ]

        # Sanitizer builds run under heavy memory pressure: besides the server's
        # own ASan overhead, the parallel runner keeps dozens of ASan-instrumented
        # `clickhouse-client` processes alive at once (~0.4 GiB each, ~17 GiB in
        # total on a 60 GiB host). With the default 0.9
        # `max_server_memory_usage_to_ram_ratio` the server is allowed to grow to
        # ~0.9 of RAM on its own, so it can drive the host into a global OOM and be
        # killed at an RSS well below its own memory limit - before any query hits
        # `MEMORY_LIMIT_EXCEEDED` - which surfaces as a "Server died" failure.
        # Cap the server low enough that the clients' footprint still fits, so the
        # server kills a runaway query gracefully instead of being OOM-killed by
        # the host. This applies to every sanitizer run, not just the flaky check
        # (which previously used 0.8, sufficient there only because it runs a small
        # test subset at reduced concurrency). 0.7 stays comfortably above the
        # largest legitimate single-query need (the ~25 GiB stateful-load INSERT).
        # The heaviest configs also cut test concurrency (see the `nproc` block
        # above, e.g. azure) so that the *aggregate* RSS of concurrent queries
        # stays under this cap too; the memory-heavy distributed-plan parallel
        # job instead runs on a larger 128 GiB runner (see `job_configs.py`),
        # where the cap sits well above its aggregate RSS at full concurrency.
        #
        # The cap must follow the binary actually being launched, not the job
        # option string: bugfix validation passes only `BugfixValidation` in
        # `--options` yet starts with the `build_types[0]` master-HEAD binary
        # (always `*_asan_ubsan`), and its validation loop later swaps to the
        # other build types, re-deriving the cap on every swap (sanitizer ->
        # 0.7, debug -> server default; see the TEST stage below).
        memory_cap_source = build_types[0] if is_bugfix_validation else args.options
        if any(san in memory_cap_source for san in SANITIZERS):
            commands.append(lambda: CH.set_memory_ratio(0.7))

        if is_flaky_check:
            commands.append(CH.enable_thread_fuzzer_config)

        os.environ["MALLOC_CONF"] = (
            f"prof_prefix:{temp_dir}/jemalloc_profiles/clickhouse.jemalloc"
        )

        if not is_llvm_coverage:
            commands.append(configure_log_export)

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found under [{ch_path}]"

    if res and JobStages.START in stages:
        step_name = "Start ClickHouse Server"
        print(step_name)

        # Reasons recorded by the setup closure that must reach the persisted
        # Result.info (CIDB test_context_raw) even when setup ultimately
        # succeeds - e.g. a non-fatal seaweedfs log-table/restart failure that would
        # otherwise be invisible in CIDB (only visible as a report-page warning).
        setup_notes = []

        def start():
            # `from_commands_run` captures this closure's stdout into the step
            # Result.info (hence CIDB test_context_raw) only when it returns a
            # failing value. Print a concise "SETUP FAILURE: <sub-step>" marker
            # at each failure point so the opaque "Start ClickHouse Server"
            # umbrella can be split into measurable sub-causes (seaweedfs /
            # wait_ready / kafka / stateful) instead of one bucket.
            if not (CH.start_seaweedfs(test_type="stateless") and CH.start_azurite()):
                print("SETUP FAILURE: seaweedfs/azurite did not start")
                return False
            if not CH.start():
                print("SETUP FAILURE: clickhouse-server process did not start")
                return False
            if not CH.wait_ready():
                # wait_ready() already tails the server err log to stdout on
                # timeout; the marker just names the sub-step for triage.
                print("SETUP FAILURE: clickhouse-server not ready (wait_ready)")
                return False

            if not CH.start_kafka():
                info.add_workflow_warning("Failed to start Kafka")
                print("SETUP FAILURE: kafka did not start")
                # Fail fast on infra setup errors so we don't burn time
                # triaging Kafka/Avro test failures caused by a broken setup.
                return False

            if not Info().is_local_run:
                if not CH.start_log_exports(stop_watch.start_time):
                    info.add_workflow_warning("Failed to start log export")
                    print("Failed to start log export")

            res = True
            if has_stateful_tests:
                if not CH.prepare_stateful_data(
                    with_s3_storage=is_s3_storage,
                    is_db_replicated=is_database_replicated,
                    # `args.options` (e.g. "amd_asan_ubsan, distributed plan, parallel")
                    # already carries the sanitizer name in the same format
                    # `prepare_stateful_data`'s `is_sanitizer` check expects, so the
                    # normal (non-bugfix-validation) path can reuse it directly - it
                    # must not stay `None` here, since every sanitizer stateless run
                    # now sets the tighter 0.7 memory ratio (see below) and needs the
                    # reduced `MAX_INSERT_THREADS` to fit under it.
                    build_type=(
                        build_types[0] if is_bugfix_validation else args.options
                    ),
                ):
                    print(
                        "SETUP FAILURE: "
                        + (
                            CH.stateful_setup_error
                            or "prepare_stateful_data failed"
                        )
                    )
                    res = False
                elif not CH.insert_system_zookeeper_config():
                    print("SETUP FAILURE: insert_system_zookeeper_config failed")
                    res = False
            if res:
                print("stateful data prepared")
            return res

        results.append(
            Result.from_commands_run(
                name=step_name,
                command=start,
            )
        )
        # Surface non-fatal setup notes (e.g. seaweedfs) into the persisted Result
        # so they are queryable in CIDB test_context_raw even on the success path.
        for note in setup_notes:
            results[-1].set_info(note)
        res = results[-1].is_ok()

    test_result = None
    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)

        ft_res_processor = FTResultsProcessor(wd=temp_dir)

        global_time_limit = 0
        if is_flaky_check:
            # The merge-queue run gets a tighter budget: it delays merges
            # directly, and its reduced rerun_count needs less time anyway.
            FLAKY_CHECK_TIME_LIMIT = 20 * 60 if info.is_merge_queue_event else 45 * 60
            # Floor the budget at a small positive value: `run_tests` interprets
            # `global_time_limit == 0` as "pass no `--global_time_limit`", i.e. no
            # cap at all. If setup already consumed the whole budget (more likely
            # under the tighter merge-queue limit) a `0` here would turn the run
            # unbounded, defeating the very latency bound it is meant to enforce.
            # A minimal explicit limit keeps the run bounded while still doing one
            # quick pass. Mirrors the targeted-check floor below.
            global_time_limit = max(
                FLAKY_CHECK_TIME_LIMIT - int(stop_watch.duration), 60
            )
            print(
                f"Flaky-check time limit: {FLAKY_CHECK_TIME_LIMIT}s"
                f" (elapsed so far: {int(stop_watch.duration)}s,"
                f" remaining: {global_time_limit}s)"
            )

            runner_exit_code = run_tests(
                batch_num=0,
                batch_total=0,
                tests=list(tests) if tests else tests,
                extra_args=runner_options,
                random_order=True,
                rerun_count=rerun_count,
                global_time_limit=global_time_limit,
            )

        elif is_targeted_check:
            TARGETED_CHECK_TIME_LIMIT = 50 * 60  # 50 min
            global_time_limit = max(
                TARGETED_CHECK_TIME_LIMIT - int(stop_watch.duration), 60
            )
            print(
                f"Targeted-check time limit: {TARGETED_CHECK_TIME_LIMIT}s"
                f" (elapsed so far: {int(stop_watch.duration)}s,"
                f" remaining: {global_time_limit}s)"
            )

            runner_exit_code = run_tests(
                batch_num=0,
                batch_total=0,
                tests=list(tests) if tests else tests,
                extra_args=runner_options,
                random_order=True,
                rerun_count=rerun_count,
                global_time_limit=global_time_limit,
            )

        else:
            runner_exit_code = run_tests(
                batch_num=batch_num,
                batch_total=total_batches,
                tests=list(tests) if tests else tests,
                extra_args=runner_options,
                random_order=is_bugfix_validation,
                rerun_count=rerun_count,
                global_time_limit=global_time_limit,
                build_type=build_types[0] if is_bugfix_validation else None,
            )

        # These checks run an explicit list of tests, and `clickhouse-test` can
        # filter all of them out (e.g. every selected test is tagged `no-tsan` in
        # a TSan job) - that is a skip, not a failure.
        test_result = ft_res_processor.run(
            runner_exit_code=runner_exit_code,
            is_bugfix_validation=is_labeled_bugfix_validation,
            allow_no_tests=is_flaky_check or is_targeted_check or is_selected_tests_run,
        )

        # Run additional build types for bugfix validation.
        # Exit early on first failure to avoid duplicate test names,
        # workspace pollution, and to preserve logs for analysis.
        # Fatal message checking (CHECK_ERRORS) is done per-build-type here
        # rather than in the outer CHECK_ERRORS stage, so that crashes in any
        # build type are detected even when logs are cleaned between builds.
        if is_bugfix_validation:
            for r in test_result.results:
                r.set_label(build_types[0])

            # Check fatal messages for the first build type before cleaning logs
            first_bt_fatals = CH.check_fatal_messages_in_logs()
            for r in first_bt_fatals:
                r.set_label(build_types[0])
            reconcile_bugfix_crash_repro(test_result, first_bt_fatals)

            if test_result.is_ok():
                for bugfix_bt in build_types[1:]:
                    print(f"\n=== Bugfix validation with {bugfix_bt} ===")
                    # Stop the server before overwriting the binary: on Linux,
                    # `cp` over a running ELF fails with `Text file busy`,
                    # and `strict=True` ensures a failed switch is not ignored.
                    # Use `stop_server` rather than `terminate` so the auxiliary
                    # services (Kafka/Redpanda, SeaweedFS) started
                    # in the outer setup keep running for the next build type;
                    # `terminate` would tear them down, making Kafka/SeaweedFS tests
                    # spuriously "reproduce" a bug under later build types.
                    # `stop_server` does not guarantee that every descendant
                    # process (transient `clickhouse-client` invocations, stray
                    # workers) has released the binary by the time we replace
                    # it, so unlink the destination first: any process still
                    # holding the old inode keeps executing from it, while
                    # `cp` creates a fresh inode for the new binary.
                    CH.stop_server()
                    Shell.run(
                        f"rm -f {ch_path}/clickhouse && "
                        f"cp {temp_dir}/clickhouse_{bugfix_bt} {ch_path}/clickhouse",
                        verbose=True,
                        strict=True,
                    )
                    Shell.run(
                        f"chmod +x {ch_path}/clickhouse",
                        verbose=True,
                        strict=True,
                    )
                    # The downloaded build-type binaries are self-extracting:
                    # the first invocation decompresses the real ELF in place.
                    # Trigger that synchronously here - exactly as the install
                    # stage does via `clickhouse-server --version` - instead of
                    # letting the server self-extract during `start`.
                    # Decompressing a sanitizer binary takes longer than
                    # `start`'s 15s pid-file wait, so the swap would otherwise
                    # time out; worse, a later `clickhouse local` (log scraping)
                    # racing the half-written binary fails with
                    # `open: Is a directory`.
                    Shell.run(
                        "clickhouse-server --version",
                        verbose=True,
                        strict=True,
                    )
                    CH.clean_logs()
                    # The server memory cap must follow the binary being
                    # launched (see the install-stage comment): sanitizer
                    # builds get the tighter 0.7 ratio, the debug build
                    # reverts to the server default. The config tree is not
                    # reinstalled on a binary swap, so the override written
                    # for the previous build type would otherwise persist.
                    if any(san in bugfix_bt for san in SANITIZERS):
                        CH.set_memory_ratio(0.7)
                    else:
                        CH.reset_memory_ratio()
                    # Fail closed if the server cannot come back up after the
                    # binary swap: running tests against a dead server would
                    # produce `Server died` FAILs that the bugfix inverter
                    # then flips into a successful reproduction, even though
                    # the selected binary never became ready. Record an ERROR
                    # row (preserved by `invert_bugfix_validation_status`) and
                    # stop before running tests for this build type.
                    if not (CH.start() and CH.wait_ready()):
                        startup_error = Result(
                            name=f"Server startup ({bugfix_bt})",
                            status=Result.Status.ERROR,
                            info="Server failed to start after switching to the "
                            f"{bugfix_bt} binary",
                        )
                        startup_error.set_label(bugfix_bt)
                        test_result.results.append(startup_error)
                        test_result.status = Result.Status.ERROR
                        break

                    # `start` wipes the server data directory (`run_path0`), so
                    # the environment built once in the START stage is gone: for
                    # stateful suites, reload the stateful data and the
                    # `system.zookeeper` config. Auxiliary services
                    # (Kafka/Redpanda, SeaweedFS) keep running across `stop_server`,
                    # so only the server-side state has to be rebuilt. Without
                    # this a stateful changed test fails only because
                    # `test.hits`/`datasets`/the auxiliary ZooKeeper row
                    # disappeared, and the bugfix inverter reports that false
                    # failure as a successful bug reproduction.
                    reprepared = True
                    reprepare_error = None
                    if has_stateful_tests:
                        # Split the two sub-steps so the persisted Environment
                        # setup row names the operation that actually failed
                        # instead of collapsing both into the generic "failed to
                        # re-prepare stateful data" bucket.
                        if not CH.prepare_stateful_data(
                            with_s3_storage=is_s3_storage,
                            is_db_replicated=is_database_replicated,
                            build_type=bugfix_bt,
                        ):
                            # Prefer the concrete sub-command + ClickHouse error
                            # captured by prepare_stateful_data() over the generic
                            # message, so the (intermittent, msan) re-prepare
                            # failure is diagnosable in CIDB test_context_raw.
                            reprepared = False
                            reprepare_error = (
                                CH.stateful_setup_error
                                or "failed to re-prepare stateful data"
                            )
                        elif not CH.insert_system_zookeeper_config():
                            reprepared = False
                            reprepare_error = "insert_system_zookeeper_config failed"
                    if not reprepared:
                        info_text = (
                            "Failed to re-prepare the test environment "
                            f"after switching to the {bugfix_bt} binary"
                        )
                        if reprepare_error:
                            info_text += f" ({reprepare_error})"
                        setup_error = Result(
                            name=f"Environment setup ({bugfix_bt})",
                            status=Result.Status.ERROR,
                            info=info_text,
                        )
                        setup_error.set_label(bugfix_bt)
                        test_result.results.append(setup_error)
                        test_result.status = Result.Status.ERROR
                        break

                    ft_res_processor_bt = FTResultsProcessor(wd=temp_dir)
                    bt_runner_exit_code = run_tests(
                        batch_num=0,
                        batch_total=0,
                        tests=tests,
                        extra_args=runner_options,
                        random_order=True,
                        rerun_count=1,
                        build_type=bugfix_bt,
                    )
                    bt_result = ft_res_processor_bt.run(
                        runner_exit_code=bt_runner_exit_code,
                        is_bugfix_validation=is_labeled_bugfix_validation,
                    )

                    # Check fatal messages for this build type. As with the
                    # first build type, a `BLOCKER` fatal is the bug crashing
                    # the master binary, not infra: reuse the same downgrade so
                    # a crash-only repro on a later build type
                    # (amd_tsan / amd_msan / amd_debug) is counted as a
                    # reproduction instead of being restored to `ERROR` and
                    # preserved as inconclusive by the inverter.
                    bt_fatals = CH.check_fatal_messages_in_logs()
                    for r in bt_fatals:
                        r.set_label(bugfix_bt)
                    reconcile_bugfix_crash_repro(bt_result, bt_fatals)

                    for r in bt_result.results:
                        r.set_label(bugfix_bt)
                    test_result.results = bt_result.results
                    test_result.status = bt_result.status
                    debug_files += ft_res_processor_bt.debug_files

                    if not bt_result.is_ok():
                        break

        if not info.is_local_run:
            CH.stop_log_exports()

        results.append(test_result)
        debug_files += ft_res_processor.debug_files

        results[-1].set_timing(stopwatch=stop_watch_)
        if results[-1].info:
            job_info = results[-1].info
            results[-1].info = ""

        res = results[-1].is_ok()

    if JobStages.DIAGNOSTICS in stages and test_result and test_result.is_failure():
        diag_stopwatch = Utils.Stopwatch()
        failed_tests_seen = set()
        failed_tests = []
        has_errors = False
        for t in test_result.results:
            if t.is_failure() and t.name and t.name[0].isdigit():
                if t.name not in failed_tests_seen:
                    failed_tests_seen.add(t.name)
                    failed_tests.append(t.name)
            elif t.is_error():
                has_errors = True
                print(
                    "NOTE: Skipping diagnostics because the main test run ended with errors"
                )
                break

        if has_errors:
            pass
        elif len(failed_tests) > 10:
            results.append(
                Result(
                    name="Diagnostics",
                    status=Result.Status.SKIPPED,
                    info="Too many failed tests",
                ).set_timing(stopwatch=diag_stopwatch)
            )
        elif failed_tests:
            memory_limit = stateless_memory_limit(Info().job_name)
            diag_command = (
                f"clickhouse-test --testname --check-zookeeper-session --hung-check"
                f" --memory-limit {memory_limit} --trace --capture-client-stacktrace"
                f" --queries ./tests/queries --shard --zookeeper"
                f" --diagnose-random-settings"
                f" --random-settings-diagnostics-dir {diagnostics_dir}"
                f" --no-random-settings --no-random-merge-tree-settings"
                f" -- {' '.join(failed_tests)}"
            )
            print(f"Running diagnostics for {len(failed_tests)} test(s)...")
            diag_exit_code = Shell.run(diag_command, verbose=True)

            # Read diagnostics results and prepend to original test info
            diag_results_path = os.path.join(
                diagnostics_dir, "random_settings_diagnostics_results.jsonl"
            )
            diag_results = {}
            if os.path.isfile(diag_results_path):
                with open(diag_results_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entry = json.loads(line)
                            diag_results[entry["test_name"]] = entry
            label_map = {
                "setting": Result.Label.SETTING_VALUE,
                "flaky": Result.Label.FLAKY,
                "reproducible": Result.Label.REPRODUCIBLE,
            }
            for test_case in test_result.results:
                diag = diag_results.get(test_case.name)
                if not diag:
                    continue
                if diag.get("diagnosis"):
                    test_case.info = diag["diagnosis"] + "\n" + test_case.info
                label_key = diag.get("label", "")
                if label_key in label_map:
                    test_case.set_label(label_map[label_key])
                if label_key == "flaky" and is_llvm_coverage:
                    # Coverage binaries are slow and prone to timing-related flakiness
                    # (e.g. TIMEOUT_EXCEEDED on SystemLogQueue). Don't penalise them
                    # for it — mark the test green so it doesn't block coverage jobs.
                    # See: https://github.com/ClickHouse/ClickHouse/pull/95763
                    test_case.set_status(Result.Status.OK)
            if diag_exit_code != 0:
                diag_status = Result.Status.FAIL
                diag_info = (
                    f"Diagnostics runner exited with code {diag_exit_code}; "
                    f"diagnosed {len(diag_results)} out of {len(failed_tests)} failed test(s)"
                )
            else:
                diag_status = Result.Status.OK
                diag_info = (
                    f"Diagnosed {len(diag_results)} out of {len(failed_tests)} failed test(s)"
                )
            results.append(
                Result(
                    name="Diagnostics",
                    status=diag_status,
                    info=diag_info,
                ).set_timing(stopwatch=diag_stopwatch)
            )

    if args.debug:
        print("\n\n=== Debug mode enabled, starting clickhouse-client ===\n")
        subprocess.call("clickhouse-client", shell=True)

    CH.terminate()

    reset_success = False
    if (
        test_result
        and not test_result.is_error()
        and JobStages.COLLECT_COVERAGE in stages
    ):
        print("Collect coverage")
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        results.append(
            Result.from_commands_run(
                name="Collect coverage",
                command=lambda: CoverageExporter(
                    src=CH,
                    dest=cidb_cluster,
                    job_name=info.job_name,
                ).do(),
            )
        )
        if results[-1].is_ok():
            reset_success = True

    if test_result and JobStages.CHECK_ERRORS in stages:
        print("Check fatal errors")
        sw_ = Utils.Stopwatch()
        results.append(
            Result.create_from(
                name="Check errors",
                results=CH.check_fatal_messages_in_logs(),
                status=Result.Status.OK,
                stopwatch=sw_,
            )
        )
        # fatal failures found in logs represented as normal test cases
        test_result.extend_sub_results(results[-1].results)
        results[-1].results = []

    # `invert_bugfix_validation_status` below rewrites a reproduced failure to
    # `OK` (and a no-repro to `SKIPPED`), both of which `Result.is_ok` accepts.
    # The collect-logs gate must see the run's real outcome, or a bugfix
    # validation job that reproduced a crash would attach neither its cores nor
    # its full logs.
    test_run_failed = bool(test_result) and not test_result.is_ok()

    # invert result status for bugfix validation
    bugfix_validation_no_repro = False
    if is_labeled_bugfix_validation and test_result:
        # `invert_bugfix_validation_status` returns True when the bug did not
        # reproduce on this arch. In that case it sets `test_result` to
        # SKIPPED; the SKIPPED status must also be propagated to the top-level
        # `R` below, because `Result.create_from` treats SKIPPED child results
        # as benign and defaults the parent status to OK - which would let the
        # post-hook in `new_tests_check.py` count this per-arch job as a
        # validation via `is_success()`.
        bugfix_validation_no_repro = invert_bugfix_validation_status(test_result)

    if JobStages.COLLECT_LOGS in stages:
        print("Collect logs")

        def collect_logs():
            CH.prepare_logs(all=test_run_failed, info=info)

        results.append(
            Result.from_commands_run(
                name="Collect logs",
                command=collect_logs,
            )
        )
        if test_result and CH.extra_tests_results:
            attach_post_verdict_artifacts(
                test_result,
                CH.extra_tests_results,
                preserve_verdict=is_labeled_bugfix_validation,
            )

    # Decide whether to block the CI pipeline on test failures
    force_ok_exit = False
    if test_result:
        failures_cnt = len([r for r in test_result.results if not r.is_ok()])
        if failures_cnt > 0 and failures_cnt < 2:
            print(
                f"NOTE: Failed {failures_cnt} tests - do not block pipeline, exit with 0"
            )
            force_ok_exit = True
        elif failures_cnt > 0 and "ci-non-blocking" in info.pr_labels:
            print(
                f"NOTE: Failed {failures_cnt} tests, label 'ci-non-blocking' is set - do not block pipeline - exit with 0"
            )
            force_ok_exit = True
    if is_llvm_coverage and test_result:
        # do not block pipeline on amd_llvm_coverage job failures
        print("NOTE: LLVM coverage job - do not block pipeline - exit with 0")
        force_ok_exit = True
    if is_bugfix_validation:
        # Per-arch bugfix-validation jobs are advisory: their pass/fail status
        # records "did the bug reproduce on this arch?", not whether the PR
        # should be blocked. Setting `do_not_block_pipeline_on_failure=True`
        # marks the job as non-blocking so downstream jobs are not dropped
        # when this job reports FAIL. The process itself still exits with
        # the natural status (`Result.complete_job` calls `sys.exit(1)` on
        # non-OK results); the non-blocking flag is metadata for the
        # pipeline scheduler. The PR-merge-blocking decision lives in the
        # `new_tests_check.py` workflow post-hook, which OR's the per-arch
        # bugfix-validation job statuses.
        print(
            "NOTE: Bugfix validation job - marking as non-blocking; "
            "failure here will not block downstream pipeline jobs "
            "(process exit code still reflects the actual job status)"
        )
        force_ok_exit = True

    if test_result:
        test_result.sort()

    # On a test timeout or a failed hung check the full server stacktrace
    # dumps land in the working directory (stdout keeps a trimmed preview).
    for stacktrace_log in ("sql_stacktraces.log", "c_stacktraces.log"):
        stacktrace_log_path = Path(stacktrace_log)
        if stacktrace_log_path.exists():
            debug_files.append(stacktrace_log_path)

    R = Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=CH.logs + debug_files,
        info=job_info,
    )

    if bugfix_validation_no_repro:
        # See the comment above where `bugfix_validation_no_repro` is set.
        # `R` is otherwise OK because `Result.create_from` skips over SKIPPED
        # children when deriving the parent status. Mirror the per-arch
        # integration-test path (`integration_test_job.py`) and set SKIPPED on
        # `R` directly so the post-hook does not treat this arch as a
        # validation.
        R.set_status(Result.Status.SKIPPED).set_info(
            "Bug does not reproduce on this arch - bugfix validation N/A"
        )

    if is_llvm_coverage and not is_per_test_coverage:
        print("Collecting and merging LLVM coverage files...")
        Shell.get_output("pwd", verbose=True).strip().split("\n")
        profraw_files = (
            Shell.get_output("find . -name '*.profraw'", verbose=True)
            .strip()
            .split("\n")
        )
        profraw_files = [f.strip() for f in profraw_files if f.strip()]

        # Name the profile after this job's own coverage artifact, so the
        # aggregation can tell which shards arrived from the filenames alone.
        # JOB_CONFIG has been through dump()/get() by the time a job body runs,
        # so it is a plain dict here.
        _provides = (info.job_config or {}).get("provides")
        assert (
            isinstance(_provides, list)
            and len(_provides) == 1
            and isinstance(_provides[0], str)
            and _provides[0]
        ), f"expected exactly one provided artifact name, got {_provides!r}"
        merged_file = f"./{_provides[0]}.profdata"

        # llvm-profdata truncates its -o target in place instead of replacing it,
        # so a stale profile at the target name must be removed before deciding
        # whether to merge at all - otherwise a skipped or failed merge would let
        # the uploader publish the stale file as this shard's contribution.
        if os.path.exists(merged_file):
            print(f"Removing pre-existing {merged_file}")
            os.unlink(merged_file)

        # A missing test_result means the test stage never ran, and a runner-level
        # ERROR means it terminated unexpectedly; either way the .profraw files
        # understate coverage. FAIL is a completed run and still publishes.
        if test_result is None or test_result.is_error():
            _gate_reason = (
                "the test stage did not run"
                if test_result is None
                else "the test runner terminated unexpectedly (runner-level ERROR)"
            )
            print(
                f"ERROR: {_gate_reason}, so this shard's coverage is incomplete; "
                f"publishing no profile"
            )
            profraw_files = []

        # A zero-length .profraw is silently accepted by llvm-profdata at every
        # --failure-mode, so it would drop one process's coverage with no signal.
        # Treat it as an incomplete shard and publish no profile.
        _empty_files = [f for f in profraw_files if os.path.getsize(f) == 0]
        if _empty_files:
            print(
                f"ERROR: {len(_empty_files)} .profraw files are empty, so this shard's "
                f"coverage is incomplete; publishing no profile: {', '.join(_empty_files)}"
            )
            profraw_files = []

        if profraw_files:
            print(f"Found {len(profraw_files)} .profraw files:")
            for f in profraw_files:
                try:
                    size_bytes = os.path.getsize(f)
                    print(f"  {size_bytes:>12} bytes | {f}")
                except OSError:
                    continue

            # Auto-detect available LLVM profdata tool
            llvm_profdata = None
            for ver in ["22", "21", "20", "18", "19", "17", "16", ""]:
                cmd = f"llvm-profdata{'-' + ver if ver else ''}"
                if Shell.check(f"command -v {cmd}", verbose=False):
                    llvm_profdata = cmd
                    break

            if not llvm_profdata:
                print("ERROR: llvm-profdata not found in PATH")
            else:
                print(f"Using {llvm_profdata} to merge coverage files")

                # --failure-mode=any makes the merge all-or-nothing: on any invalid
                # input it exits non-zero and writes no file, so the shard is simply
                # absent (and the aggregate job reports SKIPPED with the shard name)
                # instead of contributing a silently short profile.
                merge_cmd = f"{llvm_profdata} merge -sparse -failure-mode=any {' '.join(profraw_files)} -o {merged_file} 2>&1"
                merge_output = Shell.get_output(merge_cmd, verbose=True)

                # Attach profdata file to the result report so it is uploaded
                # unconditionally (even when tests fail) and visible in the CI report.
                if os.path.exists(merged_file):
                    R.files.append(merged_file)
                else:
                    print(f"ERROR: coverage merge produced no profile:\n{merge_output}")

        else:
            print("No usable .profraw files found for coverage")

    if reset_success:
        # coverage job ignores test failures
        R.set_success()

    R.complete_job(do_not_block_pipeline_on_failure=force_ok_exit)


if __name__ == "__main__":
    main()
