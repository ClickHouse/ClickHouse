"""The FT diagnostics rerun must mirror the environment of the run it diagnoses.

`ci/jobs/functional_tests.py` reruns each failed test with
`--diagnose-random-settings` to decide whether the failure is reproducible. If
that rerun does not receive the job's own runner options
(`--replicated-database`, `--s3-storage`, ...), a configuration-specific failure
cannot reproduce, the diagnosis says "not reproducible", the test is labelled
`flaky` and on `llvm_coverage` jobs it is force-set to `OK` - so a
deterministically failing test is reported green in the CI report and in CIDB.

These tests pin the three properties the forwarding depends on, driving the real
`tests/clickhouse-test` argument parser (never a stand-in copy: a hand-built
parser that omits the `add_mutually_exclusive_group` wrappers reports
last-one-wins and would pin a false property).
"""

import inspect
import os
import re
import runpy
import shlex
import stat
import sys
import textwrap
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.functional_tests as functional_tests  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
CLICKHOUSE_TEST = os.path.join(REPO_ROOT, "tests", "clickhouse-test")

# Representative `runner_options` strings, assembled exactly as `main` does
# from OPTIONS_TO_TEST_RUNNER_ARGUMENTS plus the unconditional tail.
_COVERAGE_TAIL = (
    " --llvm-coverage --no-random-settings --no-random-merge-tree-settings  --no-long"
    " --jobs 57 --random-settings-diagnostics-dir /tmp/diag"
)
RUNNER_OPTIONS_CASES = {
    # DBReplicated is the flavour that hid the access-control bypass in #111561.
    # --encrypted-storage is appended by `main` OUTSIDE the options map, so a
    # map-driven case list would miss it (`main` adds it only when s3/azure is on).
    "DBReplicated_s3_coverage": functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS[
        "s3 storage"
    ]
    + functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS["DBReplicated"]
    + " "
    + functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS["parallel"]
    + _COVERAGE_TAIL
    + " --encrypted-storage",
    # ParallelReplicas carries --no-zookeeper --no-shard: passing the positive
    # twins as well is an argparse error (exit 2).
    "ParallelReplicas_s3_coverage": functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS[
        "ParallelReplicas"
    ]
    + " "
    + functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS["s3 storage"]
    + _COVERAGE_TAIL,
    "azure_sequential_coverage": functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS[
        "azure"
    ]
    + " "
    + functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS["sequential"]
    + _COVERAGE_TAIL,
    "AsyncInsert_parallel": functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS[
        "AsyncInsert"
    ]
    + " "
    + functional_tests.OPTIONS_TO_TEST_RUNNER_ARGUMENTS["parallel"]
    + " --jobs 57 --random-settings-diagnostics-dir /tmp/diag",
    "plain": " --jobs 57 --random-settings-diagnostics-dir /tmp/diag",
}

FAILED_TESTS = ["04541_create_as_select_table_scoped_insert_grant", "04418_x"]

# The stage reruns at most 10 tests (above that it is SKIPPED as "Too many
# failed tests"), so this covers the whole reachable count range.
TEN = [f"0454{i}_x" for i in range(10)]

# (elapsed minutes, failed test count) cells the budget must hold for. The job
# timeout is 150 minutes, so elapsed values run up to just under it.
BUDGET_MATRIX = [
    (elapsed, num_tests)
    for elapsed in (0, 15, 30, 45, 60, 75, 90, 105, 120, 130, 140)
    for num_tests in (1, 2, 3, 5, 7, 10)
]


def _main_run_test_timeout():
    """`clickhouse-test`'s `--timeout` default, parsed from its own source.

    The main run passes no `--timeout` (`run_tests` builds its command without
    one), so this default is the bound every test in the main pass gets.
    """
    source = open(CLICKHOUSE_TEST, encoding="utf-8").read()
    block = source[source.index('        "--timeout",') :]
    match = re.search(r"default=(\d+),", block)
    assert match, block[:400]
    return int(match.group(1))


def _diagnostics_source_block():
    """Return the diagnostics-command construction, verbatim from `main`."""
    source = inspect.getsource(functional_tests.main)
    marker = "            diag_options = runner_options"
    assert source.count(marker) == 1, "the slice start marker must be unique"
    start = source.index(marker)
    tail = source.index("f\" -- {' '.join(failed_tests)}\"", start)
    end = source.index("\n", source.index("\n", tail) + 1)
    return textwrap.dedent(source[start:end])


def _build_diag_command(runner_options, elapsed=0.0, failed_tests=None):
    """Compose the diagnostics command by EXECUTING `main`'s own source block.

    The block's timeouts come from the real `diagnostics_budget`, and the block
    itself runs against the real `functional_tests` globals with only its own
    local inputs stubbed. So the composed command is driven by the shipped
    function and constants, never by copies living here.
    """
    tests = FAILED_TESTS if failed_tests is None else failed_tests
    budget = functional_tests.diagnostics_budget(elapsed, len(tests))
    assert budget is not None, (elapsed, len(tests))
    diag_stage_timeout, diag_test_timeout = budget
    namespace = dict(vars(functional_tests))
    namespace.update(
        {
            "runner_options": runner_options,
            "memory_limit": 12345,
            "diagnostics_dir": "/tmp/diag",
            "failed_tests": tests,
            "stop_watch": SimpleNamespace(duration=elapsed),
            "diag_stage_timeout": diag_stage_timeout,
            "diag_test_timeout": diag_test_timeout,
        }
    )
    exec(_diagnostics_source_block(), namespace)  # noqa: S102
    return namespace["diag_command"]


def _diag_test_timeout(command):
    """The `--timeout` value the diagnostics command passes, read before `--`."""
    flags, separator, _ = command.partition(" -- ")
    assert separator, command
    tokens = shlex.split(flags)
    assert "--timeout" in tokens, (
        "The diagnostics rerun must bound each test: an unbounded rerun loop can "
        "outlast the job timeout, which is recorded as ERROR and skips the "
        f"artifact upload the coverage job depends on. Got: {command}"
    )
    return int(tokens[tokens.index("--timeout") + 1])


@pytest.fixture(scope="module")
def parse_test_runner_args(tmp_path_factory):
    """`tests/clickhouse-test`'s own parse_args, loaded with runpy.

    `runpy.run_path` on that file is the established in-tree idiom
    (`ci/jobs/scripts/functional_tests_results.py`). A stub `clickhouse` on PATH
    satisfies the `--binary` default's `find_binary` type; nothing is executed.
    """
    stub_dir = tmp_path_factory.mktemp("stub-bin")
    stub = stub_dir / "clickhouse"
    stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

    globals_ = runpy.run_path(CLICKHOUSE_TEST)
    real_parse_args = globals_["parse_args"]

    def parse(argv):
        old_argv, old_path = sys.argv, os.environ["PATH"]
        sys.argv = ["clickhouse-test"] + argv
        os.environ["PATH"] = f"{stub_dir}{os.pathsep}{old_path}"
        try:
            return real_parse_args()
        finally:
            sys.argv, os.environ["PATH"] = old_argv, old_path

    return parse


def test_diagnostics_command_forwards_runner_options():
    """The job's own runner options must reach the rerun, before the separator."""
    block = _diagnostics_source_block()
    assert "diag_options = runner_options" in block, (
        "The diagnostics rerun must forward the job's runner_options, or a "
        "configuration-specific failure cannot reproduce in it."
    )
    assert "{diag_options}" in block

    # The two positive flags must be conditional, mirroring `run_tests`: passing
    # e.g. both --shard and --no-shard is an argparse error (exit 2).
    assert '--shard --zookeeper"' not in block, (
        "--shard/--zookeeper must not be passed unconditionally: the forwarded "
        "options may carry --no-shard/--no-zookeeper, which conflicts."
    )
    assert 'if "--no-shard" not in diag_options:' in block
    assert 'if "--no-zookeeper" not in diag_options:' in block

    # Everything after the `--` separator is consumed as a test-name regex
    # (`parser.add_argument("test", nargs="*")`), so the forwarded options and
    # both conditional positives must appear BEFORE it.
    command = _build_diag_command(" --replicated-database")
    flags, separator, tests = command.partition(" -- ")
    assert separator, command
    assert "--replicated-database" in flags
    assert " --shard" in flags and " --zookeeper" in flags
    assert tests.split() == FAILED_TESTS


def test_diagnostics_command_bounds_each_rerun(parse_test_runner_args):
    """Each rerun must carry an explicit per-test timeout, before the separator.

    Without one, `clickhouse-test` falls back to its 600 s default and the stage
    can outlast the job timeout. That is recorded as `Result.Status.ERROR`, which
    skips the artifact upload, so the coverage job loses its input.
    """
    command = _build_diag_command(" --replicated-database")
    timeout = _diag_test_timeout(command)
    assert timeout > 0

    flags, _, tests = command.partition(" -- ")
    assert "--timeout" in flags and "--timeout" not in tests, command

    args = parse_test_runner_args(shlex.split(command)[1:])
    assert args.timeout == timeout
    assert args.test == FAILED_TESTS


def test_diagnostics_budget_shrinks_with_elapsed_time_and_test_count():
    """The bound must be derived from the job's remaining time, not a constant.

    A fixed value cannot keep the stage inside the job timeout: what is left
    depends on how long the main run already took and on how many tests the
    stage has to rerun.
    """
    fresh = _diag_test_timeout(_build_diag_command("", elapsed=0, failed_tests=TEN))
    late = _diag_test_timeout(
        _build_diag_command("", elapsed=60 * 60, failed_tests=TEN)
    )
    assert late < fresh, (fresh, late)

    three_tests = _diag_test_timeout(
        _build_diag_command("", elapsed=60 * 60, failed_tests=TEN[:3])
    )
    assert late < three_tests, (three_tests, late)


def test_diagnostics_rerun_is_never_more_permissive_than_the_main_run():
    """The rerun's per-test timeout must not exceed the main run's own.

    The main run passes no `--timeout`, so it uses `clickhouse-test`'s 600 s
    default. A rerun given MORE time can pass a test the main pass timed out on,
    which reads as `Failed 0 out of 3 reruns`, is labelled `flaky` and is then
    force-set to `OK` on coverage jobs - the exact whitewash this PR removes.
    """
    for elapsed_min, num_tests in BUDGET_MATRIX:
        budget = functional_tests.diagnostics_budget(elapsed_min * 60, num_tests)
        if budget is None:
            continue
        _, test_timeout = budget
        assert test_timeout <= functional_tests.DIAGNOSTICS_MAIN_RUN_TEST_TIMEOUT_S, (
            elapsed_min,
            num_tests,
            test_timeout,
        )
        command = _build_diag_command(
            "", elapsed=elapsed_min * 60, failed_tests=TEN[:num_tests]
        )
        assert _diag_test_timeout(command) == test_timeout

    # The default the main run actually gets, so the cap is pinned to a real value.
    assert (
        functional_tests.DIAGNOSTICS_MAIN_RUN_TEST_TIMEOUT_S == _main_run_test_timeout()
    )


def test_diagnostics_stage_bound_leaves_the_finalization_reserve():
    """The stage's outer bound must always end before the job timeout minus reserve.

    A job that hits its own timeout is recorded as `Result.Status.ERROR`, which
    skips `complete_job` and therefore the artifact upload, so the downstream
    coverage job loses the file these jobs provide.
    """
    for elapsed_min, num_tests in BUDGET_MATRIX:
        budget = functional_tests.diagnostics_budget(elapsed_min * 60, num_tests)
        if budget is None:
            continue
        stage_timeout, _ = budget
        assert (
            elapsed_min * 60 + stage_timeout
            <= functional_tests.DIAGNOSTICS_JOB_TIMEOUT
            - functional_tests.DIAGNOSTICS_RESERVE_S
        ), (elapsed_min, num_tests, stage_timeout)


def test_diagnostics_is_given_up_when_too_little_job_time_is_left():
    """Below a usable per-test bound the stage must be skipped, not truncated.

    Starting a diagnosis that cannot finish is what overruns the reserve, so the
    budget returns `None` and `main` reports a SKIPPED `Diagnostics` result.
    """
    assert functional_tests.diagnostics_budget(140 * 60, 1) is None
    assert functional_tests.diagnostics_budget(130 * 60, 3) is None
    # And an ordinary case is still funded, so the guard is not vacuous.
    assert functional_tests.diagnostics_budget(60 * 60, 3) is not None

    source = inspect.getsource(functional_tests.main)
    assert 'info="Not enough job time left to diagnose"' in source, (
        "main must skip the diagnosis when diagnostics_budget returns None, "
        "rather than starting a run that cannot finish."
    )
    assert (
        "diagnostics_budget(stop_watch.duration, len(failed_tests)) is None" in source
    )


def test_diagnostics_stage_has_an_outer_timeout():
    """The per-test cap alone cannot bound the stage, so the run needs an outer one.

    On the 23 of 37 DIAGNOSTICS-reachable flavours that save randomized settings,
    `diagnose_random_settings` reruns a test an unbounded number of times
    (two steps, up to two category probes, then ddmin), so no `tests x reruns`
    model bounds it. Only the outer `Shell.run` timeout does. Asserted on `main`
    source, as `Shell.run` is never executed here.
    """
    source = inspect.getsource(functional_tests.main)
    assert (
        "Shell.run(\n                diag_command, verbose=True, timeout=diag_stage_timeout\n            )"
        in source
    ), "the diagnostics Shell.run must be bounded by the stage timeout"


def test_diagnostics_job_timeout_matches_the_real_job_config():
    """The hand-copied job timeout must equal the one the job actually gets."""
    from ci.defs.job_configs import common_ft_job_config

    assert functional_tests.DIAGNOSTICS_JOB_TIMEOUT == common_ft_job_config.timeout


def test_diagnostics_reruns_per_test_matches_clickhouse_test():
    """The divisor must equal `clickhouse-test`'s own `max_reruns`."""
    source = open(CLICKHOUSE_TEST, encoding="utf-8").read()
    matches = re.findall(r"^\s*max_reruns = (\d+)$", source, re.MULTILINE)
    assert len(matches) == 1, matches
    assert functional_tests.DIAGNOSTICS_RERUNS_PER_TEST == int(matches[0])


@pytest.mark.parametrize("case", sorted(RUNNER_OPTIONS_CASES))
def test_diagnostics_command_never_passes_conflicting_shard_or_zookeeper_flags(
    case, parse_test_runner_args
):
    """The composed command must parse, with the job's own shard/zookeeper intent."""
    runner_options = RUNNER_OPTIONS_CASES[case]
    command = _build_diag_command(runner_options)
    argv = shlex.split(command)
    assert argv[0] == "clickhouse-test"

    args = parse_test_runner_args(argv[1:])

    assert args.shard is ("--no-shard" not in runner_options)
    assert args.zookeeper is ("--no-zookeeper" not in runner_options)
    # A flag placed after the `--` separator would be swallowed into args.test.
    assert args.test == FAILED_TESTS
    # The environment flags actually arrive.
    assert args.replicated_database is ("--replicated-database" in runner_options)
    assert args.s3_storage is ("--s3-storage" in runner_options)
    assert args.azure_blob_storage is ("--azure-blob-storage" in runner_options)
    assert args.no_parallel_replicas is ("--no-parallel-replicas" in runner_options)
    assert args.no_async_insert is ("--no-async-insert" in runner_options)
    assert args.encrypted_storage is ("--encrypted-storage" in runner_options)


def test_conflicting_shard_flags_really_are_rejected(parse_test_runner_args):
    """Negative control: the conflict the conditional defaulting avoids is real.

    Deliberately independent of the diagnostics command builder - this is a
    property of `tests/clickhouse-test`'s parser, and it must keep failing
    loudly if that parser ever stops declaring the flags mutually exclusive.
    """
    base = ["--queries", "./tests/queries", "--"] + FAILED_TESTS
    assert parse_test_runner_args(["--no-shard", "--no-zookeeper"] + base)
    with pytest.raises(SystemExit):
        parse_test_runner_args(["--shard", "--no-shard"] + base)
    with pytest.raises(SystemExit):
        parse_test_runner_args(["--zookeeper", "--no-zookeeper"] + base)


def test_reproducible_diagnosis_is_not_whitewashed():
    """A reproducing rerun must label `reproducible`, a partial one `flaky`.

    `reproducible` misses the `label_key == "flaky"` guard in
    functional_tests.py, so the test correctly stays FAIL. The complementary
    assertion pins that PR #95763's genuine coverage timing flakiness is STILL
    tolerated. Driven by the real classifier in `tests/clickhouse-test`.
    """
    label_of = _diagnosis_label_classifier()
    assert label_of("--- Failure reproducibility check ---\nFailed 3 out of 3 reruns.\n") == (
        "reproducible"
    )
    assert label_of("--- Failure reproducibility check ---\nFailed 1 out of 3 reruns.\n") == (
        "flaky"
    )
    assert label_of("--- Failure reproducibility check ---\nFailed 0 out of 3 reruns.\n") == (
        "flaky"
    )


def _diagnosis_label_classifier():
    """Extract the label classification from `run_diagnose_from_artifacts`.

    `tests/clickhouse-test` has no standalone classifier function, so the
    if/elif chain is executed verbatim out of its own source rather than
    re-implemented here (a re-implementation could drift silently).
    """
    source = open(CLICKHOUSE_TEST, encoding="utf-8").read()
    marker = '            label = ""\n'
    start = source.index(marker)
    end = source.index("            result_entry = {", start)
    block = textwrap.dedent(source[start:end])
    assert "Failed (\\d+) out of (\\d+) reruns" in block, block

    def label_of(diagnosis):
        namespace = {"diagnosis": diagnosis, "re": __import__("re")}
        exec(block, namespace)  # noqa: S102
        return namespace["label"]

    return label_of


def test_non_coverage_jobs_status_is_unaffected():
    """The OK override stays gated on `is_llvm_coverage`.

    So a corrected diagnosis on the ~40 non-coverage FT flavours cannot change
    any test's pass/fail: both `flaky` and `reproducible` leave the status FAIL
    there.
    """
    source = inspect.getsource(functional_tests.main)
    assert 'if label_key == "flaky" and is_llvm_coverage:' in source
    assert source.count("test_case.set_status(Result.Status.OK)") == 1
