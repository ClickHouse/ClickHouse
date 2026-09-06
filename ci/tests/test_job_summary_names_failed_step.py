"""
Tests for the job summary that `Result.complete_job` writes into a job's own info.

A praktika job publishes one job-level CIDB row plus one row per child of the node
named by `result_name_for_cidb`. A step that fails outside that node - `Install
ClickHouse`, `Start ClickHouse Server`, `Checkout Submodules` - is therefore reachable
from no row at all. The summary named only a count, so the cause reached neither CIDB,
nor the job's own report page, nor its GitHub issue body. It now names each failed step.

The summary is written only while `info` is empty. That is exactly the affected class:
`ci/jobs/functional_tests.py` lifts the `Tests` node's text into the job info at the end
of its test stage, and that stage is gated on every earlier stage having succeeded, so a
job that died before it leaves `info` empty. The same guard is what keeps the two
root-copy shapes (`ci/jobs/fast_test.py`, `ci/jobs/performance_tests.py`) from doubling
their text.

`Result.from_commands_run` already caps a step's own info at 300 lines, two different
ways, and the cause can sit at either end of the result. `_trim_step_info` therefore
keeps both ends rather than one.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result

MARKER = "Poco::Exception. Code: 1000, e.code() = 111, Connection refused"
# `Result.from_commands_run`'s default branch emits a leading marker plus the last 300
# lines, so 301 lines - matching real master payloads at 301 lines / ~26 KB. The cause
# sits at the very tail.
_SERVER_LOG_BODY = [f"srvlog{i}: " + "x" * 76 for i in range(1, 300)]
SERVER_LOG = "\n".join(["~~~~~ truncated 912 lines ~~~~~", *_SERVER_LOG_BODY, MARKER])
# Its error-centered branch wraps 300 retained lines in BOTH a leading and a trailing
# marker, so 302 is the real ceiling and the cause is NOT last. Reached by any step whose
# captured log holds ": error:" - `Install ClickHouse` running `clickhouse install`, or a
# `Start ClickHouse Server` log tail.
_ERROR_LOG_RETAINED = (
    [f"ctx{i}: " + "x" * 78 for i in range(50)]
    + ["src/Foo.cpp:12:3: error: boom"]
    + [f"ctx{i}: " + "x" * 78 for i in range(50, 249)]
    + [MARKER]
    + [f"ctx{i}: " + "x" * 78 for i in range(249, 298)]
)
ERROR_CENTERED_LOG = "\n".join(
    [
        "~~~~~ truncated 50 lines at the beginning ~~~~~",
        *_ERROR_LOG_RETAINED,
        "~~~~~ truncated 650 lines at the end ~~~~~",
    ]
)
FAST_TEST_INFO = "1 tests failed:\n00001_foo FAIL"


def node(name, status, info="", results=None):
    return Result(
        name=name,
        status=status,
        info=info,
        results=results or [],
        start_time=1700000000.0,
        duration=1.0,
    )


def job(children, info=""):
    """A job result shaped like `functional_tests.py` builds one, then summarized."""
    result = node(
        "Stateless tests (amd_debug, parallel)", Result.Status.FAIL, info, children
    )
    result._add_job_summary_to_info()
    return result


def make_tests_stage(status=Result.Status.OK, info="", cases=None):
    return node(
        "Tests",
        status,
        info,
        cases if cases is not None else [node("00001_ok", Result.Status.OK)],
    )


@pytest.mark.parametrize("step_status", [Result.Status.FAIL, Result.Status.ERROR])
def test_failing_step_outside_tests_node_is_named(step_status):
    """
    The real master shape: `Start ClickHouse Server` dies before the test phase.

    Both statuses are read, matching the summary's own `is_ok()` predicate:
    `ci/jobs/performance_tests.py` appends a direct `Download datasets` child with
    `status=ERROR`.
    """
    result = job(
        [
            node("Install ClickHouse", Result.Status.OK),
            node("Start ClickHouse Server", step_status, SERVER_LOG),
            node("Collect logs", Result.Status.OK),
        ]
    )
    assert result.info.startswith("Failures: 1/3\n")
    assert "Start ClickHouse Server: " in result.info
    assert MARKER in result.info


def test_two_failing_steps_are_both_named():
    result = job(
        [
            node("Install ClickHouse", Result.Status.FAIL, "INSTALL_MARKER"),
            node("Collect logs", Result.Status.FAIL, "LOGS_MARKER"),
            make_tests_stage(),
        ]
    )
    assert result.info == (
        "Failures: 2/3\n"
        "Install ClickHouse: INSTALL_MARKER\n"
        "Collect logs: LOGS_MARKER"
    )


def test_count_line_stays_the_first_line():
    """Queries and readers written against the old value keep matching."""
    result = job([node("Install ClickHouse", Result.Status.FAIL, "INSTALL_MARKER")])
    assert result.info.splitlines()[0] == "Failures: 1/1"


def test_all_children_ok():
    result = job([node("Install ClickHouse", Result.Status.OK), make_tests_stage()])
    assert result.info == "Failures: 0/2"


def test_skipped_step_is_not_a_failure():
    """`is_ok()` accepts SKIPPED, so a skipped step is neither counted nor named."""
    result = job(
        [
            node("Diagnostics", Result.Status.SKIPPED, "Too many failed tests"),
            make_tests_stage(),
        ]
    )
    assert result.info == "Failures: 0/2"


def test_failing_step_with_empty_info():
    """A failing step with no info of its own contributes no line, only its count."""
    result = job(
        [
            make_tests_stage(Result.Status.FAIL, cases=[]),
            node("Check errors", Result.Status.FAIL),
        ]
    )
    assert result.info == "Failures: 2/2"


@pytest.mark.parametrize(
    "existing_info",
    ["3 tests failed", FAST_TEST_INFO, f"Unit tests: {MARKER}"],
    ids=["plain", "fast-test-root-copy", "lifted-child-info"],
)
def test_existing_job_info_is_never_touched(existing_info):
    """
    The summary writes only while `info` is empty.

    `ci/jobs/fast_test.py` copies the `Tests` node's info onto the root without
    clearing it, and `Result.create_from(with_info_from_results=True)` already lifts
    child info as `"<name>: <info>"`. Both would double their own text otherwise.
    """
    result = job(
        [
            make_tests_stage(
                Result.Status.FAIL,
                existing_info,
                [node("00001_foo", Result.Status.FAIL, "assert")],
            )
        ],
        info=existing_info,
    )
    assert result.info == existing_info
    assert "Failures:" not in result.info


def test_nested_failure_under_a_failing_child_is_not_reached():
    """
    Only direct children are named: `Result.results` has no cardinality bound, and a
    test-case list one level down would put thousands of rows into one string. The
    direct child is still named, so the subtree is locatable.
    """
    result = job(
        [
            node(
                "Keeper Stress (with-faults)",
                Result.Status.FAIL,
                results=[
                    node("test_scenario[prod-mix]", Result.Status.FAIL, "KEEPER_MARKER")
                ],
            )
        ]
    )
    assert result.info == "Failures: 1/1"
    assert "KEEPER_MARKER" not in result.info


@pytest.mark.parametrize(
    "payload, line_count",
    [(SERVER_LOG, 301), (ERROR_CENTERED_LOG, 302)],
    ids=["default-branch", "error-centered"],
)
def test_real_payload_shapes_are_named_whole(payload, line_count):
    """
    Both `from_commands_run` shapes are ~26 KB, and neither is re-capped by line here:
    the trim is a character bound applied to both ends. Each shape's own line count is
    pinned so a clip of the appended text cannot stay green.
    """
    step = node("Start ClickHouse Server", Result.Status.FAIL, payload)
    result = job([node("Install ClickHouse", Result.Status.OK), step])

    named = result.info.split("\n", 1)[1]
    assert len(step.info.splitlines()) == line_count
    # Exact equality: a real payload of this size is named whole, so any clip of
    # the appended text - at either end or in the middle - reddens here.
    assert named == f"{step.name}: {step.info}"
    assert len(named) > 20_000
    assert MARKER in named


@pytest.mark.parametrize(
    "cause_at_end", [False, True], ids=["cause-near-front", "cause-at-tail"]
)
def test_trim_keeps_both_ends_of_an_oversized_step(cause_at_end):
    """
    `from_commands_run` puts the cause near the front in its error-centered branch and
    at the very tail in its default branch, so a one-ended trim drops the cause in one
    of the two. The head also carries the step name, which is the job row's only
    identifying text.
    """
    filler = "filler line to exceed the character bound\n" * 1200
    payload = filler + "FATAL_CAUSE" if cause_at_end else "FATAL_CAUSE\n" + filler
    step = node("Install ClickHouse", Result.Status.FAIL, payload)
    result = job([step])

    named = result.info.split("\n", 1)[1]
    assert len(f"{step.name}: {payload}") > len(named), "payload must be oversized"
    assert named.startswith("Install ClickHouse: ")
    assert "FATAL_CAUSE" in named
    assert "trimmed" in named


def test_trim_bounds_a_pathological_step_info():
    result = job([node("Install ClickHouse", Result.Status.FAIL, "z" * 500_000)])
    assert len(result.info) < 34_000


def test_many_failing_cases_are_not_all_copied_into_the_job_row():
    """
    A pytest-style job (`ci/jobs/ci_tests_job.py`) has test cases as its direct
    children and no CIDB selector, so every case already has its own row. Naming all
    of them here would repeat those payloads in the job row and scale it with the
    failure count.
    """
    cases = [node(f"test_{i}", Result.Status.FAIL, "E" * 3000) for i in range(40)]
    result = job(cases)

    assert result.info.startswith("Failures: 40/40\n")
    assert len(result.info) < 32_768
    assert result.info.count("test_") <= 3


def test_the_first_failing_steps_are_named_in_order():
    steps = [
        node("Install ClickHouse", Result.Status.FAIL, "A_MARKER"),
        node("Start ClickHouse Server", Result.Status.FAIL, "B_MARKER"),
        node("Collect logs", Result.Status.FAIL, "C_MARKER"),
        node("Check errors", Result.Status.FAIL, "D_MARKER"),
    ]
    result = job(steps)

    assert result.info.splitlines()[0] == "Failures: 4/4"
    assert "Install ClickHouse: A_MARKER" in result.info
    assert "Start ClickHouse Server: B_MARKER" in result.info
    assert "Collect logs: C_MARKER" in result.info
    assert "D_MARKER" not in result.info


def test_the_named_step_reaches_the_cidb_job_row():
    """
    End-to-end through the real publisher: the summary must survive into the
    job-level row's `test_context_raw`, and it must not add or alter any test row.
    """
    import json
    import types

    import ci.praktika.cidb as cidb_module
    from ci.praktika.cidb import CIDB

    class FakeEnv:
        PR_NUMBER = 0
        SHA = "d6473dbb36b463ff3d5a46edfe76dae8eef0065b"
        COMMIT_URL = ""
        CHANGE_URL = ""
        BASE_BRANCH = "master"
        REPOSITORY = "ClickHouse/ClickHouse"
        BRANCH = "master"
        FORK_NAME = "ClickHouse/ClickHouse"
        INSTANCE_TYPE = "c6a.4xlarge"
        INSTANCE_LIFE_CYCLE = "spot"
        INSTANCE_ID = "i-0"

    class FakeInfo:
        def get_job_report_url(self, *a, **k):
            return "https://example.invalid/report"

        def get_job_url(self, *a, **k):
            return "https://example.invalid/job"

    saved_env, saved_info = cidb_module._Environment, cidb_module.Info
    cidb_module._Environment = types.SimpleNamespace(get=lambda: FakeEnv())
    cidb_module.Info = FakeInfo
    # Drive the production entry point, not the private formatter: `complete_job` is
    # what every job script calls, so losing the call there must fail this test.
    result = node(
        "Stateless tests (amd_debug, parallel)",
        Result.Status.FAIL,
        "",
        [node("Start ClickHouse Server", Result.Status.FAIL, SERVER_LOG), make_tests_stage()],
    )
    try:
        with pytest.raises(SystemExit):
            result.complete_job(disable_attached_files_sorting=True)
        rows = [
            json.loads(row)
            for row in CIDB.json_data_generator(result, result_name_for_cidb="Tests")
        ]
    finally:
        cidb_module._Environment, cidb_module.Info = saved_env, saved_info

    assert rows[0]["test_context_raw"] == result.info
    assert "Start ClickHouse Server: " in rows[0]["test_context_raw"]
    assert MARKER in rows[0]["test_context_raw"]
    # No pseudo test case, and the real child row is untouched.
    assert [r["test_name"] for r in rows] == ["", "00001_ok"]


def test_trim_keeps_a_cause_that_sits_in_the_dropped_span():
    """
    `from_commands_run` bounds line count, not line length, so its error-centered
    excerpt can put 50 long context lines ahead of the cause and push it past the kept
    head. A two-ended trim alone would drop exactly the error this summary publishes.
    """
    context = [("c%03d: " % i) + "x" * 400 for i in range(50)]
    after = [("a%03d: " % i) + "x" * 400 for i in range(249)]
    payload = "\n".join(
        ["~~~~~ truncated 900 lines at the beginning ~~~~~"]
        + context
        + ["src/Foo.cpp:12:3: error: THE_REAL_CAUSE"]
        + after
        + ["~~~~~ truncated 100 lines at the end ~~~~~"]
    )
    step = node("Start ClickHouse Server", Result.Status.FAIL, payload)
    assert f"{step.name}: {payload}".find("THE_REAL_CAUSE") > 16_384, "cause must fall past the kept head"

    result = job([step])
    named = result.info.split("\n", 1)[1]
    assert named.startswith("Start ClickHouse Server: ")
    assert "THE_REAL_CAUSE" in named
    assert "trimmed" in named


def test_a_failing_selected_node_own_info_is_named():
    """
    CIDB publishes the selected node's CHILDREN, never the node itself, so a failing
    `Tests` node's own command log reaches no row unless the summary names it.
    """
    result = job(
        [
            make_tests_stage(
                Result.Status.FAIL,
                "SELECTED_NODE_MARKER",
                [node("00001_foo", Result.Status.FAIL, "assert")],
            )
        ]
    )
    assert result.info == "Failures: 1/1\nTests: SELECTED_NODE_MARKER"
