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
    assert named.startswith(f"{step.name}: ")
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
