"""
Tests for the CIDB job-level row's `test_context_raw`.

A praktika job publishes one job-level row plus one row per child of the node named
by `result_name_for_cidb`. A step that fails outside that node - `Install ClickHouse`,
`Start ClickHouse Server`, `Collect logs` - is therefore reachable from no row at all,
and the job-level row carries only `Failures: N/M`, so no CIDB query can see the cause.
`_job_test_context` names those steps on the job-level row.

Two shapes make the containment clause load-bearing rather than defensive: `ci/jobs/
fast_test.py` copies the `Tests` node's info onto the root without clearing it, and
`Result.create_from(with_info_from_results=True)` already lifts child info as
`"<name>: <info>"`. Both would double the same text without it.

The nested case stays uncollected on purpose: only direct children are read, because
`Result.results` has no cardinality bound. `test_nested_failure_under_unpublished_child`
pins that boundary.
"""

import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.praktika.cidb as cidb_module
from ci.praktika.cidb import CIDB
from ci.praktika.result import Result

MARKER = "Poco::Exception. Code: 1000, e.code() = 111, Connection refused"
# `Result.from_commands_run` truncates two ways (`ci/praktika/result.py`). Its default
# branch emits a leading marker plus the last 300 lines, so 301 lines - matching real
# master payloads at 301 lines / ~26 KB. Both fixtures match the line and the byte axis,
# so a line clip or a byte clip of the appended text cannot stay green.
_SERVER_LOG_BODY = [f"srvlog{i}: " + "x" * 76 for i in range(1, 300)]
SERVER_LOG = "\n".join(["~~~~~ truncated 912 lines ~~~~~", *_SERVER_LOG_BODY, MARKER])
# Its error-centered branch wraps 300 retained lines in BOTH a leading and a trailing
# marker, so 302 is the real ceiling and the cause is not last. Reached by any step whose
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


class FakeEnv:
    PR_NUMBER = 0
    SHA = "d6473dbb36b463ff3d5a46edfe76dae8eef0065b"
    COMMIT_URL = "https://github.com/ClickHouse/ClickHouse/commit/d6473dbb36b4"
    CHANGE_URL = ""
    BASE_BRANCH = "master"
    REPOSITORY = "ClickHouse/ClickHouse"
    BRANCH = "master"
    FORK_NAME = "ClickHouse/ClickHouse"
    INSTANCE_TYPE = "c6a.4xlarge"
    INSTANCE_LIFE_CYCLE = "spot"
    INSTANCE_ID = "i-0123456789abcdef0"


class FakeInfo:
    def get_job_report_url(self, *args, **kwargs):
        return "https://example.invalid/report"

    def get_job_url(self, *args, **kwargs):
        return "https://example.invalid/job"


@pytest.fixture(autouse=True)
def offline_env(monkeypatch):
    monkeypatch.setattr(
        cidb_module, "_Environment", types.SimpleNamespace(get=lambda: FakeEnv())
    )
    monkeypatch.setattr(cidb_module, "Info", FakeInfo)


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
    """A job result shaped like `functional_tests.py` builds one."""
    result = node(
        "Stateless tests (amd_debug, parallel)", Result.Status.FAIL, info, children
    )
    if not info:
        result._add_job_summary_to_info()
    return result


def rows(result, result_name_for_cidb="Tests"):
    return [
        json.loads(row)
        for row in CIDB.json_data_generator(result, result_name_for_cidb)
    ]


def published_node(status=Result.Status.OK, info="", cases=None):
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

    Both statuses are read, matching `_add_job_summary_to_info`'s own `is_ok()`
    predicate: `ci/jobs/performance_tests.py` appends a direct `Download datasets`
    child with `status=ERROR` under `result_name_for_cidb="Tests"`.
    """
    result = job(
        [
            node("Install ClickHouse", Result.Status.OK),
            node("Start ClickHouse Server", step_status, SERVER_LOG),
            node("Collect logs", Result.Status.OK),
        ]
    )
    assert result.info == "Failures: 1/3"

    published = rows(result)
    context = published[0]["test_context_raw"]
    assert "Start ClickHouse Server:" in context
    assert MARKER in context
    # No new rows: per-test statistics must not gain a pseudo test case.
    assert [r["test_name"] for r in published] == [""]


def test_two_failing_steps_are_both_named():
    result = job(
        [
            node("Install ClickHouse", Result.Status.FAIL, "INSTALL_MARKER"),
            node("Collect logs", Result.Status.FAIL, "LOGS_MARKER"),
            published_node(),
        ]
    )
    context = rows(result)[0]["test_context_raw"]
    assert "Install ClickHouse: INSTALL_MARKER" in context
    assert "Collect logs: LOGS_MARKER" in context


@pytest.mark.parametrize("job_info", ["", "3 tests failed"])
def test_existing_info_stays_a_prefix(job_info):
    """`test_context_raw` queries written against the old value keep matching."""
    result = job(
        [
            published_node(
                Result.Status.FAIL,
                cases=[node("00001_foo", Result.Status.FAIL, "assert")],
            ),
            node("Collect logs", Result.Status.FAIL, "LOGS_MARKER"),
        ],
        info=job_info,
    )
    assert result.info
    context = rows(result)[0]["test_context_raw"]
    assert context.startswith(result.info)
    assert "LOGS_MARKER" in context


def test_empty_result_name_publishes_every_child_already():
    """`result_name_for_cidb=""` selects the job itself, so every child has its own row."""
    result = job([node("00001_foo", Result.Status.FAIL, "assert MARKER")])
    published = rows(result, result_name_for_cidb="")
    assert published[0]["test_context_raw"] == result.info
    assert [r["test_name"] for r in published] == ["", "00001_foo"]


def test_all_children_ok():
    result = job([node("Install ClickHouse", Result.Status.OK), published_node()])
    assert rows(result)[0]["test_context_raw"] == result.info


def test_failing_test_inside_published_node():
    """An ordinary failing test carries its own info on its own row."""
    result = job(
        [
            published_node(
                Result.Status.FAIL,
                cases=[node("00001_foo", Result.Status.FAIL, "assert MARKER")],
            )
        ]
    )
    published = rows(result)
    assert published[0]["test_context_raw"] == result.info
    assert published[1]["test_context_raw"] == "assert MARKER"


def test_selected_node_own_info_is_named():
    """
    The selected node is never in its own published set: CIDB publishes its CHILDREN.
    `Result.from_commands_run` fills a failing node's own info (`with_info_on_failure`
    defaults to True), so a failing `Tests` node's command log reaches no row without
    this. The root info here is a different string, so containment does not apply.
    """
    result = job(
        [
            published_node(
                Result.Status.FAIL,
                "SELECTED_NODE_MARKER",
                [node("00001_foo", Result.Status.FAIL, "assert")],
            ),
            node("Report", Result.Status.OK),
        ],
        info="4 slower, 0 unstable",
    )
    published = rows(result)
    assert (
        published[0]["test_context_raw"]
        == "4 slower, 0 unstable\nTests: SELECTED_NODE_MARKER"
    )
    # Child rows are untouched: no pseudo test case, no changed context.
    assert [r["test_name"] for r in published] == ["", "00001_foo"]
    assert published[1]["test_context_raw"] == "assert"


def test_skipped_step_is_not_a_failure():
    """`is_ok()` accepts SKIPPED, matching `_add_job_summary_to_info`'s own predicate."""
    result = job(
        [
            node("Diagnostics", Result.Status.SKIPPED, "Too many failed tests"),
            published_node(
                Result.Status.FAIL,
                cases=[node("00001_foo", Result.Status.FAIL, "assert")],
            ),
        ]
    )
    assert rows(result)[0]["test_context_raw"] == result.info


def test_failing_step_with_empty_info():
    """A failing step with no info of its own adds nothing (the `r.info` guard)."""
    result = job(
        [
            published_node(
                Result.Status.FAIL,
                cases=[node("00001_foo", Result.Status.FAIL, "assert")],
            ),
            node("Check errors", Result.Status.FAIL),
        ]
    )
    assert rows(result)[0]["test_context_raw"] == result.info


@pytest.mark.parametrize(
    "payload, line_count",
    [(SERVER_LOG, 301), (ERROR_CENTERED_LOG, 302)],
    ids=["default-branch", "error-centered"],
)
def test_large_payload_is_appended_verbatim(payload, line_count):
    """
    `Result.from_commands_run` already caps its info; do not cap again. Its default
    branch emits a marker plus the last 300 lines, its error-centered branch wraps 300
    retained lines in two markers. Both shapes are ~26 KB and the cause can sit anywhere
    in them, so each one's own line count and byte size are pinned.
    """
    step = node("Start ClickHouse Server", Result.Status.FAIL, payload)
    result = job([node("Install ClickHouse", Result.Status.OK), step])
    context = rows(result)[0]["test_context_raw"]

    appended = context[len(result.info) + 1 :]
    assert appended == f"{step.name}: {step.info}"
    assert len(appended.splitlines()) == len(step.info.splitlines()) == line_count
    assert len(appended) > 20_000
    assert MARKER in appended


def test_nested_failure_under_unpublished_child():
    """
    Known limitation, out of scope here: `Keeper Stress` names a `result_name_for_cidb`
    no child ever carries (`ci/defs/job_configs.py` vs `ci/jobs/keeper_stress_job.py`),
    so its pytest cases reach no row and their parent's own info is empty. Only direct
    children are read, so this stays uncollected. Fixing it means renaming the nodes.
    """
    result = job(
        [
            node(
                "Keeper Stress (with-faults)",
                Result.Status.FAIL,
                results=[
                    node("test_scenario[prod-mix]", Result.Status.FAIL, "KEEPER_MARKER")
                ],
            ),
            node("Post Hooks", Result.Status.OK),
        ]
    )
    published = rows(result, result_name_for_cidb="Keeper Stress")
    assert published[0]["test_context_raw"] == result.info
    assert "KEEPER_MARKER" not in published[0]["test_context_raw"]
    assert [r["test_name"] for r in published] == [""]


def test_named_node_info_copied_to_root_is_not_repeated():
    """
    `ci/jobs/fast_test.py` copies the `Tests` node's info onto the root without
    clearing it, and the named node is never in its own published set.
    """
    result = job(
        [
            published_node(
                Result.Status.FAIL,
                FAST_TEST_INFO,
                [node("00001_foo", Result.Status.FAIL, "assert")],
            )
        ],
        info=FAST_TEST_INFO,
    )
    context = rows(result)[0]["test_context_raw"]
    assert context == result.info
    assert context.count("00001_foo FAIL") == 1


def test_lifted_child_info_is_not_repeated():
    """
    `Result.create_from(with_info_from_results=True)` already writes `"<name>: <info>"`
    onto the parent; `ci/jobs/unit_tests_bugfix_validation_job.py` uses it under a
    `result_name_for_cidb` that matches no child.
    """
    result = job(
        [node("Unit tests", Result.Status.FAIL, MARKER)], info=f"Unit tests: {MARKER}"
    )
    context = rows(result)[0]["test_context_raw"]
    assert context == result.info
    assert context.count(MARKER) == 1
