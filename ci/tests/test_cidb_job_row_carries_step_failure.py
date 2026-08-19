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
SERVER_LOG = "\n".join(f"srvlog{i}" for i in range(300) if i) + "\n" + MARKER
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


def test_failing_step_outside_tests_node_is_named():
    """The real master shape: `Start ClickHouse Server` dies before the test phase."""
    result = job(
        [
            node("Install ClickHouse", Result.Status.OK),
            node("Start ClickHouse Server", Result.Status.FAIL, SERVER_LOG),
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
    """`Check errors` re-parents its children into the Tests node and empties itself."""
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


def test_large_payload_is_appended_verbatim():
    """`Result.from_commands_run` already caps its info at 300 lines; do not cap again."""
    step = node("Start ClickHouse Server", Result.Status.FAIL, SERVER_LOG)
    result = job([node("Install ClickHouse", Result.Status.OK), step])
    context = rows(result)[0]["test_context_raw"]

    appended = context[len(result.info) + 1 :]
    assert appended == f"{step.name}: {step.info}"
    assert len(appended.splitlines()) == len(step.info.splitlines())
    assert context.endswith(MARKER)


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
