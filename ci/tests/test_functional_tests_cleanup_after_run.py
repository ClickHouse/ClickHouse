"""
Regression test: the stateless job reaps escaped tests after every test run.

`clickhouse-test --cleanup` kills the process groups recorded by the per-worker
group pid files. It was wired only into `ClickHouseProc.run_test` (fast test), so
the stateless job never ran it - tests that escaped into their own session kept
consuming the machine and querying the server while the job tore down and
collected logs.

`run_tests` now calls it in a `finally`, which is what makes it cover every
invocation: bugfix validation calls `run_tests` once per build type, so hanging
the call off the first call site would silently skip the later runs. These tests
assert both the presence and that placement, including when the run raises.
"""

import ast
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FUNCTIONAL_TESTS = _REPO_ROOT / "ci" / "jobs" / "functional_tests.py"

sys.path.insert(0, str(_REPO_ROOT))

CLEANUP_COMMAND = "clickhouse-test --cleanup"


def _run_tests_node():
    tree = ast.parse(_FUNCTIONAL_TESTS.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "run_tests":
            return node
    raise AssertionError("run_tests not found in functional_tests.py")


def _cleanup_literals(node):
    return [
        literal.value
        for literal in ast.walk(node)
        if isinstance(literal, ast.Constant)
        and isinstance(literal.value, str)
        and CLEANUP_COMMAND in literal.value
    ]


def test_run_tests_invokes_cleanup():
    assert _cleanup_literals(_run_tests_node()), (
        f"run_tests must invoke `{CLEANUP_COMMAND}` so escaped tests are reaped"
    )


def test_cleanup_is_in_a_finally_so_it_covers_every_invocation():
    """Placement, not just presence.

    `run_tests` is called once per build type by bugfix validation. Only a
    `finally` around the run covers all of them, and it still runs when the test
    run raises.
    """
    node = _run_tests_node()
    in_finally = [
        literal
        for trier in ast.walk(node)
        if isinstance(trier, ast.Try)
        for stmt in trier.finalbody
        for literal in _cleanup_literals(stmt)
    ]
    assert in_finally, (
        f"`{CLEANUP_COMMAND}` must sit in a `finally` inside run_tests, so it "
        "covers every invocation and also runs when the test run raises"
    )


def test_cleanup_runs_for_every_run_tests_call(monkeypatch, tmp_path):
    """Drive the real function twice, including one raising run."""
    from ci.jobs import functional_tests

    calls = []
    monkeypatch.setattr(
        functional_tests.Shell, "check", lambda cmd, **kw: calls.append(cmd) or True
    )
    monkeypatch.setattr(functional_tests, "temp_dir", str(tmp_path))
    monkeypatch.setattr(
        functional_tests, "stateless_memory_limit", lambda source: 1, raising=False
    )
    monkeypatch.setattr(
        functional_tests.Info, "__init__", lambda self: None, raising=False
    )
    monkeypatch.setattr(
        functional_tests.Info, "job_name", "amd_asan_ubsan", raising=False
    )

    monkeypatch.setattr(functional_tests.Shell, "run", lambda *a, **kw: 0)
    assert functional_tests.run_tests(0, 0, build_type="amd_debug") == 0

    def boom(*_args, **_kwargs):
        raise RuntimeError("test run died")

    monkeypatch.setattr(functional_tests.Shell, "run", boom)
    try:
        functional_tests.run_tests(0, 0, build_type="amd_asan_ubsan")
    except RuntimeError:
        pass

    cleanups = [cmd for cmd in calls if CLEANUP_COMMAND in cmd]
    assert len(cleanups) == 2, (
        f"expected a cleanup per run_tests call (incl. the raising one), got {calls}"
    )
