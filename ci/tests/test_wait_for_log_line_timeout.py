"""
Tests that `ClickHouseInstance.wait_for_log_line` gives the container-side wait a python-side
budget that outlives it.

`wait_for_log_line(timeout=N)` interpolates N into a container-side shell command
(`timeout N ... tail | tee | grep`). That command runs through
`exec_in_container` -> `subprocess_check_call` -> `run_and_check`, and
`run_and_check`'s `timeout` parameter defaults to 300. If no python-side timeout is
forwarded, `subprocess.run` kills the `docker exec` at 300 s, so any caller asking for
more than 300 s silently gets 300 s and its own value never takes effect.

The invariant pinned here: for every `wait_for_log_line` call the container-side budget
expires strictly before the python-side one, so the pipeline can exit and the lines it
collected can be returned. The `repetitions > 1` branch reads those lines, so a
python-side kill (which raises instead of returning output) is not an equivalent outcome.

Static analysis rather than execution: `wait_for_log_line` needs a live container, while
the property under test is a property of the call it makes. `test_integration_test_name_quoting`
is the precedent for pinning a property of `tests/integration` code from `ci/tests`.

Related: ClickHouse/ClickHouse#114552, which raised a caller to `timeout=600`.
"""

import ast
import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
CLUSTER_PY = os.path.join(REPO_ROOT, "tests/integration/helpers/cluster.py")

# `run_and_check`'s default, i.e. the budget that applies when nothing is forwarded.
RUN_AND_CHECK_DEFAULT_TIMEOUT = 300

# Inner values spanning the callers that exist today (3 .. 600) plus the boundary at
# `run_and_check`'s default, where the unfixed code starts losing the caller's value.
SAMPLE_INNER_TIMEOUTS = [3, 30, 60, 300, 600]


def _find_function(source, class_name, func_name):
    """The `func_name` FunctionDef inside `class_name`. Scoped by class because
    `exec_in_container` and other names are defined on more than one class here."""
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef) and sub.name == func_name:
                    return sub
    raise AssertionError(f"{class_name}.{func_name} not found")


def _exec_in_container_call(func_node):
    """The `self.exec_in_container(...)` Call inside `func_node`."""
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "exec_in_container"
        ):
            return node
    raise AssertionError("no self.exec_in_container(...) call found")


def _timeout_keyword(call_node):
    for kw in call_node.keywords:
        if kw.arg == "timeout":
            return kw
    return None


def _param_names(func_node):
    return [a.arg for a in func_node.args.args] + [
        a.arg for a in func_node.args.kwonlyargs
    ]


def _current_source():
    with open(CLUSTER_PY) as f:
        return f.read()


def _prefix_source():
    """`cluster.py` as of HEAD, for the negative control.

    Read via `git show`, never `git stash`: `refs/stash` lives in a git store shared by
    many worktrees, so pushing or popping there mutates other checkouts.
    """
    res = subprocess.run(
        ["git", "show", "HEAD:tests/integration/helpers/cluster.py"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
        check=False,
    )
    if res.returncode != 0:
        pytest.skip(f"git show failed: {res.stderr.decode('utf-8', 'ignore')[:200]}")
    return res.stdout.decode("utf-8")


def _outer_timeout_for(source, inner):
    """Evaluate the forwarded `timeout=` expression with the function's own `timeout`
    parameter bound to `inner`. Returns None when nothing is forwarded."""
    func = _find_function(source, "ClickHouseInstance", "wait_for_log_line")
    call = _exec_in_container_call(func)
    kw = _timeout_keyword(call)
    if kw is None:
        return None
    names = {
        a.arg: d
        for a, d in zip(
            func.args.args[-len(func.args.defaults) :] if func.args.defaults else [],
            func.args.defaults,
        )
    }
    env = {n: ast.literal_eval(d) for n, d in names.items()}
    env["timeout"] = inner
    expr = ast.Expression(body=kw.value)
    return eval(compile(expr, "<forwarded-timeout>", "eval"), {"__builtins__": {}}, env)


# --- arm 1: the plumbing exists, and is derived from the caller's own value -----------


def test_forwards_a_python_timeout():
    func = _find_function(_current_source(), "ClickHouseInstance", "wait_for_log_line")
    call = _exec_in_container_call(func)
    kw = _timeout_keyword(call)
    assert kw is not None, (
        "wait_for_log_line must forward a `timeout=` keyword to exec_in_container, "
        f"otherwise run_and_check's default of {RUN_AND_CHECK_DEFAULT_TIMEOUT}s caps "
        "every caller"
    )


def test_forwarded_timeout_references_the_callers_value():
    """A hardcoded constant would satisfy the arm above while still capping some caller."""
    func = _find_function(_current_source(), "ClickHouseInstance", "wait_for_log_line")
    kw = _timeout_keyword(_exec_in_container_call(func))
    assert kw is not None
    referenced = {
        n.id for n in ast.walk(kw.value) if isinstance(n, ast.Name)
    }
    assert "timeout" in referenced, (
        "the forwarded timeout must be derived from the function's own `timeout` "
        f"parameter, got the constant expression {ast.dump(kw.value)}"
    )


# --- arm 2: the ordering invariant, which arm 1 alone does not pin --------------------


@pytest.mark.parametrize("inner", SAMPLE_INNER_TIMEOUTS)
def test_outer_budget_strictly_exceeds_inner(inner):
    """`timeout=timeout` would pass arm 1 and race here."""
    outer = _outer_timeout_for(_current_source(), inner)
    assert outer is not None
    assert outer > inner, (
        f"python-side budget {outer} must strictly exceed the container-side {inner} so "
        "the container's `timeout` fires first and grep's collected lines are returned"
    )


@pytest.mark.parametrize("inner", SAMPLE_INNER_TIMEOUTS)
def test_callers_above_the_default_are_no_longer_capped(inner):
    """The defect: an inner value above run_and_check's default was unreachable."""
    outer = _outer_timeout_for(_current_source(), inner)
    assert outer is not None
    if inner >= RUN_AND_CHECK_DEFAULT_TIMEOUT:
        assert outer > RUN_AND_CHECK_DEFAULT_TIMEOUT, (
            f"inner={inner} still capped at {RUN_AND_CHECK_DEFAULT_TIMEOUT}"
        )


# --- arm 3: negative control. These must FAIL against the pre-fix source -------------


def test_negative_control_prefix_source_lacks_the_forward():
    """Pins that the arms above discriminate. If the pre-fix source already forwarded a
    timeout, every arm above would pass on both trees and assert nothing."""
    prefix = _prefix_source()
    func = _find_function(prefix, "ClickHouseInstance", "wait_for_log_line")
    kw = _timeout_keyword(_exec_in_container_call(func))
    if kw is not None:
        pytest.skip("HEAD already carries the fix; control is not meaningful here")
    assert _outer_timeout_for(prefix, 600) is None


def test_negative_control_prefix_source_capped_large_callers():
    prefix = _prefix_source()
    if _outer_timeout_for(prefix, 600) is not None:
        pytest.skip("HEAD already carries the fix; control is not meaningful here")
    # With nothing forwarded the effective budget is run_and_check's default, so a
    # caller asking for more than that got less than it asked for.
    assert RUN_AND_CHECK_DEFAULT_TIMEOUT < 600


# --- arm 4: the container-side timeout must not be removed ---------------------------


def test_container_side_timeout_is_still_interpolated():
    """Dropping the inner `timeout` and relying on the python side alone would break the
    `repetitions > 1` branch, which reads the lines grep collected before exiting."""
    func = _find_function(_current_source(), "ClickHouseInstance", "wait_for_log_line")
    shell_strings = [
        ast.get_source_segment(_current_source(), n)
        for n in ast.walk(func)
        if isinstance(n, ast.JoinedStr)
    ]
    assert any(
        s and "timeout {timeout}" in s for s in shell_strings
    ), "the container-side `timeout {timeout}` interpolation must remain"


def test_use_cli_default_carries_the_kwarg():
    """`Cluster.exec_in_container` only consumes a `timeout` kwarg on its `use_cli=True`
    path; the `use_cli=False` path forwards **kwargs to docker-py, which has no such
    parameter. `wait_for_log_line` never sets `use_cli`, so it must default to True."""
    source = _current_source()
    cluster_exec = _find_function(source, "ClickHouseCluster", "exec_in_container")
    names = _param_names(cluster_exec)
    assert "use_cli" in names
    defaults = dict(
        zip(
            names[len(names) - len(cluster_exec.args.defaults) :],
            [ast.literal_eval(d) for d in cluster_exec.args.defaults],
        )
    )
    assert defaults["use_cli"] is True

    wait = _find_function(source, "ClickHouseInstance", "wait_for_log_line")
    call = _exec_in_container_call(wait)
    assert _timeout_keyword(call) is not None
    assert not any(
        kw.arg == "use_cli" for kw in call.keywords
    ), "wait_for_log_line must not pin use_cli; the timeout kwarg needs the CLI path"
