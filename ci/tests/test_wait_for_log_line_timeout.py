"""
Tests the two budget invariants of `ClickHouseInstance.wait_for_log_line`.

`wait_for_log_line(timeout=N)` interpolates N into a container-side shell command
(`timeout N ... tail | tee | grep`), which runs through `exec_in_container` ->
`subprocess_check_call` -> `run_and_check`. The python-side budget must therefore
(1) outlive the container-side one, so the pipeline can exit and the lines it collected
can be returned -- the `repetitions > 1` branch reads those lines, and a python-side kill
raises instead of returning output -- and (2) never fall below `run_and_check`'s own
default, which is what a command that forwards nothing already gets.

Static analysis rather than execution: `wait_for_log_line` needs a live container, while
the property under test is a property of the call it makes. `test_integration_test_name_quoting`
is the precedent for pinning a property of `tests/integration` code from `ci/tests`.
"""

import ast
import os

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


# `wait_for_log_line` as it stood before this change, inlined verbatim rather than read
# out of git: in PR CI the checkout already carries the change, so a control derived from
# the working tree or from HEAD compares the new code against itself and asserts nothing.
PREFIX_SOURCE = '''
class ClickHouseInstance:
    def wait_for_log_line(
        self,
        regexp,
        filename="/var/log/clickhouse-server/clickhouse-server.log",
        timeout=30,
        repetitions=1,
        look_behind_lines=10000,
    ):
        start_time = time.time()
        result = self.exec_in_container(
            [
                "bash",
                "-c",
                f"timeout {timeout} stdbuf -o0 -e0 tail -Fn{look_behind_lines} {shlex.quote(filename)} | stdbuf -o0 -e0 tee -a {filename}.wait_for_log_line | grep -Em {repetitions} {shlex.quote(regexp)}",
            ]
        )
'''


def _prefix_source():
    """`cluster.py`'s `wait_for_log_line` before this change, from the copy above."""
    return PREFIX_SOURCE


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
    # Module-level names the expression may reference, resolved from the same source so
    # a renamed or re-valued constant cannot silently keep this passing.
    env.update(_module_int_constants(source))
    expr = ast.Expression(body=kw.value)
    return eval(
        compile(expr, "<forwarded-timeout>", "eval"), {"__builtins__": {"max": max}}, env
    )


def _module_int_constants(source):
    """Module-level `NAME = <int>` assignments, e.g. `RUN_AND_CHECK_DEFAULT_TIMEOUT`."""
    out = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, int):
                    out[target.id] = node.value.value
    return out


def test_run_and_check_default_matches_the_constant_pinned_here():
    """`RUN_AND_CHECK_DEFAULT_TIMEOUT` below is a copy of the suite's own default; if the
    two drift, every budget assertion in this file is measured against the wrong number."""
    source = _current_source()
    run_and_check = next(
        n
        for n in ast.parse(source).body
        if isinstance(n, ast.FunctionDef) and n.name == "run_and_check"
    )
    names = [a.arg for a in run_and_check.args.args]
    defaults = dict(
        zip(names[len(names) - len(run_and_check.args.defaults) :], run_and_check.args.defaults)
    )
    kw = defaults["timeout"]
    if isinstance(kw, ast.Constant):
        actual = kw.value
    else:
        actual = _module_int_constants(source)[kw.id]
    assert actual == RUN_AND_CHECK_DEFAULT_TIMEOUT


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


@pytest.mark.parametrize("inner", SAMPLE_INNER_TIMEOUTS)
def test_outer_budget_is_never_shorter_than_the_suite_default(inner):
    """No caller may end up with less python-side patience than a command that forwards
    nothing at all. The container-side `timeout` signals only its direct child, while
    `docker exec` returns only once the whole pipeline has exited, so a short inner value
    does not bound the outer wait and must not shorten its budget."""
    outer = _outer_timeout_for(_current_source(), inner)
    assert outer is not None
    assert outer >= RUN_AND_CHECK_DEFAULT_TIMEOUT, (
        f"inner={inner} yields {outer}, below the suite-wide default of "
        f"{RUN_AND_CHECK_DEFAULT_TIMEOUT}"
    )


# --- arm 3: negative control. These must FAIL against the pre-fix source -------------


def test_negative_control_prefix_source_lacks_the_forward():
    """Pins that the arms above discriminate. If the pre-fix source already forwarded a
    timeout, every arm above would pass on both trees and assert nothing."""
    prefix = _prefix_source()
    func = _find_function(prefix, "ClickHouseInstance", "wait_for_log_line")
    kw = _timeout_keyword(_exec_in_container_call(func))
    assert kw is None, "the pre-fix copy must not forward a timeout"
    assert _outer_timeout_for(prefix, 600) is None


def test_negative_control_prefix_source_capped_large_callers():
    prefix = _prefix_source()
    assert _outer_timeout_for(prefix, 600) is None
    # With nothing forwarded the effective budget is run_and_check's default, so a
    # caller asking for more than that got less than it asked for.
    assert RUN_AND_CHECK_DEFAULT_TIMEOUT < 600


def test_prefix_copy_is_faithful_to_the_shipped_container_side_command():
    """The inlined pre-fix copy is only a control if it differs from the current source in
    exactly the forwarded kwarg. Compares the container-side command, which the change
    does not touch: a copy that had drifted there would be a different function."""
    shell_of = lambda src: [
        seg
        for n in ast.walk(_find_function(src, "ClickHouseInstance", "wait_for_log_line"))
        if isinstance(n, ast.JoinedStr)
        for seg in [ast.get_source_segment(src, n)]
        if seg
    ]
    assert shell_of(_prefix_source()) == shell_of(_current_source())


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
