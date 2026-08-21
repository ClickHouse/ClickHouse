"""Tests that a failed log-export configuration does not fail the job that owns it.

Log export is telemetry: `start_log_exports` guards on `log_export_host`, and a job
whose export was never configured runs its full suite (the llvm-coverage stateless
configs skip the step outright and are continuously green). The *start* step already
reports a lapse as a workflow warning, but the *configure* step used to propagate the
SSM fetch exception, so a transient `aws ssm get-parameters` failure - 502 Bad Gateway,
connect timeout, or absent instance credentials - failed `Install ClickHouse` and the
Stateless job aborted having run zero tests. These cells pin that each telemetry call
site tolerates the failure and records a warning, and that the tolerance did not widen
to anything else.

The real production code runs against a fake `aws` on `PATH`, so the actual subprocess
and the real `Secret` fetch are exercised. The call sites are AST-extracted from the job
scripts and EXECUTED, never pattern-matched: importing those modules would run whole
jobs, and matching their text would accept a handler that can never run.
"""

import ast
import os
import stat
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `sqlstorm_test` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_CI = os.path.join(_REPO_ROOT, "ci")
_FUNCTIONAL_TESTS = os.path.join(_CI, "jobs/functional_tests.py")
_CLICKBENCH = os.path.join(_CI, "jobs/clickbench.py")
_SQLSTORM = os.path.join(_CI, "jobs/sqlstorm_test.py")

# The three shapes observed in CI, with the AWS CLI's real exit code and stderr.
_AWS_FAILURES = [
    (
        254,
        "aws: [ERROR]: An error occurred (502) when calling the GetParameters "
        "operation (reached max retries: 2): Bad Gateway",
    ),
    (
        255,
        "aws: [ERROR]: Connect timeout on endpoint URL: "
        '"https://ssm.us-east-1.amazonaws.com/"',
    ),
    (
        253,
        "aws: [ERROR]: An error occurred (NoCredentials): Unable to locate credentials.",
    ),
]


class _FakeEnv:
    """Records workflow warnings the way `_Environment` accumulates them.

    Same shape as the stand-in `test_job_image_pull_retry.py` installs. Used instead of
    the real environment file so a cell can assert the stored message, and so a test run
    never writes into the job environment on disk.
    """

    JOB_NAME = "Stateless tests (amd_asan_ubsan)"
    WORKFLOW_CONFIG = None
    # `Info.is_local_run` reads this; a local run skips the step entirely, so every
    # cell here has to look like CI.
    LOCAL_RUN = False

    def __init__(self):
        self.warnings = []

    def dump(self):
        pass

    def add_workflow_warning(self, message, source=""):
        self.warnings.append(message)


@pytest.fixture
def env(monkeypatch):
    """The real `Info` over a recording environment; returns the recorder."""
    import ci.praktika._environment as environment_module

    fake = _FakeEnv()
    monkeypatch.setattr(
        environment_module._Environment, "get", staticmethod(lambda: fake)
    )
    return fake


@pytest.fixture
def fake_aws(tmp_path, monkeypatch):
    """Install a fake `aws` on PATH; returns an installer and an invocation counter.

    The counter is what tells "the fetch was attempted and tolerated" from "the fetch was
    never reached": a cell asserting only that nothing raised is equally satisfied by a
    call site that silently stopped configuring log export at all.
    """
    counter = tmp_path / "invocations"

    def _write(body):
        script = tmp_path / "aws"
        script.write_text(f'#!/bin/bash\necho x >> "{counter}"\n{body}')
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

    def install(exit_code, stderr):
        _write(f"printf '%s\\n' {stderr!r} >&2\nexit {exit_code}\n")

    def succeed_with(host, password):
        """Emit the `Name\\tValue` pairs the real `--query 'Parameters[*].[Name,Value]'`
        produces, so the success path runs the real parsing rather than a shortcut."""
        _write(
            f"printf 'clickhouse_ci_logs_host\\t{host}\\n"
            f"clickhouse_ci_logs_password\\t{password}\\n'\n"
        )

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # `create_log_export_config` copies a repo-relative config file.
    monkeypatch.chdir(_REPO_ROOT)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    install.invocations = invocations
    install.succeed_with = succeed_with
    # The default shape, so a cell that does not parametrise still fails realistically.
    install(*_AWS_FAILURES[0])
    return install


def _extract_function(path, name, args):
    """Compile the definition of `name` taking exactly the parameters `args`, from `path`.

    The module is read, not imported: importing a job script runs the job. The definition
    is then executed, so a handler nested under a condition that can never hold, or one
    whose body was gutted, fails here rather than passing a text match.

    `args` disambiguates same-named definitions - `sqlstorm_test.py` has both the
    `ClickHouseBinary.start(self)` method and the local `start()` closure - so a rename or
    a signature change surfaces as a failure here rather than silently selecting the other.
    """
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == name
        and [a.arg for a in node.args.args] == list(args)
    ]
    assert (
        len(matches) == 1
    ), f"expected exactly one `{name}({', '.join(args)})` in {path}, got {len(matches)}"
    return compile(ast.Module(body=[matches[0]], type_ignores=[]), path, "exec")


def _define(path, name, namespace, args=()):
    """Execute the extracted definition of `name` in `namespace` and return the callable.

    `namespace` doubles as the function's globals, so its free variables (`CH`, `ch`,
    `info`, ...) resolve to the stand-ins the caller put there.
    """
    exec(  # noqa: S102 - the definition comes from this repo's own job script
        _extract_function(path, name, args), namespace
    )
    return namespace[name]


# --- the callee's contract, which the call sites rely on ----------------------


@pytest.mark.parametrize("exit_code,stderr", _AWS_FAILURES)
def test_the_fetch_failure_reaches_the_caller_as_an_exception(
    fake_aws, tmp_path, exit_code, stderr
):
    """r2-1. This fix does not change the callee, so pin what it does for all three
    observed shapes: it raises, leaves no host, and writes no config."""
    fake_aws(exit_code, stderr)
    config_dir = tmp_path / "etc"
    ch = ClickHouseProc()
    with pytest.raises(Exception) as excinfo:
        ch.create_log_export_config(config_dir=str(config_dir))
    assert f"exit_code {exit_code}" in str(excinfo.value)
    assert ch.log_export_host is None
    assert not (config_dir / "config.d" / "system_logs_export.yaml").exists()


def test_a_tolerated_failure_leaves_no_half_written_config(fake_aws, tmp_path):
    """r2-2. The directory is created before the fetch, so what survives must be an empty
    one - a partial `system_logs_export.yaml` would make the server fail to start."""
    config_dir = tmp_path / "etc"
    with pytest.raises(Exception):
        ClickHouseProc().create_log_export_config(config_dir=str(config_dir))
    assert [p.name for p in config_dir.iterdir()] == ["config.d"]
    assert list((config_dir / "config.d").iterdir()) == []


# --- Install ClickHouse survives it (the defect) ------------------------------


def _functional_tests_closure(env, config_dir):
    """The production `configure_log_export` closure, with its two free variables bound."""
    ch = ClickHouseProc()
    namespace = {
        "info": Info(),
        # The closure calls `create_log_export_config()` with no argument; redirect the
        # write into the test's directory without touching the code under test.
        "CH": type(
            "_CH",
            (),
            {
                "create_log_export_config": lambda _self: ch.create_log_export_config(
                    config_dir=config_dir
                )
            },
        )(),
    }
    return _define(_FUNCTIONAL_TESTS, "configure_log_export", namespace)


def _clickbench_hook(ch):
    """The production ClickBench config hook, with its two free variables bound."""
    return _define(
        _CLICKBENCH,
        "configure_log_export",
        {"info": Info(), "ch": ch},
        args=("config_dir", "var_lib_dir"),
    )


def test_install_clickhouse_survives_a_failed_log_export_fetch(fake_aws, env, tmp_path):
    """r2-3. The claim of the whole change, through the production result machinery: a
    telemetry blip must not abort the job before a single test runs."""
    closure = _functional_tests_closure(env, str(tmp_path / "etc"))
    result = Result.from_commands_run(
        name="Install ClickHouse", command=["true", closure]
    )
    assert result.is_ok(), result.info
    # The fetch was really attempted, so the cell is not passing because the step
    # stopped configuring log export altogether.
    assert fake_aws.invocations() == 1


def test_without_the_tolerance_install_clickhouse_fails(fake_aws, tmp_path):
    """r2-4. Negative control. The same fetch failure, reported the way it was before:
    if this passes, the oracle above cannot redden and pins nothing."""
    ch = ClickHouseProc()

    def untolerated():
        return ch.create_log_export_config(config_dir=str(tmp_path / "etc"))

    result = Result.from_commands_run(
        name="Install ClickHouse", command=["true", untolerated]
    )
    assert not result.is_ok()


def test_returning_false_would_not_be_fail_open(fake_aws, tmp_path):
    """The reason the tolerance returns truthy rather than `False`: `from_commands_run`
    treats a `False` return as a failed command, so a "fail-open" `return False` would
    keep the job dead while looking tolerant."""
    assert not Result.from_commands_run(
        name="Install ClickHouse", command=["true", lambda: False]
    ).is_ok()
    assert Result.from_commands_run(
        name="Install ClickHouse", command=["true", lambda: None]
    ).is_ok()


def test_the_lapse_is_recorded_as_a_workflow_warning(fake_aws, env, tmp_path):
    """r2-8. Since the tolerance does not classify the cause, the warning is the only
    thing standing between a real SSM misconfiguration and silence. Assert the stored
    message, not merely that nothing escaped."""
    closure = _functional_tests_closure(env, str(tmp_path / "etc"))
    closure()
    assert len(env.warnings) == 1
    assert "log export" in env.warnings[0]
    # The cause has to survive into the message, or the warning cannot be acted on.
    assert "502" in env.warnings[0]


def test_the_tolerance_did_not_widen_to_the_rest_of_the_step(fake_aws, env, tmp_path):
    """r2-9. A real failure elsewhere in the same command list must still fail the step:
    the catch is scoped to the telemetry call, not draped over the install."""
    closure = _functional_tests_closure(env, str(tmp_path / "etc"))
    result = Result.from_commands_run(
        name="Install ClickHouse", command=[closure, "false"]
    )
    assert not result.is_ok()


def test_a_successful_fetch_is_unaffected(fake_aws, env, tmp_path):
    """The happy path still writes the config and records no warning: the `try` wraps the
    call, so it must be transparent when the call succeeds."""
    host = "ci-logs.example.com"
    fake_aws.succeed_with(host=host, password="secret")

    config_dir = tmp_path / "etc"
    closure = _functional_tests_closure(env, str(config_dir))
    assert closure()
    written = (config_dir / "config.d" / "system_logs_export.yaml").read_text()
    assert host in written
    assert env.warnings == []


# --- production wiring: the closure is in the mandatory command list ----------


def _appends_of_closure(node):
    """The names of the lists `node` appends `configure_log_export` to."""
    return [
        ast.unparse(inner.func)[: -len(".append")]
        for inner in ast.walk(node)
        if isinstance(inner, ast.Call)
        and ast.unparse(inner.func).endswith(".append")
        and any(
            isinstance(arg, ast.Name) and arg.id == "configure_log_export"
            for arg in inner.args
        )
    ]


def _install_stage_guard():
    """The innermost `if` guarding the append, plus the enclosing install-stage block.

    The guard is EXECUTED below rather than matched: one amended with `and False`, or an
    append moved under a condition that never holds, keeps every substring while never
    running in CI. The enclosing block is returned too, because the guard alone does not
    say which command list the closure joins - an append into a list nobody runs would
    satisfy the execution check.
    """
    with open(_FUNCTIONAL_TESTS, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_FUNCTIONAL_TESTS)

    guards = [
        n for n in ast.walk(tree) if isinstance(n, ast.If) and _appends_of_closure(n)
    ]
    assert guards, "no guard appends configure_log_export to any command list"
    # Innermost: every enclosing guard also contains the append, and executing an outer
    # one would need the whole install stage's locals.
    innermost = [
        g for g in guards if not any(g is not o and o in ast.walk(g) for o in guards)
    ]
    assert len(innermost) == 1, f"expected one innermost guard, got {len(innermost)}"
    targets = _appends_of_closure(innermost[0])
    assert len(targets) == 1, f"expected one append target, got {targets}"
    enclosing = [g for g in guards if g is not innermost[0]]
    return innermost[0], enclosing, targets[0]


@pytest.mark.parametrize("is_llvm_coverage,expected", [(False, True), (True, False)])
def test_the_closure_is_appended_to_the_install_commands(is_llvm_coverage, expected):
    """r2-10. Every cell above calls the closure directly, so without this one deleting
    the append would leave the file green. Both directions are asserted, so a guard that
    always ran would fail here rather than pass for the wrong reason."""
    statement, _, target = _install_stage_guard()
    appended = []
    namespace = {
        target: appended,
        "configure_log_export": lambda: True,
        "is_llvm_coverage": is_llvm_coverage,
    }
    exec(  # noqa: S102 - the statement comes from this repo's own job script
        compile(
            ast.Module(body=[statement], type_ignores=[]), _FUNCTIONAL_TESTS, "exec"
        ),
        namespace,
    )
    assert bool(appended) is expected


def test_the_appended_closure_lands_in_the_install_result_command_list():
    """The append's target must be the very list handed to `Result.from_commands_run(name=
    "Install ClickHouse", ...)`. Without this, the cell above is satisfied by an append
    into some other list that no result ever runs - the closure would be tolerant and also
    dead, and log export would silently stop being configured at all."""
    _, enclosing, target = _install_stage_guard()
    assert enclosing, "the append is not nested inside the install-stage block"
    outermost = max(enclosing, key=lambda g: len(list(ast.walk(g))))
    runs = [
        node
        for node in ast.walk(outermost)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func).endswith("Result.from_commands_run")
        and any(
            kw.arg == "name"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value == "Install ClickHouse"
            for kw in node.keywords
        )
    ]
    assert (
        len(runs) == 1
    ), "the install-stage block does not run an Install ClickHouse result"
    command = [kw.value for kw in runs[0].keywords if kw.arg == "command"]
    assert command, "the Install ClickHouse result takes no `command`"
    assert ast.unparse(command[0]) == target


# --- the other two telemetry call sites --------------------------------------


def test_clickbench_config_hook_survives_and_does_not_trip_the_hook_gate(
    fake_aws, env, tmp_path
):
    """r2-5. ClickBench fails by a second mechanism: the hook runs inside
    `ClickHouseService.__enter__`, which raises both when a hook raises and when it
    returns exactly `False`. So the tolerant hook must clear both gates."""
    ch = ClickHouseProc(ch_config_dir=str(tmp_path / "etc"))
    hook = _clickbench_hook(ch)

    returned = hook(str(tmp_path / "etc"), str(tmp_path / "var"))
    assert returned is not False
    assert fake_aws.invocations() == 1
    assert len(env.warnings) == 1

    # Apply `ClickHouseService.__enter__`'s own gate, so this cell keeps testing the real
    # rule rather than a local restatement of it: were the gate changed to `if not
    # hook(...)`, a truthy-but-not-True return would start failing there and here.
    service = ClickHouseService(results=[], config_hooks=[hook])
    service.ch_config_dir = str(tmp_path / "etc")
    service.ch_var_lib_dir = str(tmp_path / "var")
    for configured_hook in service.config_hooks:
        assert (
            configured_hook(service.ch_config_dir, service.ch_var_lib_dir) is not False
        )


def test_clickbench_hook_failure_would_otherwise_abort_the_benchmark(
    fake_aws, tmp_path
):
    """Negative control for r2-5: the untolerated hook raises out of the hook loop, which
    `ClickHouseService` turns into a job error."""
    ch = ClickHouseProc(ch_config_dir=str(tmp_path / "etc"))
    with pytest.raises(Exception):
        ch.create_log_export_config(str(tmp_path / "etc"))


def test_sqlstorm_start_reaches_the_server_start(fake_aws, env, tmp_path):
    """r2-6. SQLStorm's tolerance sits inline in the `start()` callable handed to
    `from_commands_run`, so assert the step both survives and continues: `ch.start()` has
    to be reached, and the tolerated fetch has to have been attempted."""
    reached = []

    class _Ch:
        log_export_host = None

        def install(self):
            return True

        def create_log_export_config(self):
            # The real fetch, so the real exception shape crosses the boundary.
            return ClickHouseProc().create_log_export_config(
                config_dir=str(tmp_path / "etc")
            )

        def start(self):
            reached.append("start")
            return True

        def start_log_exports(self, check_start_time):
            reached.append("start_log_exports")
            return False

    namespace = {
        "ch": _Ch(),
        "info": Info(),
        "stop_watch": type("_SW", (), {"start_time": 0})(),
    }
    start = _define(_SQLSTORM, "start", namespace, args=())

    assert start() is True
    assert reached == ["start", "start_log_exports"]
    assert fake_aws.invocations() == 1
    assert len(env.warnings) == 1


# --- the catch is not over-broad: other secret consumers stay fatal ----------


def test_a_non_telemetry_secret_consumer_still_fails_loudly(fake_aws):
    """r2-7. Nothing in `praktika.secret` changed, so every caller whose secret IS the
    job's purpose - release, docker, jepsen, cidb, copilot - keeps raising under the very
    same fetch failure. A fix that had softened the shared fetch would pass every cell
    above and silently degrade those jobs."""
    with pytest.raises(Exception) as excinfo:
        Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_PARAMETER,
            region="us-east-1",
        ).get_value()
    assert "exit_code 254" in str(excinfo.value)


def test_a_missing_or_empty_parameter_is_still_fatal(fake_aws, tmp_path, monkeypatch):
    """A genuine misconfiguration must not be laundered into a warning by the callee: an
    absent parameter name and an empty value both keep raising, so only the *caller* is
    tolerant and only for telemetry."""
    script = tmp_path / "aws"
    # rc=0 but the requested names are not in the answer.
    script.write_text("#!/bin/bash\nprintf 'some_other_name\\tvalue\\n'\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    with pytest.raises(RuntimeError) as excinfo:
        ClickHouseProc().create_log_export_config(config_dir=str(tmp_path / "etc"))
    assert "Failed to get value for parameter" in str(excinfo.value)
