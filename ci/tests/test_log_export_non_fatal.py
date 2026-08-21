"""Tests that a failed log-export configuration does not fail the job that owns it.

Log export is telemetry: `start_log_exports` guards on `log_export_host`, and a job
whose export was never configured runs its full suite (the llvm-coverage stateless
configs skip the step outright and are continuously green). The *start* step already
reports a lapse as a workflow warning, but the *configure* step used to propagate the
SSM fetch exception, so a transient `aws ssm get-parameters` failure - 502 Bad Gateway,
connect timeout, or absent instance credentials - failed `Install ClickHouse` and the
Stateless job aborted having run zero tests. These cells pin that each telemetry call
site tolerates the failure and records a warning, that a permanent misconfiguration
still fails the step, and that the tolerance did not widen to anything else.

The real production code runs against a fake `aws` on `PATH`, so the actual subprocess
and the real `Secret` fetch are exercised. The call sites are AST-extracted from the job
scripts and EXECUTED, never pattern-matched: importing those modules would run whole
jobs, and matching their text would accept a handler that can never run.
"""

import ast
import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `sqlstorm_test` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.scripts.clickhouse_proc as clickhouse_proc_module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika import Secret, SecretMisconfigured
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

# The two ways `Secret` itself rejects an answer it did receive. Both must stay fatal:
# `aws` exited 0, so no amount of retrying or tolerating changes the outcome.
_MISCONFIGURATIONS = {
    # The requested names are simply not in the answer.
    "missing_name": "printf 'some_other_name\\tvalue\\n'\n",
    # Both names are present, one with no value.
    "empty_value": (
        "printf 'clickhouse_ci_logs_host\\t\\nclickhouse_ci_logs_password\\tp\\n'\n"
    ),
}


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
def proc(monkeypatch, tmp_path):
    """A `ClickHouseProc` whose repo-global temp dir is redirected into `tmp_path`.

    `__init__` ends by rmtree-ing `{temp_dir}/var/log/clickhouse-server`, a path taken
    from a module global that the `ch_config_dir` argument does not redirect and that
    the `ClickHouseService` this suite runs inside uses for the live server's log and
    stderr sinks. Same convention as `test_collect_core_dumps.py`.
    """
    monkeypatch.setattr(clickhouse_proc_module, "temp_dir", str(tmp_path))
    monkeypatch.setattr(clickhouse_proc_module, "p_temp_dir", Path(tmp_path))

    def make(**kwargs):
        return ClickHouseProc(**kwargs)

    return make


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

    def misconfigure(shape):
        """`aws` exits 0 but the answer is unusable, so `Secret`'s own checks reject it."""
        _write(_MISCONFIGURATIONS[shape])

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # `create_log_export_config` copies a repo-relative config file.
    monkeypatch.chdir(_REPO_ROOT)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    install.invocations = invocations
    install.succeed_with = succeed_with
    install.misconfigure = misconfigure
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
    `info`, ...) resolve to the stand-ins the caller put there. Names the module imports
    are resolved through the module's own import statement instead, so a cell tests the
    module's real bindings rather than the test's idea of them.
    """
    namespace.update(_imported_names(path))
    exec(  # noqa: S102 - the definition comes from this repo's own job script
        _extract_function(path, name, args), namespace
    )
    return namespace[name]


def _module_constants(path, names):
    """The values `path` assigns to `names` at module level.

    Read from the module under test rather than borrowed from a sibling: both
    `sqlstorm_test.py` and `clickhouse_proc.py` define their own
    `LOG_EXPORT_CONFIG_TEMPLATE` and cluster/user constants, and the whole point of
    the SQLStorm cells is that the two copies are separate code.
    """
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    found = {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in names
    }
    missing = set(names) - set(found)
    assert not missing, f"{path} does not define {sorted(missing)}"
    return found


def _imported_names(path):
    """Resolve `path`'s own top-level `from X import a, b` bindings.

    The import root is load-bearing and invisible to a namespace the test fills in
    itself. `ci.praktika` and `praktika` are two distinct module objects for the same
    file, so a class defined there exists as two unrelated types: an instance of one is
    not caught by an `except` on the other, and because both are `RuntimeError`
    subclasses the miss does not crash - the handler falls through to its tolerant arm
    and the misconfiguration is laundered into a warning. Reading the binding from the
    module under test is what makes that observable here.
    """
    import importlib

    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    resolved = {}
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level:
            continue
        module = importlib.import_module(node.module)
        for alias in node.names:
            resolved[alias.asname or alias.name] = getattr(module, alias.name)
    return resolved


# --- the callee's contract, which the call sites rely on ----------------------


@pytest.mark.parametrize("exit_code,stderr", _AWS_FAILURES)
def test_the_fetch_failure_reaches_the_caller_as_an_exception(
    fake_aws, proc, tmp_path, exit_code, stderr
):
    """This fix does not change the callee, so pin what it does for all three observed
    shapes: it raises, leaves no host, and writes no config."""
    fake_aws(exit_code, stderr)
    config_dir = tmp_path / "etc"
    ch = proc()
    with pytest.raises(Exception) as excinfo:
        ch.create_log_export_config(config_dir=str(config_dir))
    assert f"exit_code {exit_code}" in str(excinfo.value)
    assert ch.log_export_host is None
    assert not (config_dir / "config.d" / "system_logs_export.yaml").exists()


def test_a_tolerated_failure_leaves_no_half_written_config(fake_aws, proc, tmp_path):
    """The directory is created before the fetch, so what survives must be an empty
    one - a partial `system_logs_export.yaml` would make the server fail to start."""
    config_dir = tmp_path / "etc"
    with pytest.raises(Exception):
        proc().create_log_export_config(config_dir=str(config_dir))
    assert [p.name for p in config_dir.iterdir()] == ["config.d"]
    assert list((config_dir / "config.d").iterdir()) == []


def test_a_failed_write_leaves_no_host_to_export_against(fake_aws, proc, tmp_path):
    """`start_log_exports` reads `log_export_host` to decide the cluster is defined on
    the server, so the fetched value may only be published once the file is on disk.
    A host set without the config written makes the export run against a cluster the
    server never got - the `Code: 701` failure this method's own comment documents."""
    fake_aws.succeed_with(host="ci-logs.example.com", password="secret")
    config_dir = tmp_path / "etc"
    config_file = config_dir / "config.d" / "system_logs_export.yaml"
    config_file.parent.mkdir(parents=True)
    config_file.write_text("PARTIAL")
    config_file.chmod(0o444)

    ch = proc()
    with pytest.raises(OSError):
        ch.create_log_export_config(config_dir=str(config_dir))
    assert ch.log_export_host is None
    assert ch.log_export_password is None


def test_the_sqlstorm_duplicate_also_publishes_only_what_it_wrote(fake_aws, tmp_path):
    """`sqlstorm_test.py` carries its own near-duplicate of the method, with the same
    two attributes and the same `start_log_exports` guard, so it needs the same commit
    ordering. Asserted separately because a fix applied to one copy leaves the other."""
    fake_aws.succeed_with(host="ci-logs.example.com", password="secret")
    config_path = tmp_path / "config"
    config_file = config_path / "config.d" / "system_logs_export.yaml"
    config_file.parent.mkdir(parents=True)
    config_file.write_text("PARTIAL")
    config_file.chmod(0o444)

    binary, configure = _sqlstorm_binary_config(config_path)
    with pytest.raises(OSError):
        configure()
    assert getattr(binary, "log_export_host", None) is None

    config_file.chmod(0o644)
    assert configure()
    assert binary.log_export_host == "ci-logs.example.com"


def test_a_successful_fetch_publishes_the_host_it_wrote(fake_aws, proc, tmp_path):
    """The other direction of the cell above: on the happy path the attributes must be
    set, or the export the config enables would never start."""
    host = "ci-logs.example.com"
    fake_aws.succeed_with(host=host, password="secret")
    config_dir = tmp_path / "etc"
    ch = proc()
    assert ch.create_log_export_config(config_dir=str(config_dir))
    assert ch.log_export_host == host
    assert ch.log_export_password == "secret"
    assert host in (config_dir / "config.d" / "system_logs_export.yaml").read_text()


@pytest.mark.parametrize("shape", sorted(_MISCONFIGURATIONS))
def test_a_rejected_answer_raises_the_distinguishable_type(
    fake_aws, proc, tmp_path, shape
):
    """A misconfiguration arrives after `aws` exited 0, so `Secret` knows locally that
    it is permanent. It carries its own type, which is the only thing letting the
    telemetry handlers tolerate a transport failure without tolerating this."""
    fake_aws.misconfigure(shape)
    with pytest.raises(SecretMisconfigured):
        proc().create_log_export_config(config_dir=str(tmp_path / "etc"))


# --- Install ClickHouse survives it (the defect) ------------------------------


def _functional_tests_closure(config_dir, ch):
    """The production `configure_log_export` closure, with its two free variables bound."""
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
    """The production ClickBench config hook, with its free variables bound."""
    return _define(
        _CLICKBENCH,
        "configure_log_export",
        {"info": Info(), "ch": ch},
        args=("config_dir", "var_lib_dir"),
    )


def test_install_clickhouse_survives_a_failed_log_export_fetch(
    fake_aws, env, proc, tmp_path
):
    """The claim of the whole change, through the production result machinery: a
    telemetry blip must not abort the job before a single test runs."""
    closure = _functional_tests_closure(str(tmp_path / "etc"), proc())
    result = Result.from_commands_run(
        name="Install ClickHouse", command=["true", closure]
    )
    assert result.is_ok(), result.info
    # The fetch was really attempted, so the cell is not passing because the step
    # stopped configuring log export altogether.
    assert fake_aws.invocations() == 1


def test_without_the_tolerance_install_clickhouse_fails(fake_aws, proc, tmp_path):
    """Negative control. The same fetch failure, reported the way it was before:
    if this passes, the oracle above cannot redden and pins nothing."""
    ch = proc()

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


def test_the_lapse_is_recorded_as_a_workflow_warning(fake_aws, env, proc, tmp_path):
    """Since the tolerance does not classify the cause, the warning is the only thing
    standing between a real SSM outage and silence. Assert the stored message, not
    merely that nothing escaped."""
    closure = _functional_tests_closure(str(tmp_path / "etc"), proc())
    closure()
    assert len(env.warnings) == 1
    assert "log export" in env.warnings[0]
    # The cause has to survive into the message, or the warning cannot be acted on.
    assert "502" in env.warnings[0]


def test_the_tolerance_did_not_widen_to_the_rest_of_the_step(
    fake_aws, env, proc, tmp_path
):
    """A real failure elsewhere in the same command list must still fail the step:
    the catch is scoped to the telemetry call, not draped over the install."""
    closure = _functional_tests_closure(str(tmp_path / "etc"), proc())
    result = Result.from_commands_run(
        name="Install ClickHouse", command=[closure, "false"]
    )
    assert not result.is_ok()


def test_a_successful_fetch_is_unaffected(fake_aws, env, proc, tmp_path):
    """The happy path still writes the config and records no warning: the `try` wraps the
    call, so it must be transparent when the call succeeds."""
    host = "ci-logs.example.com"
    fake_aws.succeed_with(host=host, password="secret")

    config_dir = tmp_path / "etc"
    closure = _functional_tests_closure(str(config_dir), proc())
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


def _append_guard(path):
    """The innermost `if` guarding the append, the guards enclosing it, and its target.

    The guard is EXECUTED by the callers rather than matched: one amended with
    `and False`, or an append moved under a condition that never holds, keeps every
    substring while never running in CI. The enclosing guards are returned too, because
    the guard alone does not say which list the closure joins - an append into a list
    nobody runs would satisfy the execution check.
    """
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)

    guards = [
        n for n in ast.walk(tree) if isinstance(n, ast.If) and _appends_of_closure(n)
    ]
    assert guards, f"no guard appends configure_log_export in {path}"
    # Innermost: every enclosing guard also contains the append, and executing an outer
    # one would need the whole enclosing stage's locals.
    innermost = [
        g for g in guards if not any(g is not o and o in ast.walk(g) for o in guards)
    ]
    assert len(innermost) == 1, f"expected one innermost guard, got {len(innermost)}"
    targets = _appends_of_closure(innermost[0])
    assert len(targets) == 1, f"expected one append target, got {targets}"
    enclosing = [g for g in guards if g is not innermost[0]]
    return innermost[0], enclosing, targets[0], tree


def _run_guard(statement, path, namespace):
    exec(  # noqa: S102 - the statement comes from this repo's own job script
        compile(ast.Module(body=[statement], type_ignores=[]), path, "exec"), namespace
    )


@pytest.mark.parametrize("is_llvm_coverage,expected", [(False, True), (True, False)])
def test_the_closure_is_appended_to_the_install_commands(is_llvm_coverage, expected):
    """Every cell above calls the closure directly, so without this one deleting the
    append would leave the file green. Both directions are asserted, so a guard that
    always ran would fail here rather than pass for the wrong reason."""
    statement, _, target, _ = _append_guard(_FUNCTIONAL_TESTS)
    appended = []
    _run_guard(
        statement,
        _FUNCTIONAL_TESTS,
        {
            target: appended,
            "configure_log_export": lambda: True,
            "is_llvm_coverage": is_llvm_coverage,
        },
    )
    assert bool(appended) is expected


def test_the_appended_closure_lands_in_the_install_result_command_list():
    """The append's target must be the very list handed to `Result.from_commands_run(name=
    "Install ClickHouse", ...)`. Without this, the cell above is satisfied by an append
    into some other list that no result ever runs - the closure would be tolerant and also
    dead, and log export would silently stop being configured at all."""
    _, enclosing, target, _ = _append_guard(_FUNCTIONAL_TESTS)
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


@pytest.mark.parametrize("is_local_run,expected", [(False, True), (True, False)])
def test_the_clickbench_hook_is_appended_to_the_config_hooks(is_local_run, expected):
    """The ClickBench counterpart, and it is not redundant: deleting this append left the
    whole module green, so log export could silently stop being configured for ClickBench.
    Both directions, so a guard that always ran fails here too."""
    statement, _, target, _ = _append_guard(_CLICKBENCH)
    appended = []
    _run_guard(
        statement,
        _CLICKBENCH,
        {
            target: appended,
            "configure_log_export": lambda config_dir, var_lib_dir: True,
            "install_ci_logs_sender": lambda config_dir, var_lib_dir: True,
            "info": type("_Info", (), {"is_local_run": is_local_run})(),
        },
    )
    assert bool(appended) is expected


def test_the_appended_clickbench_hook_lands_in_the_service_config_hooks():
    """The append's target must be the list handed to `ClickHouseService(config_hooks=)`,
    which is what runs the hooks. Retargeting the append to any other list keeps the cell
    above green while the hook never runs."""
    _, _, target, tree = _append_guard(_CLICKBENCH)
    services = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func).endswith("ClickHouseService")
    ]
    assert (
        len(services) == 1
    ), f"expected one ClickHouseService call, got {len(services)}"
    hooks = [kw.value for kw in services[0].keywords if kw.arg == "config_hooks"]
    assert hooks, "the ClickHouseService is constructed without `config_hooks`"
    assert ast.unparse(hooks[0]) == target


# --- the other two telemetry call sites --------------------------------------


def test_clickbench_config_hook_survives_and_does_not_trip_the_hook_gate(
    fake_aws, env, proc, tmp_path
):
    """ClickBench fails by a second mechanism: the hook runs inside
    `ClickHouseService.__enter__`, which raises both when a hook raises and when it
    returns exactly `False`. So the tolerant hook must clear both gates."""
    ch = proc(ch_config_dir=str(tmp_path / "etc"))
    hook = _clickbench_hook(ch)

    returned = hook(str(tmp_path / "etc"), str(tmp_path / "var"))
    assert returned is not False
    assert fake_aws.invocations() == 1
    assert len(env.warnings) == 1

    # Apply `ClickHouseService.__enter__`'s own gate, so this cell keeps testing the real
    # rule rather than a local restatement of it: were the gate changed to `if not
    # hook(...)`, a truthy-but-not-True return would start failing there and here.
    for configured_hook in _service_hooks(hook, tmp_path):
        assert configured_hook(*_service_dirs(tmp_path)) is not False


def _service_dirs(tmp_path):
    return str(tmp_path / "etc"), str(tmp_path / "var")


def _service_hooks(hook, tmp_path):
    service = ClickHouseService(results=[], config_hooks=[hook])
    service.ch_config_dir, service.ch_var_lib_dir = _service_dirs(tmp_path)
    return service.config_hooks


def test_clickbench_hook_failure_would_otherwise_abort_the_benchmark(
    fake_aws, proc, tmp_path
):
    """Negative control: the untolerated hook raises out of the hook loop, which
    `ClickHouseService` turns into a job error."""
    ch = proc(ch_config_dir=str(tmp_path / "etc"))
    with pytest.raises(Exception):
        ch.create_log_export_config(str(tmp_path / "etc"))


def _sqlstorm_binary_config(config_path):
    """The production `ClickHouseBinary.create_log_export_config`, bound to `config_path`.

    Extracted and executed rather than reached through `ClickHouseProc`: the two
    implementations are near-duplicates, and the SQLStorm one raises `praktika`'s class
    while `ClickHouseProc` raises `ci.praktika`'s. Substituting one for the other would
    hand the handler an exception it can never catch, and the cell would report a
    laundered misconfiguration that production does not have (or hide one it does).
    """
    binary = type("_Binary", (), {"config_path": str(config_path)})()
    namespace = {"Path": Path}
    namespace.update(
        _module_constants(
            _SQLSTORM,
            (
                "LOG_EXPORT_CONFIG_TEMPLATE",
                "CLICKHOUSE_CI_LOGS_CLUSTER",
                "CLICKHOUSE_CI_LOGS_USER",
            ),
        )
    )
    method = _define(_SQLSTORM, "create_log_export_config", namespace, args=("self",))
    return binary, lambda: method(binary)


def _sqlstorm_start(config_path, env_recorder, reached):
    """The production `start()` closure, over a binary whose config step is the real one."""
    binary, configure = _sqlstorm_binary_config(config_path)

    class _Ch:
        log_export_host = None

        def install(self):
            return True

        def create_log_export_config(self):
            return configure()

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
    return _define(_SQLSTORM, "start", namespace, args=()), binary


def test_sqlstorm_start_reaches_the_server_start(fake_aws, env, tmp_path):
    """SQLStorm's tolerance sits inline in the `start()` callable handed to
    `from_commands_run`, so assert the step both survives and continues: `ch.start()` has
    to be reached, and the tolerated fetch has to have been attempted."""
    reached = []
    start, _ = _sqlstorm_start(tmp_path / "config", env, reached)

    assert start() is True
    assert reached == ["start", "start_log_exports"]
    assert fake_aws.invocations() == 1
    assert len(env.warnings) == 1


# --- a permanent misconfiguration still fails each owning step ----------------
#
# These drive each site through its own production handler rather than calling the
# callee directly: with no handler on the stack a cell cannot observe the handler
# laundering a misconfiguration into a warning, which is exactly what it must catch.


@pytest.mark.parametrize("shape", sorted(_MISCONFIGURATIONS))
def test_a_misconfiguration_fails_install_clickhouse(
    fake_aws, env, proc, tmp_path, shape
):
    fake_aws.misconfigure(shape)
    closure = _functional_tests_closure(str(tmp_path / "etc"), proc())
    result = Result.from_commands_run(
        name="Install ClickHouse", command=["true", closure]
    )
    assert not result.is_ok()
    assert env.warnings == []


@pytest.mark.parametrize("shape", sorted(_MISCONFIGURATIONS))
def test_a_misconfiguration_fails_the_clickbench_hook_gate(
    fake_aws, env, proc, tmp_path, shape
):
    fake_aws.misconfigure(shape)
    hook = _clickbench_hook(proc(ch_config_dir=str(tmp_path / "etc")))
    with pytest.raises(SecretMisconfigured):
        for configured_hook in _service_hooks(hook, tmp_path):
            configured_hook(*_service_dirs(tmp_path))
    assert env.warnings == []


@pytest.mark.parametrize("shape", sorted(_MISCONFIGURATIONS))
def test_a_misconfiguration_fails_sqlstorm_start(fake_aws, env, tmp_path, shape):
    fake_aws.misconfigure(shape)
    reached = []
    start, _ = _sqlstorm_start(tmp_path / "config", env, reached)

    result = Result.from_commands_run(name="Start ClickHouse", command=start)
    assert not result.is_ok()
    # The step must abort at the misconfiguration, not carry on into a server start.
    assert reached == []
    assert env.warnings == []


# --- the catch is not over-broad: other secret consumers stay fatal ----------


def test_a_non_telemetry_secret_consumer_still_fails_loudly(fake_aws):
    """Nothing tolerates the fetch inside `praktika.secret`, so every caller whose secret
    IS the job's purpose - release, docker, jepsen, cidb, copilot - keeps raising under
    the very same fetch failure. A fix that had softened the shared fetch would pass
    every cell above and silently degrade those jobs."""
    with pytest.raises(Exception) as excinfo:
        Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_PARAMETER,
            region="us-east-1",
        ).get_value()
    assert "exit_code 254" in str(excinfo.value)


@pytest.mark.parametrize("shape", sorted(_MISCONFIGURATIONS))
def test_a_non_telemetry_consumer_still_fails_on_a_misconfiguration(fake_aws, shape):
    """The typed exception is a `RuntimeError` subclass raised from the same two checks
    as before, so a non-telemetry consumer sees no change: it still fails, with the same
    message. This bounds the blast radius of touching the shared `secret` module."""
    fake_aws.misconfigure(shape)
    with pytest.raises(RuntimeError) as excinfo:
        Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_PARAMETER,
            region="us-east-1",
        ).join_with(
            Secret.Config(
                name="clickhouse_ci_logs_password",
                type=Secret.Type.AWS_SSM_PARAMETER,
                region="us-east-1",
            )
        ).get_value()
    assert "parameter [" in str(excinfo.value)
