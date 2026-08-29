"""
Regression tests for the per-statement bound in `prepare_stateful_data`.

Two contrasts carry the design, so the arms below assert them rather than only
"is it bounded?":

- `Shell.run(timeout=)` bounds the run too, but it SIGTERMs the process group,
  leaving no exit code for the ERR trap and an empty stderr.
- A child that IGNORES SIGTERM is the realistic wedged-client shape; one
  `timeout` around the whole script would SIGKILL the trap's own shell.

Arms 2 and 4 drive the real `Shell.run` with fake shell commands, so they
exercise the actual signal semantics. No ClickHouse binary, server or docker
is needed.
"""

import dataclasses
import inspect
import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import Job`, so it needs `ci/`
# on the path as well (precedent: `test_new_tests_check.py`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.job_configs import JobConfigs
from ci.jobs import functional_tests
from ci.jobs.scripts import clickhouse_proc
from ci.jobs.functional_tests import (
    STATEFUL_PREP_STEP_TIMEOUT_RATIO,
    stateful_prep_step_timeout,
)
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika._environment import _Environment
from ci.praktika.job import Job
from ci.praktika.utils import Shell

# Worst prep wall-clock over 30 job logs, 3 days, 7 flavours: 1010 s
# (`amd_asan_ubsan, flaky check`), worst `amd_tsan` 671 s. The recorded hangs
# each burned 8414 s or more, so the bound sits between the two populations.
# 3x this is 3030 s against a 3150 s bound today: only 120 s of margin.
WORST_HEALTHY_PREP_S = 1010

# Teardown after a failed prep: `_flush_system_logs` issues 2 client calls per
# replica (<=3), each bounded by the socket receive timeout
# (`Connection.cpp:409` reads `receive_timeout`, default 300 s in
# `src/Core/Defines.h:15`), so <= 6 * 300.
TEARDOWN_WORST_S = 6 * 300

# Worst observed `Collect logs` stage: 754.4 s in `Stateless tests
# (amd_llvm_coverage, old analyzer, s3 storage, DBReplicated, WasmEdge,
# sequential, 1/2)`, PR115544 @ 4a23cb2bf742e66.
COLLECT_LOGS_WORST_S = 900

# Any positive bound: the render arms assert the script's shape, not the number.
_ANY_BOUND_S = 1234

# The ERR trap the prep script installs, verbatim from `prepare_stateful_data`.
TRAP = (
    "trap 'rc=$?; echo \"prepare_stateful_data: command [$BASH_COMMAND] "
    "at line $LINENO failed with exit $rc\" >&2' ERR"
)

# Every statement the prep script bounds: a client invocation or one of the
# dataset scripts, at the start of a line inside the script body.
_STATEMENT = re.compile(
    r"^(?P<prefix>\s*)(?P<bound>\$PREP_TIMEOUT )?"
    r"(clickhouse-client\b|bash \./tests/docker_scripts/create_)",
    re.MULTILINE,
)


class _FakeInfo:
    def __init__(self, is_local_run=False, job_config=None):
        self.is_local_run = is_local_run
        self.job_config = job_config


def _functional_test_job_timeouts():
    """`{job name: timeout}` for every job that runs `functional_tests.py`.

    Read from the production configs so a retuned budget reaches the arms below
    instead of being asserted against a copy of today's number. Matched by
    duck typing on purpose: `ci/defs/job_configs.py` imports `praktika.job`
    while this file imports `ci.praktika.job`, which are distinct module
    objects, so `isinstance(j, Job.Config)` matches nothing here.
    """
    timeouts = {}
    for attr in dir(JobConfigs):
        if attr.startswith("_"):
            continue
        value = getattr(JobConfigs, attr)
        for job in value if isinstance(value, (list, tuple)) else [value]:
            command = getattr(job, "command", None)
            if isinstance(command, str) and "functional_tests.py" in command:
                timeouts[getattr(job, "name", attr)] = getattr(job, "timeout", None)
    assert timeouts, "no functional-test job matched: this matcher is stale"
    return timeouts


def _script_max_execution_time():
    """`MAX_EXECUTION_TIME` as the prep script actually assigns it."""
    source = inspect.getsource(ClickHouseProc.prepare_stateful_data)
    matches = re.findall(r"^MAX_EXECUTION_TIME=(\d+)$", source, re.MULTILINE)
    assert len(matches) == 1, f"expected one assignment, found {matches}"
    return int(matches[0])


def _render(monkeypatch, *, with_s3_storage, step_timeout):
    """Return the prep script `prepare_stateful_data` would run."""
    captured = []

    class _CapturingShell:
        @staticmethod
        def run(command, **_kwargs):
            captured.append(command)
            return 0

    monkeypatch.setattr(clickhouse_proc, "Shell", _CapturingShell)
    proc = ClickHouseProc.__new__(ClickHouseProc)
    assert ClickHouseProc.prepare_stateful_data(
        proc,
        with_s3_storage=with_s3_storage,
        is_db_replicated=False,
        build_type="amd_tsan",
        step_timeout=step_timeout,
    )
    assert len(captured) == 1
    return captured[0]


def _prep_timeout_value(script):
    """The `PREP_TIMEOUT=...` value the script assigns, unquoted."""
    m = re.search(r"^PREP_TIMEOUT='([^']*)'$", script, re.MULTILINE)
    assert m, f"no PREP_TIMEOUT assignment in:\n{script[:400]}"
    return m.group(1)


def _bound_prefix(seconds, kill_after=2):
    """The production bound prefix with only the kill grace shortened, so a
    change to the prefix reaches the behavioural arms and not only the string
    ones."""
    prefix = ClickHouseProc.prep_timeout_prefix(seconds)
    return prefix.replace(
        f"--kill-after={ClickHouseProc.PREP_STEP_KILL_AFTER_S} ",
        f"--kill-after={kill_after} ",
    )


def _mini_script(tmp_path, bound_prefix, payload):
    """A miniature script with the prep script's own shape: `set -e`, the same
    ERR trap, and one bounded statement."""
    path = tmp_path / "mini.sh"
    path.write_text(
        "set -e\n"
        "set -o pipefail\n"
        f"{TRAP}\n"
        f"PREP_TIMEOUT={bound_prefix!r}\n"
        "echo before\n"
        f"$PREP_TIMEOUT {payload}\n"
        "echo after\n"
    )
    return path


def _run_mini(tmp_path, bound_prefix, payload, shell_timeout=None):
    """Drive the real `Shell.run` over a miniature script; return (rc, log tail)."""
    script = _mini_script(tmp_path, bound_prefix, payload)

    log_file = tmp_path / "mini.log"
    rc = Shell.run(
        f"bash {script}",
        log_file=str(log_file),
        verbose=False,
        timeout=shell_timeout,
    )
    return rc, log_file.read_text()


# --------------------------------------------------------------------------- #
# 1. Every statement is bounded, and the bound is derived, not a literal.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("with_s3_storage", [False, True])
def test_every_statement_is_bounded(monkeypatch, with_s3_storage):
    script = _render(
        monkeypatch, with_s3_storage=with_s3_storage, step_timeout=_ANY_BOUND_S
    )
    statements = list(_STATEMENT.finditer(script))
    assert statements, "no prep statements matched: the matcher is stale"
    unbounded = [
        script[m.start() : script.index("\n", m.start())]
        for m in statements
        if not m.group("bound")
    ]
    assert not unbounded, f"unbounded prep statements: {unbounded}"


def test_bound_is_derived_from_the_job_budget():
    """A statement bound must follow a retuned job timeout, so the number may not
    be written into the script or the derivation."""
    source = inspect.getsource(stateful_prep_step_timeout)
    for timeout in sorted(set(_functional_test_job_timeouts().values())):
        bound = int(timeout * STATEFUL_PREP_STEP_TIMEOUT_RATIO)
        assert (
            stateful_prep_step_timeout(_FakeInfo(job_config={"timeout": timeout}))
            == bound
        )
        # Halving the budget must halve the bound: a literal would not move.
        assert stateful_prep_step_timeout(
            _FakeInfo(job_config={"timeout": timeout // 2})
        ) == int(timeout // 2 * STATEFUL_PREP_STEP_TIMEOUT_RATIO)
        for literal in (str(timeout), str(bound)):
            assert literal not in source, f"hardcoded [{literal}] in the derivation"


# --------------------------------------------------------------------------- #
# 2. The diagnostic survives the bound - and only the in-command spelling.
# --------------------------------------------------------------------------- #
def test_bound_statement_is_named_in_the_log(tmp_path):
    rc, log = _run_mini(tmp_path, _bound_prefix(1), "sleep 30")
    assert rc == 124, log
    assert "timeout: sending signal TERM" in log, log
    assert "failed with exit 124" in log, log
    assert "sleep 30" in log, log


def test_shell_run_timeout_loses_the_diagnostic(tmp_path):
    """`Shell.run(timeout=)` also bounds the run, so a naive "is it bounded?"
    assertion stays green either way; it names no statement, which is what makes
    the in-command spelling load-bearing."""
    rc, log = _run_mini(tmp_path, "", "sleep 30", shell_timeout=1)
    assert rc != 124
    assert "failed with exit" not in log, log
    assert "timeout: sending signal" not in log, log


# --------------------------------------------------------------------------- #
# 3. The bound cannot pre-empt healthy work.
# --------------------------------------------------------------------------- #
def test_bound_clears_healthy_prep_and_the_script_limit():
    """Asserted for every real functional-test job, against the script's own
    limit as it is written, so a retuned budget that pushed the bound under a
    legitimate INSERT would redden this arm instead of passing silently."""
    script_max_execution_time = _script_max_execution_time()
    for job, job_timeout in sorted(_functional_test_job_timeouts().items()):
        bound = stateful_prep_step_timeout(_FakeInfo(job_config={"timeout": job_timeout}))
        assert bound >= 3 * WORST_HEALTHY_PREP_S, (
            f"[{job}]: bound {bound}s is under 3x the worst observed healthy prep "
            f"({WORST_HEALTHY_PREP_S}s): it would abort healthy runs"
        )
        assert bound > script_max_execution_time, (
            f"[{job}]: bound {bound}s would cut short an INSERT the script already "
            f"allows {script_max_execution_time}s"
        )
        assert bound < job_timeout, f"[{job}]: the bound must fire before the watchdog"


# --------------------------------------------------------------------------- #
# 4. A TERM-ignoring statement is still bounded AND still named.
# --------------------------------------------------------------------------- #
def test_term_ignoring_statement_is_killed_and_named(tmp_path):
    """`--kill-after` bounds a TERM-ignoring statement, and because the bound
    sits on the statement the trap still runs in the parent shell; one `timeout`
    around the whole script would SIGKILL that shell instead."""
    rc, log = _run_mini(
        tmp_path, _bound_prefix(1), "bash -c 'trap \"\" TERM; sleep 60'"
    )
    assert rc == 137, log
    assert "timeout: sending signal KILL" in log, log
    assert "failed with exit 137" in log, log
    assert "sleep 60" in log, log


def test_bound_includes_kill_after(monkeypatch):
    """Without `--kill-after` a TERM-ignoring statement runs to completion, so
    the prep would not be bounded at all in the arm above."""
    value = _prep_timeout_value(
        _render(monkeypatch, with_s3_storage=False, step_timeout=_ANY_BOUND_S)
    )
    assert f"--kill-after={ClickHouseProc.PREP_STEP_KILL_AFTER_S}" in value, value
    assert "--verbose" in value, value
    assert "--signal=TERM" in value, value


# --------------------------------------------------------------------------- #
# 5. Reading the job budget: `JOB_CONFIG` is a dict, and a local run is unbounded.
# --------------------------------------------------------------------------- #
def test_job_config_survives_serialization_as_a_mapping():
    """`_Environment.from_dict` rebuilds no nested dataclass, so `JOB_CONFIG`
    arrives as a plain dict and attribute access would raise in every stateless
    job."""
    sentinel_timeout = 4242  # arbitrary: only the round-trip shape is asserted
    fields = {f.name: f for f in dataclasses.fields(_Environment)}
    required = {
        name: (0 if f.type == "int" else "" if f.type == "str" else False)
        for name, f in fields.items()
        if f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
    }
    env = _Environment(**required)
    env.JOB_CONFIG = Job.Config(
        name="x", runs_on=["a"], command="c", timeout=sentinel_timeout
    )
    assert env.JOB_CONFIG.timeout == sentinel_timeout

    round_tripped = _Environment.from_dict(
        json.loads(json.dumps(dataclasses.asdict(env), default=str))
    )
    assert isinstance(round_tripped.JOB_CONFIG, dict)
    assert round_tripped.JOB_CONFIG["timeout"] == sentinel_timeout
    assert (
        stateful_prep_step_timeout(_FakeInfo(job_config=round_tripped.JOB_CONFIG))
        is not None
    )


def test_local_run_is_unbounded_and_renders_unchanged(monkeypatch):
    """A missing job budget means a local run, which stays unbounded: a numeric
    fallback would invent a limit for a laptop and mask a CI harness bug."""
    assert stateful_prep_step_timeout(_FakeInfo(is_local_run=True)) is None
    assert (
        stateful_prep_step_timeout(_FakeInfo(is_local_run=True, job_config=None))
        is None
    )

    script = _render(monkeypatch, with_s3_storage=False, step_timeout=None)
    assert _prep_timeout_value(script) == ""
    assert "timeout" not in _prep_timeout_value(script)


def test_unusable_job_budget_in_ci_fails_loudly():
    """In CI the budget is required: a missing or nonsensical one is a harness
    bug to surface, not something to paper over with a default."""
    for job_config in ({}, {"timeout": None}, {"timeout": 0}, {"timeout": "x"}, None):
        with pytest.raises(RuntimeError):
            stateful_prep_step_timeout(_FakeInfo(job_config=job_config))


# --------------------------------------------------------------------------- #
# 6. A setup failure still reaches log collection, and no unsafe timer is added.
# --------------------------------------------------------------------------- #
def test_collect_logs_is_not_gated_on_setup_success():
    """`COLLECT_LOGS` is reached unguarded while the test stages sit behind
    `if res and ...`, so a failed prep still uploads the server logs."""
    lines = inspect.getsource(functional_tests).split("\n")
    collect = [
        line.strip()
        for line in lines
        if "JobStages.COLLECT_LOGS in stages" in line and line.startswith("    if ")
    ]
    assert collect, "COLLECT_LOGS stage guard not found: this check is stale"
    assert all(
        line == "if JobStages.COLLECT_LOGS in stages:" for line in collect
    ), collect
    assert any(
        line.strip().startswith("if res and") and "JobStages.TEST" in line
        for line in lines
    ), "expected the test stage to be guarded by `res`"


def test_a_bound_hit_still_leaves_budget_for_log_collection():
    """Failing fast only preserves the server logs if the job can still reach
    `COLLECT_LOGS` inside its budget, so the worst path has to fit: the bound,
    the kill grace, teardown, then log collection."""
    for job, job_timeout in sorted(_functional_test_job_timeouts().items()):
        bound = stateful_prep_step_timeout(_FakeInfo(job_config={"timeout": job_timeout}))
        worst = (
            bound
            + ClickHouseProc.PREP_STEP_KILL_AFTER_S
            + TEARDOWN_WORST_S
            + COLLECT_LOGS_WORST_S
        )
        assert worst < job_timeout, (
            f"[{job}]: worst path after a bound hit is {worst}s against a "
            f"{job_timeout}s budget, so log collection would be cut off"
        )


def test_prep_adds_no_shell_timer_callers():
    """`Shell`'s own timer `killpg`s a process group that may already be
    recycled, so the bound must stay in the command."""
    source = inspect.getsource(ClickHouseProc.prepare_stateful_data)
    for unsafe in ("Shell.run(", "Shell.check("):
        for call in re.findall(re.escape(unsafe) + r"[^)]*\)", source, re.DOTALL):
            assert "timeout=" not in call, call


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
