"""
Regression tests for the per-statement bound in `prepare_stateful_data`.

The prep script ran as one unbounded `Shell.run`, so a `clickhouse-client` that
never returned was not an error: `set -e` and the ERR trap only fire on a
non-zero exit. Nothing terminated the step, and praktika's 9000 s job watchdog
SIGKILLed the whole job with `clickhouse-test` never started, no test row in
CIDB and no uploaded server logs (a killed job never reaches `COLLECT_LOGS`).

Every statement now carries `timeout --verbose --signal=TERM --kill-after=N`,
sized as a fraction of the job budget. A bound reached exits 124 (or 137 after
the kill), which `set -e` and the existing trap turn into a line naming the
statement.

Two contrasts carry the design and neither is decoration:

- `Shell.run(timeout=)` also bounds the run, but it SIGTERMs the process group,
  so there is no exit code for the trap and its stderr is empty. Only the
  in-command spelling produces a diagnostic (arm 2 asserts both spellings).
- A child that IGNORES SIGTERM is the realistic shape of a wedged client. One
  `timeout` around the whole script loses the naming there, because `--kill-after`
  delivers an uncatchable SIGKILL to the trap's own shell. Per-statement bounds
  keep it (arm 4 asserts the 137 path still names the statement).

Arms 2 and 4 drive the real `Shell.run` with fake shell commands, so they
exercise the actual signal semantics rather than a model of them. No ClickHouse
binary, no server and no docker are needed.
"""

import dataclasses
import inspect
import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

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

# The praktika budget for every stateless functional-test job
# (`common_ft_job_config`, `ci/defs/job_configs.py`).
JOB_TIMEOUT_S = int(3600 * 2.5)

# Worst prep wall-clock observed over 40 downloaded job logs, all flavours
# (`amd_asan_ubsan, flaky check`). The worst `amd_tsan, flaky check` was 675 s.
# The five recorded hangs each burned 8414 s or more, so the two populations do
# not overlap and the bound has to sit between them.
WORST_HEALTHY_PREP_S = 965

# `MAX_EXECUTION_TIME` inside the prep script: the heavy INSERTs pass it to the
# server, so a bound below it would cut a legitimate INSERT short.
SCRIPT_MAX_EXECUTION_TIME_S = 1800

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
    """The production bound prefix, with only the kill grace shortened so these
    arms finish quickly. Taking the rest from production means a change to the
    prefix reaches the behavioural arms below instead of only the string ones."""
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
        monkeypatch, with_s3_storage=with_s3_storage, step_timeout=3150
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
    assert stateful_prep_step_timeout(
        _FakeInfo(job_config={"timeout": JOB_TIMEOUT_S})
    ) == int(JOB_TIMEOUT_S * STATEFUL_PREP_STEP_TIMEOUT_RATIO)
    # Halving the budget must halve the bound: a literal would not move.
    assert stateful_prep_step_timeout(
        _FakeInfo(job_config={"timeout": JOB_TIMEOUT_S // 2})
    ) == int(JOB_TIMEOUT_S // 2 * STATEFUL_PREP_STEP_TIMEOUT_RATIO)

    source = inspect.getsource(stateful_prep_step_timeout)
    for literal in (str(JOB_TIMEOUT_S), "3150"):
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
    """`Shell.run(timeout=)` bounds the run too, so a naive "is it bounded?"
    assertion stays green either way. It SIGTERMs the process group, so the trap
    never runs and nothing names the statement: that contrast is what makes the
    in-command spelling load-bearing."""
    rc, log = _run_mini(tmp_path, "", "sleep 30", shell_timeout=1)
    assert rc != 124
    assert "failed with exit" not in log, log
    assert "timeout: sending signal" not in log, log


# --------------------------------------------------------------------------- #
# 3. The bound cannot pre-empt healthy work.
# --------------------------------------------------------------------------- #
def test_bound_clears_healthy_prep_and_the_script_limit():
    bound = stateful_prep_step_timeout(
        _FakeInfo(job_config={"timeout": JOB_TIMEOUT_S})
    )
    assert bound >= 3 * WORST_HEALTHY_PREP_S, (
        f"bound {bound}s is under 3x the worst observed healthy prep "
        f"({WORST_HEALTHY_PREP_S}s): it would abort healthy runs"
    )
    assert bound > SCRIPT_MAX_EXECUTION_TIME_S, (
        f"bound {bound}s would cut short an INSERT the script already allows "
        f"{SCRIPT_MAX_EXECUTION_TIME_S}s"
    )
    assert bound < JOB_TIMEOUT_S, "the bound must fire before the job watchdog"


# --------------------------------------------------------------------------- #
# 4. A TERM-ignoring statement is still bounded AND still named.
# --------------------------------------------------------------------------- #
def test_term_ignoring_statement_is_killed_and_named(tmp_path):
    """A client wedged in a syscall or a lock wait does not act on SIGTERM.
    `--kill-after` bounds it, and because the bound sits on the statement rather
    than around the whole script, the trap still runs in the parent shell and
    names it. One `timeout` around the script would SIGKILL that shell instead."""
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
        _render(monkeypatch, with_s3_storage=False, step_timeout=3150)
    )
    assert f"--kill-after={ClickHouseProc.PREP_STEP_KILL_AFTER_S}" in value, value
    assert "--verbose" in value, value
    assert "--signal=TERM" in value, value


# --------------------------------------------------------------------------- #
# 5. Reading the job budget: `JOB_CONFIG` is a dict, and a local run is unbounded.
# --------------------------------------------------------------------------- #
def test_job_config_survives_serialization_as_a_mapping():
    """`_Environment.from_dict` ends in `cls(**filtered_obj)` and rebuilds no
    nested dataclass, so `JOB_CONFIG` arrives as a plain dict inside the job.
    Attribute access would raise in every stateless job. If someone later
    teaches `from_dict` to rebuild it, this arm says the derivation needs a look
    rather than letting it regress silently."""
    fields = {f.name: f for f in dataclasses.fields(_Environment)}
    required = {
        name: (0 if f.type == "int" else "" if f.type == "str" else False)
        for name, f in fields.items()
        if f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
    }
    env = _Environment(**required)
    env.JOB_CONFIG = Job.Config(
        name="x", runs_on=["a"], command="c", timeout=JOB_TIMEOUT_S
    )
    assert env.JOB_CONFIG.timeout == JOB_TIMEOUT_S

    round_tripped = _Environment.from_dict(
        json.loads(json.dumps(dataclasses.asdict(env), default=str))
    )
    assert isinstance(round_tripped.JOB_CONFIG, dict)
    assert round_tripped.JOB_CONFIG["timeout"] == JOB_TIMEOUT_S
    assert (
        stateful_prep_step_timeout(_FakeInfo(job_config=round_tripped.JOB_CONFIG))
        is not None
    )


def test_local_run_is_unbounded_and_renders_unchanged(monkeypatch):
    """A missing job budget means a local run, so the prep keeps its previous
    unbounded behaviour there. A numeric fallback would both invent a limit for a
    developer's laptop and mask a real CI harness bug."""
    assert stateful_prep_step_timeout(_FakeInfo(is_local_run=True)) is None
    assert (
        stateful_prep_step_timeout(_FakeInfo(is_local_run=True, job_config=None))
        is None
    )

    script = _render(monkeypatch, with_s3_storage=False, step_timeout=None)
    assert _prep_timeout_value(script) == ""
    assert "timeout" not in _prep_timeout_value(script)


def test_unusable_job_budget_in_ci_fails_loudly():
    """In CI the budget is required: a missing or nonsensical one is a harness bug
    worth surfacing, not something to paper over with a default."""
    for job_config in ({}, {"timeout": None}, {"timeout": 0}, {"timeout": "x"}, None):
        with pytest.raises(RuntimeError):
            stateful_prep_step_timeout(_FakeInfo(job_config=job_config))


# --------------------------------------------------------------------------- #
# 6. A setup failure still reaches log collection, and no unsafe timer is added.
# --------------------------------------------------------------------------- #
def test_collect_logs_is_not_gated_on_setup_success():
    """Failing prep fast only preserves the server logs if log collection runs
    regardless. `COLLECT_LOGS` is reached unguarded while the test stages sit
    behind `if res and ...`."""
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


def test_prep_adds_no_shell_timer_callers():
    """`Shell`'s own timer `killpg`s a process group that may already be
    recycled, so the bound must stay in the command."""
    source = inspect.getsource(ClickHouseProc.prepare_stateful_data)
    for unsafe in ("Shell.run(", "Shell.check("):
        for call in re.findall(re.escape(unsafe) + r"[^)]*\)", source, re.DOTALL):
            assert "timeout=" not in call, call


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
