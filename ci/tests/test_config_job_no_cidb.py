"""Tests for the config-time CIDB reachability invariant of the workflow filter hooks.

`Config Workflow` is declared with `timeout=1800` (`ci/praktika/native_jobs.py`), and
praktika marks every dependee job `DROPPED` when it fails. `CIDB.query` retries 5 times
with a 60s per-request timeout and exponential backoff, so one query can take
5 * 60 + (2 + 4 + 8 + 16) = 330s. The filter hooks run once per job
(`ci/praktika/native_jobs.py`), so a single CIDB call in `should_skip_job` is multiplied by
the number of jobs that reach it and can outlast the job's whole allowance - a transient
CIDB slowdown then voids the entire test matrix. Two thirds of that allowance are already
committed to populating the submodule cache (`SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC`),
which leaves 600s of slack.

The hooks must therefore issue no CIDB query at all. That is asserted here by
monkeypatching `requests.post` in `ci.praktika.cidb` (the single network boundary of
every `CIDB` method) and driving the hooks over the real workflows' own job lists, so a
newly added parametrization is covered without editing this file.
"""

import os
import pathlib
import shlex
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

import ci.praktika.cidb as cidb_module
import ci.jobs.scripts.workflow_hooks.filter_job as fj
import ci.jobs.scripts.workflow_hooks.new_tests_check as ntc
from ci.jobs.scripts.find_tests import Targeting
from ci.praktika.native_jobs import _is_praktika_job

PR_NUMBER = 105710
# Must name a file that exists: `Targeting.get_changed_tests` silently skips paths
# failing `Path(fpath).exists()`, which would empty the selection and make every
# assertion below run against the no-tests-changed state instead.
CHANGED_TEST = "tests/queries/0_stateless/04652_compound_key_monotonicity.sql"
CHANGED_TEST_NAME = "04652_compound_key_monotonicity."
FLAKY_CHECK_JOB = "Stateless tests (amd_debug, flaky check)"


class FakeInfo:
    """Stand-in for praktika's `Info`: only what the hooks read.

    `job_name` is the hook runner job, matching production - the hooks are called
    from `Config Workflow`, not from the job being filtered.
    """

    job_name = "Config Workflow"
    pr_number = PR_NUMBER
    pr_labels = ()
    pr_body = ""
    is_local_run = False

    def __init__(self, changed_files=(CHANGED_TEST,)):
        self._changed_files = list(changed_files)

    def get_kv_data(self, key):
        return self._changed_files if key == "changed_files" else None

    def get_changed_files(self):
        return self._changed_files

    def add_workflow_note(self, message):
        pass

    def get_secret(self, name):
        # Resolvable without CI credentials, so a reintroduced CIDB call reaches the
        # request boundary `_record_cidb_requests` watches instead of failing earlier on
        # secret lookup (which would make the guard pass for the wrong reason).
        return _FakeSecret(name)


class _FakeSecret:
    def __init__(self, name):
        self.name = name

    def join_with(self, other):
        return self

    def get_value(self):
        return ("http://cidb.invalid/", "user", "passwd")


def _use_fake_info(monkeypatch, changed_files=(CHANGED_TEST,)):
    info = FakeInfo(changed_files)
    monkeypatch.setattr(fj, "_info_cache", info)
    # `has_new_*_tests` and the release-branch guard construct their own `Info()`.
    monkeypatch.setattr(fj, "Info", lambda: info)
    monkeypatch.setattr(ntc, "Info", lambda: info)
    return info


def _record_cidb_requests(monkeypatch):
    """Return a list that collects every attempted CIDB HTTP request.

    Patched at `requests.post`, which `CIDB.query`, `insert_rows` and `check` all go
    through, so a CIDB call added through any other `CIDB` method is caught too.

    The call is RECORDED rather than only raised, because `should_skip_job` used to wrap
    its CIDB lookup in `except Exception` - an assertion raised here would be swallowed
    and the test would pass against the very code it must reject. Callers assert the
    list is empty after driving the hooks.
    """
    calls = []

    def _record(*args, **kwargs):
        calls.append(kwargs.get("url") or (args[0] if args else None))
        raise RuntimeError("CIDB unreachable")

    monkeypatch.setattr(cidb_module.requests, "post", _record)
    # The retry loop sleeps 2+4+8+16s per query; the assertions are about reachability,
    # not timing, so skip the wait.
    monkeypatch.setattr(cidb_module.time, "sleep", lambda _s: None)
    return calls


def _pr_workflow_job_names():
    from ci.workflows.pull_request import workflow

    return [j.name for j in workflow.jobs if not _is_praktika_job(j.name)]


def _merge_queue_job_names():
    from ci.workflows.merge_queue import workflow

    return [j.name for j in workflow.jobs if not _is_praktika_job(j.name)]


def test_pr_workflow_jobs_are_filtered_without_cidb(monkeypatch):
    info = _use_fake_info(monkeypatch)
    calls = _record_cidb_requests(monkeypatch)
    job_names = _pr_workflow_job_names()
    assert any(
        "flaky" in n.lower() for n in job_names
    ), "no flaky job in the PR workflow"
    # The fixture must actually select a test (it names a real file, and `Path` is
    # relative, so run from the repo root), otherwise every job below is filtered in
    # the no-tests-changed state and the changed-tests branch is never traversed.
    assert Targeting(info=info).get_changed_tests() == [CHANGED_TEST_NAME]
    assert fj.should_skip_job(FLAKY_CHECK_JOB) == (False, "")
    for name in job_names:
        should_skip, _ = fj.should_skip_job(name)
        assert isinstance(should_skip, bool), name
    assert calls == [], f"config-time CIDB requests: {calls}"


def test_every_workflow_using_the_hook_is_filtered_without_cidb(monkeypatch):
    """`should_skip_job` is the filter hook of four workflows, not just `PR`.

    Covers `master`, `release_branches` and `backport_branches` too, so a flaky-check
    parametrization added to any of them cannot reintroduce a config-time CIDB call.
    """
    for module in (
        "pull_request",
        "master",
        "release_branches",
        "backport_branches",
    ):
        workflow = __import__(f"ci.workflows.{module}", fromlist=["workflow"]).workflow
        fj._pipeline_note_labels.clear()
        _use_fake_info(monkeypatch)
        calls = _record_cidb_requests(monkeypatch)
        for job in workflow.jobs:
            if _is_praktika_job(job.name):
                continue
            fj.should_skip_job(job.name)
        assert calls == [], f"{module}: config-time CIDB requests: {calls}"


def test_merge_queue_jobs_are_filtered_without_cidb(monkeypatch):
    _use_fake_info(monkeypatch)
    calls = _record_cidb_requests(monkeypatch)
    for name in _merge_queue_job_names():
        should_skip, _ = fj.should_skip_merge_queue_job(name)
        assert isinstance(should_skip, bool), name
    assert calls == [], f"config-time CIDB requests: {calls}"


@pytest.mark.parametrize(
    "changed_files",
    [
        ("docs/en/development/continuous-integration.md",),
        ("utils/exclude-authors.txt",),
        ("src/Core/Settings.cpp",),
    ],
)
def test_build_profile_diff_is_not_filtered_by_the_hook(monkeypatch, changed_files):
    """The hook must not decide whether `Build profile diff` runs.

    Which changed files affect the job is expressed by its `digest_config`, so that a
    diff which cannot change the profiled build's output also skips the build itself
    instead of rescuing it through `requires`. A hook gate here would only see the
    changed files, not the build's own digest, and would drift from it. Scheduling is
    asserted in ci/tests/test_build_profile_diff_scheduling.py.
    """
    _use_fake_info(monkeypatch, changed_files=changed_files)

    assert fj.should_skip_job(fj.JobNames.BUILD_PROFILE_DIFF) == (False, "")


def test_cidb_outage_changes_no_filter_decision(monkeypatch):
    """A CIDB outage must degrade to "run the default set", not drop the matrix.

    Compares an unreachable CIDB against a healthy one that answers every query, so a
    reintroduced config-time lookup whose result differs between the two is caught even
    if it never raises.

    The changed file is deliberately *not* a stateless test: that is the only state in
    which a previously-failed lookup could ever change a decision (with a test changed,
    the job runs either way), so it is the only state where the two arms can differ.
    """
    no_tests_changed = ("src/Core/Settings.cpp",)

    _use_fake_info(monkeypatch, changed_files=no_tests_changed)
    _record_cidb_requests(monkeypatch)
    with_cidb_down = {n: fj.should_skip_job(n) for n in _pr_workflow_job_names()}

    fj._pipeline_note_labels.clear()
    _use_fake_info(monkeypatch, changed_files=no_tests_changed)
    monkeypatch.setattr(
        cidb_module.requests,
        "post",
        lambda *a, **kw: _OkResponse(
            f"{CHANGED_TEST_NAME.rstrip('.')}\n01666_merge_tree_max_query_limit\n"
        ),
    )
    with_cidb_up = {n: fj.should_skip_job(n) for n in _pr_workflow_job_names()}

    assert with_cidb_down == with_cidb_up


class _OkResponse:
    ok = True
    status_code = 200

    def __init__(self, text):
        self.text = text


def test_flaky_check_runs_when_stateless_tests_changed(monkeypatch):
    _use_fake_info(monkeypatch)
    calls = _record_cidb_requests(monkeypatch)
    monkeypatch.setattr(
        Targeting, "get_changed_tests", lambda self: [CHANGED_TEST_NAME]
    )
    assert fj.should_skip_job(FLAKY_CHECK_JOB) == (False, "")
    assert calls == [], f"config-time CIDB requests: {calls}"


def test_flaky_check_skipped_when_no_stateless_tests_changed(monkeypatch):
    _use_fake_info(monkeypatch)
    calls = _record_cidb_requests(monkeypatch)
    monkeypatch.setattr(Targeting, "get_changed_tests", lambda self: [])
    should_skip, reason = fj.should_skip_job(FLAKY_CHECK_JOB)
    assert should_skip is True
    assert reason == "Skipped, no tests to run"
    assert calls == [], f"config-time CIDB requests: {calls}"


def test_previously_failed_cannot_change_flaky_coverage():
    """Why dropping the previously-failed lookup loses no coverage.

    The flaky check selects `get_changed_tests` in-job and exits SKIPPED when that is
    empty, so the only row where previously-failed flipped the config decision
    scheduled a job that immediately self-skipped. Asserted structurally - both sides
    of the decision must consult that one selector, and neither may consult CIDB.
    """
    import ast
    import inspect

    import ci.jobs.functional_tests as functional_tests

    targeting_methods = {n for n in dir(Targeting) if not n.startswith("__")}

    # In-job side: every `Targeting` selector reached anywhere inside an
    # `is_flaky_check` branch must be `get_changed_tests`. The whole branch body is
    # walked, so `tests += ...` and `tests.extend(...)` count too; non-`Targeting`
    # calls (`join`, `append`) are filtered out.
    selectors = set()
    for node in ast.walk(ast.parse(inspect.getsource(functional_tests))):
        if not isinstance(node, ast.If):
            continue
        if not (isinstance(node.test, ast.Name) and node.test.id == "is_flaky_check"):
            continue
        for stmt in node.body:
            for sub in ast.walk(stmt):
                if (
                    isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr in targeting_methods
                ):
                    selectors.add(sub.func.attr)
    assert selectors == {"get_changed_tests"}, selectors

    # Shape-independent backstop: the scan above only enters `if is_flaky_check:`, so a
    # hoist into the enclosing `if is_flaky_check or is_bugfix_validation:` escapes it.
    # Satisfiable because the targeted check reads `get_all_relevant_tests_with_info`.
    module_targeting_calls = {
        n.func.attr
        for n in ast.walk(ast.parse(inspect.getsource(functional_tests)))
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr in targeting_methods
    }
    assert "get_previously_failed_tests" not in module_targeting_calls, (
        module_targeting_calls
    )

    # Config-time side: the same selector, and no CIDB-backed one. Scoped to the hook
    # this PR changed, so an unrelated mention elsewhere in the module cannot mask it.
    hook_calls = {
        n.func.attr
        for n in ast.walk(ast.parse(inspect.getsource(fj.should_skip_job)))
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "get_changed_tests" in hook_calls
    assert "get_previously_failed_tests" not in hook_calls


def test_integration_flaky_check_needs_no_cidb(monkeypatch):
    _use_fake_info(monkeypatch, changed_files=("src/Core/Settings.cpp",))
    calls = _record_cidb_requests(monkeypatch)
    should_skip, reason = fj.should_skip_job(
        "Integration tests (amd_asan_ubsan, flaky)"
    )
    assert should_skip is True
    assert reason == "Skipped, no integration tests updates"
    assert calls == [], f"config-time CIDB requests: {calls}"


def test_get_changed_tests_issues_no_cidb_query(monkeypatch):
    """`get_changed_tests` reads the diff only, so it is safe at config time.

    Pinned because the fix relies on it: it is the single selector both the config-time
    skip and the in-job selection use.
    """
    info = _use_fake_info(monkeypatch)
    calls = _record_cidb_requests(monkeypatch)
    assert Targeting(info=info).get_changed_tests() == [CHANGED_TEST_NAME]
    assert calls == [], f"config-time CIDB requests: {calls}"


def test_ci_script_change_keeps_sequential_selected_tests_job(monkeypatch):
    _use_fake_info(monkeypatch, changed_files=("ci/jobs/functional_tests.py",))

    assert fj.should_skip_job(
        "Stateless tests (amd_tsan, sequential, selected tests)"
    ) == (False, "")
    assert fj.should_skip_job("Stateless tests (amd_tsan, sequential)") == (
        True,
        "Skipped: only CI scripts changed; running stateless batch 1 only",
    )


def test_config_job_outlives_the_submodule_cache_bound():
    """The job cap must exceed the population budget, or the job watchdog fires first.

    The outer watchdog covers the whole job command and starts before any configuration
    work, while the budget starts only when the population does; if the two are equal the
    job is killed with no result instead of reporting which step ran out of time.
    """
    from ci.praktika.native_jobs import _workflow_config_job
    from ci.praktika.settings import Settings

    # The rest of the job needs its own room inside the cap: the configuration work that
    # already ran before the population started.
    assert (
        _workflow_config_job.timeout - Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
        >= 600
    ), (_workflow_config_job.timeout, Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC)


def _population_deadlines_after_job_elapsed(job_elapsed_sec, spend_offered=False):
    """Collect the deadlines the population hands out `job_elapsed_sec` into the job.

    With `spend_offered` every bounded step consumes exactly the deadline it was given,
    which is the worst case the outer watchdog has to survive: a step is entitled to all
    the time it is offered, so a deadline is a promise about the job's total.
    """
    import ci.praktika.native_jobs as nj

    offered = []
    clock = [0.0]

    class _FakeStopwatch:
        start_time = 0.0

        @property
        def duration(self):
            return clock[0]

    def _record(command):
        parts = command.split()
        if parts[:3] == ["timeout", "-s", "KILL"]:
            offered.append(int(parts[3]))
            if spend_offered:
                clock[0] += int(parts[3])

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            _record(command)
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            if timeout is not None:
                offered.append(timeout)
                if spend_offered:
                    clock[0] += timeout
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            if timeout is not None:
                offered.append(timeout)
                if spend_offered:
                    clock[0] += timeout
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _FakeStopwatch
        nj._MonotonicStopwatch = _FakeStopwatch
        nj._prepare_submodule_cache(None, _Cfg(), job_elapsed_sec=job_elapsed_sec)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch = orig
        nj._MonotonicStopwatch = orig_monotonic
    return offered, clock[0]


def test_the_elapsed_job_time_is_read_from_the_running_job():
    """The clamp has to consult the job's own clock, not a default of zero.

    Nothing else supplies the elapsed time in production: the caller passes no argument, so
    a default that reports a fresh job would leave the clamp arithmetically correct and
    permanently inert.
    """
    import ci.praktika.native_jobs as nj

    original = nj._JOB_STOPWATCH
    try:
        # A job that started 900s ago. Only a clamp that reads this shrinks its budget.
        nj._JOB_STOPWATCH = type(
            "_SW", (), {"start_time": 0.0, "duration": property(lambda _self: 900.0)}
        )()
        late, _ = _population_deadlines_after_job_elapsed(None)
    finally:
        nj._JOB_STOPWATCH = original

    on_time, _ = _population_deadlines_after_job_elapsed(0)
    assert late[0] < on_time[0], (late, on_time)


def test_the_job_clock_cannot_report_a_job_as_younger_than_it_is():
    """The elapsed time must come from a clock the system cannot set backwards.

    The remainder is subtracted from a watchdog that counts real time, so a clock that
    can jump backwards reports a shrinking elapsed time and yields a remainder larger
    than the job has left, which is the overrun the clamp exists to prevent.
    """
    import ci.praktika.native_jobs as nj

    readings = [nj._JOB_STOPWATCH.duration for _ in range(50)]
    assert readings == sorted(readings), readings

    # Step every settable clock backwards, which is the case the readings above cannot
    # produce on a healthy host, and leave `time.monotonic` alone: it is the one source
    # the system cannot rewind. Both ends are exercised, since `start_time` is taken while
    # the clocks read high and `duration` while they read low, so a stopwatch measuring
    # from any of them at either end reports the job as younger than it is.
    stepped = [10_000.0, 10_000.0, 0.0, 0.0, 0.0, 0.0]

    def _settable_now():
        return stepped.pop(0) if stepped else 0.0

    class _SteppingDateTime:
        @staticmethod
        def now():
            return _SteppingDateTime(_settable_now())

        def __init__(self, value=0.0):
            self._value = value

        def timestamp(self):
            return self._value

    watch_type = type(nj._JOB_STOPWATCH)
    watch_module = sys.modules[watch_type.__module__]
    monkeypatch = pytest.MonkeyPatch()
    try:
        for module in (nj, watch_module):
            if hasattr(module, "datetime"):
                monkeypatch.setattr(module, "datetime", _SteppingDateTime)
            if hasattr(module, "time"):
                # A module-level stand-in, because `time.time` cannot be assigned on the
                # stdlib module the production code imports.
                monkeypatch.setattr(
                    module,
                    "time",
                    type(
                        "_Clocks",
                        (),
                        {
                            "time": staticmethod(_settable_now),
                            "monotonic": staticmethod(time.monotonic),
                        },
                    ),
                )
        watch = watch_type()
        elapsed = [watch.duration, watch.duration]
    finally:
        monkeypatch.undo()

    assert min(elapsed) >= 0.0, elapsed
    assert elapsed == sorted(elapsed), elapsed


def test_a_clock_stepped_back_mid_population_does_not_reopen_the_budget():
    """Spend already made cannot be given back by a backward clock correction.

    The deadlines are subtracted from a watchdog that counts real time, so a clock that
    jumps backwards while the steps run would report the budget as unspent and hand the
    remaining steps their full allowance again, past the cap the job actually has.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.native_jobs import _workflow_config_job

    offered = []
    real = [0.0]
    wall_offset = [0.0]

    class _WallClockThatSteps:
        """What `Utils.Stopwatch` reports: real time plus an offset that can jump."""

        start_time = 0.0

        @property
        def duration(self):
            return real[0] + wall_offset[0]

    class _RealMonotonic:
        """A monotonic clock: advances with real time and never carries the offset."""

        start_time = 0.0

        @property
        def duration(self):
            return real[0]

    def _spend(seconds):
        offered.append(seconds)
        real[0] += seconds
        if len(offered) == 2:
            wall_offset[0] = -100_000.0

    def _record(command):
        parts = command.split()
        if parts[:3] == ["timeout", "-s", "KILL"]:
            _spend(int(parts[3]))

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            _record(command)
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            if timeout is not None:
                _spend(timeout)
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            if timeout is not None:
                _spend(timeout)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = (nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch)
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _WallClockThatSteps
        nj._MonotonicStopwatch = _RealMonotonic
        nj._prepare_submodule_cache(None, _Cfg(), job_elapsed_sec=0.0)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch = orig
        nj._MonotonicStopwatch = orig_monotonic

    # Every step is entitled to the deadline it was offered, so their sum is what the
    # watchdog has to survive.
    assert sum(offered) <= _workflow_config_job.timeout, (sum(offered), offered)


def test_a_clock_stepped_forward_mid_population_does_not_exhaust_the_budget():
    """A clock jumping forward is not time the watchdog has counted.

    The budget is the job's own allowance, so treating a forward correction as spend
    would refuse deadlines the job can still honour and fail a healthy run.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.result import Result

    offered = []

    class _WallClockJumpedForward:
        start_time = 0.0

        @property
        def duration(self):
            return 3600.0

    class _RealMonotonic:
        start_time = 0.0

        @property
        def duration(self):
            return 1.0

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            parts = command.split()
            if parts[:3] == ["timeout", "-s", "KILL"]:
                offered.append(int(parts[3]))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            if timeout is not None:
                offered.append(timeout)
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            if timeout is not None:
                offered.append(timeout)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = (nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch)
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _WallClockJumpedForward
        nj._MonotonicStopwatch = _RealMonotonic
        result = nj._prepare_submodule_cache(None, _Cfg(), job_elapsed_sec=0.0)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch = orig
        nj._MonotonicStopwatch = orig_monotonic

    assert result.status == Result.Status.OK, (result.status, result.info)
    # One second of real time has passed, so no step may be reduced to the floor a
    # spent budget produces.
    assert min(offered) > 1, offered


def test_the_population_budget_shrinks_by_what_the_job_already_spent():
    """The budget must come from what the job has left, not from the setting alone.

    The import-time check compares two configured numbers, so it holds however long the
    preamble ran; the watchdog does not. A preamble of 900s plus a full 1200s budget is
    2100s of a 1800s job, which is the SIGTERM with no result that this path exists to
    replace.
    """
    from ci.praktika.settings import Settings

    on_time, _ = _population_deadlines_after_job_elapsed(0)
    assert on_time[0] <= Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC, on_time

    late, _ = _population_deadlines_after_job_elapsed(900)
    assert late[0] < on_time[0], (late, on_time)


@pytest.mark.parametrize("job_elapsed", [0, 184, 600, 915, 1500])
def test_the_population_leaves_the_job_room_to_report_its_own_overrun(job_elapsed):
    """The population must end early enough for the job to still publish a result.

    Every step is entitled to spend its whole deadline, so the deadlines answer for the
    job's total rather than for the population's share of it. Finishing exactly at the cap
    is not enough: the result still has to be written, and a watchdog that fires first
    leaves the matrix undefined.

    A preamble that has itself consumed the reserve is out of reach of any clamp, since the
    steps still need a deadline they can fire on; the parameters stop short of it.
    """
    from ci.praktika.native_jobs import _workflow_config_job

    _offered, spent = _population_deadlines_after_job_elapsed(
        job_elapsed, spend_offered=True
    )

    # A fixed margin rather than the constants the clamp itself uses: deriving the bound
    # from those would accept setting either of them to zero, and the room to report is
    # exactly what the outer watchdog does not leave. Above the confirmation's reserve, so
    # that reserve alone cannot satisfy it.
    assert job_elapsed + spent <= _workflow_config_job.timeout - 90, (
        job_elapsed,
        spent,
        _workflow_config_job.timeout,
    )


@pytest.mark.parametrize("job_elapsed", [1740, 1799, 1800, 10**6])
def test_a_job_with_no_time_left_does_not_start_populating(job_elapsed):
    """A preamble that ate the cap must fail without running the steps.

    Each step would start on a deadline that fires at once, so the population cannot
    succeed; attempting it anyway spends the time the job needs to report why, and the
    watchdog then kills it with no result at all.
    """
    import ci.praktika.native_jobs as nj

    offered, _ = _population_deadlines_after_job_elapsed(job_elapsed)
    assert offered == [], offered

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Digest, nj.GHAuth
    try:
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        result = nj._prepare_submodule_cache(
            None, cfg, job_elapsed_sec=float(job_elapsed)
        )
    finally:
        nj.Digest, nj.GHAuth = orig

    assert result.status == "FAIL", result.status
    assert cfg.submodule_cache_hash == "", cfg.submodule_cache_hash


@pytest.mark.parametrize("reserve", [-100, -1, 1200, 5000])
def test_a_confirmation_reserve_outside_its_range_is_refused(reserve):
    """The reserve is repository-overridable, so its range is enforced on import.

    A negative one is added to the remainder rather than held back from it, handing a step
    more time than the job has left and defeating the clamp; one as large as the budget
    leaves the steps before the confirmation nothing at all. Asserting the default is in
    range cannot show the check is reachable, so drive values that must be rejected.
    """
    import importlib

    import ci.praktika.settings as settings_module

    original = settings_module.Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC
    try:
        settings_module.Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC = reserve
        for name in [m for m in sys.modules if m.endswith("praktika.native_jobs")]:
            del sys.modules[name]
        with pytest.raises(AssertionError):
            importlib.import_module("ci.praktika.native_jobs")
    finally:
        settings_module.Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC = original
        for name in [m for m in sys.modules if m.endswith("praktika.native_jobs")]:
            del sys.modules[name]
        importlib.import_module("ci.praktika.native_jobs")


def test_the_configured_confirmation_reserve_is_inside_its_range():
    """The default has to satisfy the check that rejects an override."""
    from ci.praktika.settings import Settings

    assert (
        0
        <= Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC
        < Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    ), Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC


def test_a_clone_bound_that_crowds_the_job_cap_is_refused():
    """The two durations are set independently, so their relation is enforced on import.

    `SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC` is overridable per repository while the job cap
    is a literal, so nothing but this check stops an override from leaving the population
    unable to report its own overrun. Asserting the defaults agree cannot show the check is
    reachable, so drive a value that must be rejected.
    """
    import importlib

    import ci.praktika.settings as settings_module

    original = settings_module.Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    try:
        # Equal to the job cap: the outer watchdog would fire first.
        settings_module.Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = 1800
        for name in [m for m in sys.modules if m.endswith("praktika.native_jobs")]:
            del sys.modules[name]
        with pytest.raises(AssertionError):
            importlib.import_module("ci.praktika.native_jobs")
    finally:
        settings_module.Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = original
        for name in [m for m in sys.modules if m.endswith("praktika.native_jobs")]:
            del sys.modules[name]
        importlib.import_module("ci.praktika.native_jobs")


def test_submodule_cache_clone_is_bounded():
    """A clone that succeeds runs under `timeout` and is attempted once.

    An unbounded clone can outlast the job cap, which kills the job that computes the
    matrix and leaves no result naming the cause. `retries` must not be delegated to
    `Shell.check`, which rebuilds the deadline per attempt and so multiplies it.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    calls = []
    uploads = []
    heads = []

    class _FakeShell:
        @staticmethod
        def check(command, **kwargs):
            calls.append((command, kwargs))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            heads.append(timeout)
            return False

        @staticmethod
        def put(**kwargs):
            uploads.append(kwargs)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    # Substituted too: with ENABLE_SUBMODULE_CLONE_AUTH set, the clone's `env=` argument
    # mints a token, which would reach the network from here.
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        cfg = _Cfg()
        result = nj._prepare_submodule_cache(None, cfg)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert result.status == "OK", result.status
    assert cfg.submodule_cache_hash, "a successful population must publish its hash"
    clones = [(c, k) for c, k in calls if "submodule update" in c]
    assert len(clones) == 1, calls
    command, kwargs = clones[0]
    budget = Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    assert command.startswith("timeout -s KILL "), command
    # Whatever the clone got must come out of the budget rather than exceed it; the steps
    # before it have already spent some.
    clone_deadline = int(command.split()[3])
    assert 0 < clone_deadline <= budget, command
    # Catches a trailing `|| true`, which would mask the failing exit code.
    assert command.endswith("--jobs 64"), command
    # `Shell.check(retries=)` builds its deadline inside the retry loop, so delegating
    # there would let the attempts take `retries` budgets between them.
    assert kwargs.get("retries", 1) == 1, kwargs
    # A hash is only meaningful once the object it names exists, so the archive must be
    # uploaded, exactly once, and as a conditional create.
    assert len(uploads) == 1, uploads
    assert uploads[0].get("if_none_matched") is True, uploads[0]
    # `no_strict` would turn every upload error into a false success (see
    # run_command_with_retries); a lost conditional race is already exempted there.
    assert uploads[0].get("no_strict") in (None, False), uploads[0]
    # The upload's deadline covers its retries as a whole, so it has to fit the budget on
    # its own rather than being multiplied by the retry count.
    upload_deadline = uploads[0].get("timeout")
    assert upload_deadline, uploads[0]
    assert 0 < upload_deadline <= budget, uploads[0]
    # The archive is a pipeline, so the deadline has to come first and cover both sides:
    # applied to `tar` alone it leaves a stalled compressor running past the budget.
    # `pipefail` is what then makes a killed `tar` visible, since a pipeline's status is
    # otherwise the compressor's and that exits cleanly on a truncated stream.
    archives = [c for c, _ in calls if "tar -cf -" in c]
    assert len(archives) == 1, calls
    assert archives[0].startswith("timeout -s KILL "), archives[0]
    assert "set -o pipefail;" in archives[0], archives[0]
    # Shape checks cannot tell a wrapped pipeline from `timeout bash -c '...' | zstd`,
    # which supervises only the left side, so the pipe must be proven to be inside the
    # single quoted script that `timeout` runs.
    supervised = shlex.split(archives[0])
    assert supervised[:3] == ["timeout", "-s", "KILL"], archives[0]
    assert supervised[4:5] == ["bash"], archives[0]
    assert supervised[5:6] == ["-c"], archives[0]
    script = supervised[6]
    assert "|" in script, script
    assert script.startswith("set -o pipefail;"), script
    # Nothing may follow the supervised script: a pipe out here is outside the deadline.
    assert supervised[7:] == [], archives[0]
    # Every existence probe is bounded too: an unbounded one can outlast the whole budget
    # on its own, since the AWS client's own socket deadline is minutes long.
    assert heads, "the cache-miss probe must run"
    assert all(h and 0 < h <= budget for h in heads), heads


def test_the_producing_and_publishing_steps_are_bounded_by_the_shared_budget():
    """Each step that produces or publishes the archive carries a deadline from the budget.

    The clone is not the only step that can hang: the S3 probes and the upload are AWS
    client subprocesses whose own socket deadline is minutes long and is retried, so an
    unbounded one can outlast the budget by itself and let the job watchdog fire, which is
    the outcome the budget exists to prevent. Asserting only the clone leaves that open.

    Two calls are outside this scope, `GHAuth.auth` and the `Digest.get_submodule_shas`
    enumeration, because on the workflows this job runs for an earlier and larger call of
    the same thing is what a bound would have to cover. The two tests after this one pin
    those orderings, and the enumeration one records the workflows the ordering does not
    hold for, so the scope rests on them rather than on this wording.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    budget = Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    shell_deadlines = []
    s3_deadlines = []

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            # `rm -f` is bookkeeping on a local file and cannot hang on the network.
            if command.startswith("rm -f"):
                return True
            assert "timeout -s KILL " in command, command
            prefix = command[command.index("timeout -s KILL ") :].split()
            shell_deadlines.append(int(prefix[3]))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            s3_deadlines.append(timeout)
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            s3_deadlines.append(timeout)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        result = nj._prepare_submodule_cache(None, _Cfg())
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert result.status == "OK", result.status
    # `git submodule sync`, `git submodule init`, the clone and the archive.
    assert len(shell_deadlines) == 4, shell_deadlines
    # Both existence probes cannot run on a hit-free path that also uploads, so the count
    # is the miss probe, the upload and nothing else here.
    assert len(s3_deadlines) == 2, s3_deadlines
    for deadline in shell_deadlines + s3_deadlines:
        assert deadline and 0 < deadline <= budget, (shell_deadlines, s3_deadlines)


def test_the_budget_shrinks_as_the_population_spends_it():
    """Each step's deadline must be the remainder, not the whole budget again.

    Handing every step the full budget bounds no total: four steps of nearly the budget
    each still overrun the job cap, which is what the budget exists to prevent.
    Collaborators returning instantly cannot show the difference, so time is advanced
    between steps and the deadlines are required to decrease.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    budget = Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    step = 100
    clock = [0.0]
    deadlines = []

    class _FakeStopwatch:
        start_time = 0.0

        @property
        def duration(self):
            return clock[0]

    def _spend(deadline):
        deadlines.append(deadline)
        clock[0] += step

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            if command.startswith("rm -f"):
                return True
            marker = "timeout -s KILL "
            _spend(int(command[command.index(marker) + len(marker) :].split()[0]))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            _spend(timeout)
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            _spend(timeout)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    original_stopwatch = nj.Utils.Stopwatch
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _FakeStopwatch
        nj._MonotonicStopwatch = _FakeStopwatch
        result = nj._prepare_submodule_cache(None, _Cfg())
    finally:
        nj.Utils.Stopwatch = original_stopwatch
        nj._MonotonicStopwatch = orig_monotonic
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert result.status == "OK", result.status
    assert len(deadlines) >= 4, deadlines
    assert deadlines == sorted(deadlines, reverse=True), deadlines
    assert deadlines[-1] < deadlines[0], deadlines
    # A step starting `spent` seconds in may only have what is left, so a deadline plus
    # what was already spent before it never exceeds the budget.
    for index, deadline in enumerate(deadlines):
        assert deadline + index * step <= budget, (index, deadline, deadlines)


def test_the_auth_setup_bounds_its_own_token_call():
    """The helper's own external call must carry the deadline it was given.

    Accepting the deadline and then dropping it leaves the token request unbounded while
    every caller-side assertion still passes, and this call runs before any supervised step
    so nothing else would catch it.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    commands = []

    class _FakeShell:
        @staticmethod
        def get_output(command, **_kwargs):
            commands.append(command)
            return "token"

    orig = nj.Shell, nj.GHAuth, Settings.ENABLE_SUBMODULE_CLONE_AUTH
    try:
        nj.Shell = _FakeShell
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: True)})
        # The default is False, which returns before the token call is reached.
        Settings.ENABLE_SUBMODULE_CLONE_AUTH = True
        env = nj._submodule_auth_env(None, timeout=45)
        nj._submodule_auth_env(None)
    finally:
        nj.Shell, nj.GHAuth, Settings.ENABLE_SUBMODULE_CLONE_AUTH = orig

    assert "GIT_CONFIG_KEY_0" in env, env
    bounded, plain = commands
    assert bounded == "timeout -s KILL 45 gh auth token", bounded
    # Omitting it must leave the command alone, so other callers are unaffected.
    assert plain == "gh auth token", plain


def test_the_job_authenticates_before_the_population_can_reach_auth():
    """The population's own `GHAuth.auth` call is reached only after an earlier failure.

    The token is minted at most once per process, and the job mints it unconditionally
    before configuring anything, so on the ordinary path this call returns from the cache
    without touching the network. That earlier call is what a bound on authentication has
    to cover; bounding only this one would leave the dominant case unbounded.
    """
    import inspect

    import ci.praktika.gh_auth as gh_auth_module
    import ci.praktika.native_jobs as nj
    from ci.praktika.gh_auth import GHAuth

    source, first_line = inspect.getsourcelines(nj._config_workflow)
    unconditional = [
        first_line + offset
        for offset, line in enumerate(source)
        if "GHAuth.auth(" in line and "force=True" in line
    ]
    population = [
        first_line + offset
        for offset, line in enumerate(source)
        if "_prepare_submodule_cache(" in line
    ]
    assert unconditional and population, (unconditional, population)
    assert max(unconditional) < min(population), (unconditional, population)

    # Reached after a failure, cached after a success, so count the mints rather than the
    # calls: a helper that authenticated again on every call would satisfy the order above.
    # Both the mint and the login are replaced, or the login would reach GitHub for real
    # and this test would hang on exactly the stall the change is about.
    mints = []
    original = GHAuth._authenticated
    original_mint = GHAuth.__dict__["_get_access_token_from_lambda"]
    original_shell = gh_auth_module.Shell
    try:
        GHAuth._get_access_token_from_lambda = classmethod(
            lambda cls, name, region: mints.append(name) or "token"
        )
        gh_auth_module.Shell = type(
            "S", (), {"check": staticmethod(lambda *a, **k: True)}
        )
        GHAuth._authenticated = True
        assert GHAuth.auth(None, no_strict=True) is True
        assert mints == [], mints
        # The window this path is left with, and the only one worth bounding here.
        GHAuth._authenticated = False
        GHAuth.auth(None, no_strict=True)
        assert len(mints) == 1, mints
    finally:
        gh_auth_module.Shell = original_shell
        GHAuth._get_access_token_from_lambda = original_mint
        GHAuth._authenticated = original


def test_the_job_enumerates_submodules_before_the_population_can():
    """The digest pass runs the same enumeration before the population is reached.

    `Digest.get_submodule_shas` is unbounded, but the cache lookup that gates the population
    already runs it for every job digesting with submodules, so bounding only the
    population's call would leave that earlier and larger run to overrun the cap on its own.

    Holds for the workflows whose jobs digest with submodules, which is every cache-enabled
    one except `OPTIMIZE_WORKFLOWS_WITHOUT_SUBMODULE_DIGEST`. Those two reach the population
    with no earlier enumeration, so pin the set: a third one appearing is a workflow this
    reasoning has not been checked against.
    """
    import inspect

    import ci.praktika.digest as digest_module
    import ci.praktika.native_jobs as nj
    from ci.praktika.hook_cache import CacheRunnerHooks
    from ci.praktika.runtime import RunConfig
    from ci.workflows.pull_request import workflow

    source, first_line = inspect.getsourcelines(nj._config_workflow)
    lookup = [
        first_line + offset
        for offset, line in enumerate(source)
        if "CacheRunnerHooks.configure(" in line
    ]
    population = [
        first_line + offset
        for offset, line in enumerate(source)
        if "_prepare_submodule_cache(" in line
    ]
    assert lookup and population, (lookup, population)
    assert max(lookup) < min(population), (lookup, population)

    # The order above is not enough: it says the lookup runs first, not that it enumerates.
    # Driving it is what shows that, and the file hashing is replaced because it dominates
    # the runtime without being the subject.
    calls = []
    original_enumerate = digest_module.Digest.__dict__["get_submodule_shas"]
    original_file_digest = digest_module.Digest.__dict__["_calc_file_digest"]
    config = RunConfig(
        name=workflow.name,
        digest_jobs={},
        digest_dockers={docker.name: "0" * 12 for docker in workflow.dockers},
        sha="0" * 40,
        cache_success=[],
        cache_success_base64=[],
        cache_artifacts={},
        cache_jobs={},
        filtered_jobs={},
        custom_data={},
        submodule_cache_hash="",
    )
    try:
        digest_module.Digest.get_submodule_shas = staticmethod(
            lambda: calls.append(1) or "sha"
        )
        digest_module.Digest._calc_file_digest = staticmethod(
            lambda path, hash_md5: hash_md5
        )
        # Inherited from `Serializable`, so shadow it here and drop the override to restore.
        RunConfig.from_fs = staticmethod(lambda _name: config)
        # The lookup itself reads S3; skipping it is the path that still digests everything.
        CacheRunnerHooks.configure(workflow, skip_lookup=True)
    finally:
        del RunConfig.from_fs
        digest_module.Digest._calc_file_digest = original_file_digest
        digest_module.Digest.get_submodule_shas = original_enumerate

    assert calls, "the cache lookup must enumerate submodules, not just run first"


# Cache-enabled, yet no job in them digests with submodules, so the population's own
# enumeration is the first one and the ordering above does not cover them. Both only
# collect build profiles, so they never restore the archive they populate.
OPTIMIZE_WORKFLOWS_WITHOUT_SUBMODULE_DIGEST = ("OptimizeClickHouse", "OptimizeToolchain")


def test_the_workflows_without_an_earlier_enumeration_are_the_known_two():
    """Pin which cache-enabled workflows reach the population with no earlier enumeration.

    The population runs for every cache-enabled workflow, so a new one whose jobs do not
    digest with submodules would silently join a set the carve-out above does not reason
    about. Naming the set turns that into a failure here instead.
    """
    import importlib
    import pkgutil

    import ci.workflows

    without = []
    for module in sorted(m.name for m in pkgutil.iter_modules(ci.workflows.__path__)):
        workflow = getattr(
            importlib.import_module(f"ci.workflows.{module}"), "workflow", None
        )
        # The population is gated on the workflow's own cache being enabled.
        if workflow is None or not workflow.enable_cache:
            continue
        if not any(
            job.digest_config and job.digest_config.with_git_submodules
            for job in workflow.jobs
        ):
            without.append(workflow.name)

    assert sorted(without) == sorted(OPTIMIZE_WORKFLOWS_WITHOUT_SUBMODULE_DIGEST), without


def test_the_clone_deadline_accounts_for_authenticated_setup():
    """Preparing the clone's environment must be paid for before its deadline is set.

    Under `ENABLE_SUBMODULE_CLONE_AUTH` that preparation mints a token, which is network
    work with no deadline of its own. Python evaluates a call's arguments left to right, so
    a deadline computed in the command string while the environment is built in a later
    argument is already stale when the clone starts, and the reserve it was supposed to
    leave has been spent.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    clock = [0.0]
    deadlines = {}

    class _FakeStopwatch:
        start_time = 0.0

        @property
        def duration(self):
            return clock[0]

    def _fake_auth_env(_workflow, timeout=None):
        # The token round trip, as elapsed time rather than a real request.
        deadlines["setup"] = timeout
        clock[0] += 300
        return {}

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            if "submodule update" in command:
                deadlines["clone"] = int(command.split()[3])
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = nj.Shell, nj.S3, nj.Digest, nj._submodule_auth_env
    original_stopwatch = nj.Utils.Stopwatch
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj._submodule_auth_env = _fake_auth_env
        nj.Utils.Stopwatch = _FakeStopwatch
        nj._MonotonicStopwatch = _FakeStopwatch
        nj._prepare_submodule_cache(None, _Cfg())
    finally:
        nj.Utils.Stopwatch = original_stopwatch
        nj._MonotonicStopwatch = orig_monotonic
        nj.Shell, nj.S3, nj.Digest, nj._submodule_auth_env = orig

    # The setup's own external call has to be bounded too: it runs before the clone, so
    # nothing else is supervising it.
    assert deadlines.get("setup"), deadlines
    assert 0 < deadlines["setup"] <= Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC, (
        deadlines
    )
    assert "clone" in deadlines, deadlines
    # The 300s the setup spent must already be gone from the clone's allowance.
    assert deadlines["clone"] <= Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC - 300, (
        deadlines,
        Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC,
    )


def test_an_exhausted_budget_still_yields_a_deadline_that_fires():
    """A spent budget must not be handed to `timeout` as zero, which disables it.

    `timeout 0 CMD` runs CMD with no deadline at all, so clamping the remainder upwards is
    what keeps an overrun reported rather than unbounded.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    deadlines = []

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            if command.startswith("rm -f"):
                return True
            prefix = command[command.index("timeout -s KILL ") :].split()
            deadlines.append(int(prefix[3]))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            deadlines.append(timeout)
            return False

        @staticmethod
        def put(timeout=None, **_kwargs):
            deadlines.append(timeout)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    original_budget = Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        # Already overspent before the first step, the state a slow earlier step leaves.
        Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = -60
        nj._prepare_submodule_cache(None, _Cfg())
    finally:
        Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = original_budget
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert deadlines, "the population must still run its steps"
    assert all(d >= 1 for d in deadlines), deadlines


def _population_with_scripted_clone(clone_fails, attempts=3, budget=1200, spend=0):
    """Run the population with a clone that fails its first `clone_fails` attempts.

    `spend` advances an injected clock by that many seconds per attempt, which is what
    makes a per-attempt deadline distinguishable from a shared one.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    seen = {"clones": 0}
    deadlines = []
    clock = [0.0]

    class _FakeStopwatch:
        start_time = 0.0

        @property
        def duration(self):
            return clock[0]

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            if "submodule update" not in command:
                return True
            seen["clones"] += 1
            deadlines.append(int(command.split()[3]))
            clock[0] += spend
            return seen["clones"] > clone_fails

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            return False

        @staticmethod
        def put(**_kwargs):
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    original_stopwatch = nj.Utils.Stopwatch
    orig_monotonic = nj._MonotonicStopwatch
    original_budget = Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
    original_attempts = Settings.SUBMODULE_CACHE_CLONE_ATTEMPTS
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _FakeStopwatch
        nj._MonotonicStopwatch = _FakeStopwatch
        Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = budget
        Settings.SUBMODULE_CACHE_CLONE_ATTEMPTS = attempts
        result = nj._prepare_submodule_cache(None, cfg)
    finally:
        Settings.SUBMODULE_CACHE_CLONE_ATTEMPTS = original_attempts
        Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC = original_budget
        nj.Utils.Stopwatch = original_stopwatch
        nj._MonotonicStopwatch = orig_monotonic
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig
    return result, cfg, seen["clones"], deadlines


def test_a_transient_clone_failure_does_not_lose_the_population():
    """A clone that fails once must be retried rather than failing the job.

    Population failure is fatal, so without a retry one refused connection drops the whole
    matrix. The rest of the repo retries submodule population for the same reason
    (`ci/jobs/fast_test.py`, `ci/jobs/build_clickhouse.py`).
    """
    result, cfg, clones, _deadlines = _population_with_scripted_clone(clone_fails=1)

    assert result.status == "OK", result.status
    assert clones == 2, clones
    assert cfg.submodule_cache_hash, "a rescued population must still publish its hash"


def test_the_clone_attempts_share_one_budget():
    """The attempts together must fit the budget, not take one deadline each.

    A per-attempt deadline bounds no total: `SUBMODULE_CACHE_CLONE_ATTEMPTS` is
    repository-overridable, so N attempts of the full budget would overrun the job cap by
    N-1 budgets and reproduce the SIGTERM this path exists to prevent. Every attempt
    spends a third of the budget here, so a shared deadline has to shrink.
    """
    result, cfg, clones, deadlines = _population_with_scripted_clone(
        clone_fails=99, attempts=6, budget=1200, spend=400
    )

    assert result.status == "FAIL", result.status
    assert cfg.submodule_cache_hash == "", cfg.submodule_cache_hash
    # Deadlines shrink by what earlier attempts spent, and the last one starts before the
    # budget is gone, so together they cannot exceed it.
    assert deadlines == sorted(deadlines, reverse=True), deadlines
    assert len(set(deadlines)) > 1, deadlines
    assert deadlines[0] <= 1200, deadlines
    for index, deadline in enumerate(deadlines):
        assert deadline + index * 400 <= 1200, (index, deadline, deadlines)
    # Fewer than configured: the budget, not the attempt count, is what stops it.
    assert clones < 6, clones


def test_the_configured_attempt_count_is_what_bounds_the_retries():
    """The loop must read `SUBMODULE_CACHE_CLONE_ATTEMPTS` rather than a fixed count.

    With budget to spare the setting is the only thing that stops the loop, so a
    hardcoded default would both refuse a repository that raised it and keep retrying one
    that lowered it. Each arm needs more attempts than the default to succeed, which a
    fixed three could not deliver.
    """
    # Spends nothing, so only the attempt count can end the loop.
    result, cfg, clones, _deadlines = _population_with_scripted_clone(
        clone_fails=4, attempts=5, spend=0
    )
    assert result.status == "OK", result.status
    assert clones == 5, clones
    assert cfg.submodule_cache_hash, cfg.submodule_cache_hash

    # Lowered below the default: the loop must stop early rather than retry to three.
    result, _cfg, clones, _deadlines = _population_with_scripted_clone(
        clone_fails=4, attempts=2, spend=0
    )
    assert result.status == "FAIL", result.status
    assert clones == 2, clones


def test_an_attempt_that_overspends_the_budget_is_not_followed_by_another():
    """Once the budget is gone the loop must stop instead of starting another attempt.

    An exhausted budget still yields a deadline of at least one second, so without this
    check a failing clone would keep being retried past the point the budget allows.
    """
    result, _cfg, clones, _deadlines = _population_with_scripted_clone(
        clone_fails=99, attempts=5, budget=600, spend=900
    )

    assert result.status == "FAIL", result.status
    assert clones == 1, clones


def test_submodule_cache_overrun_fails_closed():
    """A population that does not complete must fail, not report OK.

    Dependants clone from GitHub unauthenticated, which cannot reach a private submodule,
    so accepting the miss schedules a wave of checkout failures whose cause is no longer
    visible. It must also not publish a hash, which would make dependants unpack a
    truncated archive.
    """
    import ci.praktika.native_jobs as nj

    uploads = []

    class _FakeShell:
        @staticmethod
        def check(command, **kwargs):
            if "submodule update" in command:
                return False  # `timeout` fired and killed the clone
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            return False

        @staticmethod
        def put(s3_path, **_k):
            uploads.append(s3_path)
            return True

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        result = nj._prepare_submodule_cache(None, cfg)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert result.status == "FAIL", result.status
    assert cfg.submodule_cache_hash == "", cfg.submodule_cache_hash
    assert uploads == [], uploads


def test_the_upload_deadline_covers_its_retries(monkeypatch):
    """The deadline must bound the retry loop as a whole, not one attempt of it.

    `run_command_with_retries` makes every configured attempt, and `retries` is a
    repository-overridable setting with no upper bound, so a per-attempt deadline bounds no
    total: with one second left it would still permit `retries` seconds of work and let the
    job watchdog fire. Time is advanced by the fake shell so a per-attempt reading of the
    deadline shows up as attempts continuing past it.
    """
    import ci.praktika.s3 as s3mod
    from ci.praktika.settings import Settings

    attempts = []
    clock = [0.0]

    class _FakeShell:
        @staticmethod
        def get_res_stdout_stderr(command, **_kwargs):
            attempts.append(command)
            clock[0] += 30
            # Not one of the loop's break conditions, so it retries.
            return 1, "", "An error occurred (InternalError) calling PutObject"

    body = pathlib.Path(f"{Settings.TEMP_DIR}/retry_budget_probe.bin")
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_bytes(b"x")

    # `s3mod.time` is the stdlib module itself, so a fake clock has to be installed as a
    # module attribute rather than assigned onto `time.monotonic`.
    monkeypatch.setattr(s3mod, "time", type("_C", (), {"monotonic": lambda: clock[0]}))
    orig_shell = s3mod.Shell
    try:
        s3mod.Shell = _FakeShell
        with pytest.raises(RuntimeError):
            s3mod.S3.put(
                s3_path="bucket/key",
                local_path=str(body),
                # Room for two attempts of the 30s the fake shell burns, not for three.
                timeout=50,
            )
    finally:
        s3mod.Shell = orig_shell
        body.unlink(missing_ok=True)

    # A per-attempt reading of the deadline would run all MAX_RETRIES_S3 attempts; the
    # absolute one stops once the budget is gone.
    assert len(attempts) == 2, attempts
    assert len(attempts) < Settings.MAX_RETRIES_S3, (attempts, Settings.MAX_RETRIES_S3)
    # Each attempt may only have what is left of the budget, so the deadlines decrease and
    # none of them is the full amount again.
    granted = [int(a.split()[3]) for a in attempts]
    assert granted == [50, 20], granted


def test_s3_helpers_apply_the_deadline_to_the_aws_command():
    """The deadline has to reach the AWS subprocess, which is where the hang happens.

    Passing it to the helpers is only half the contract: an argument the helper accepts and
    then drops leaves the call unbounded while every caller-side assertion still passes.
    """
    import ci.praktika.s3 as s3mod
    from ci.praktika.settings import Settings

    commands = []

    class _FakeShell:
        @staticmethod
        def get_output(command, **_kwargs):
            commands.append(command)
            return ""

        @staticmethod
        def get_res_stdout_stderr(command, **_kwargs):
            commands.append(command)
            return 0, "", ""

    body = pathlib.Path(f"{Settings.TEMP_DIR}/deadline_probe.bin")
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_bytes(b"x")

    orig = s3mod.Shell
    try:
        s3mod.Shell = _FakeShell
        s3mod.S3.head_object("bucket/key", timeout=37)
        s3mod.S3.put(s3_path="bucket/key", local_path=str(body), timeout=41)
        # Omitting it must leave the command alone, so existing callers are unaffected.
        s3mod.S3.head_object("bucket/key")
    finally:
        s3mod.Shell = orig
        body.unlink(missing_ok=True)

    head_bounded, put_bounded, head_plain = commands
    assert head_bounded.startswith("timeout -s KILL 37 aws "), head_bounded
    # The upload's deadline is absolute, so its attempt gets what is left of it: at most
    # what was asked for, and never more.
    put_prefix = put_bounded.split()
    assert put_prefix[:3] == ["timeout", "-s", "KILL"], put_bounded
    assert 0 < int(put_prefix[3]) <= 41, put_bounded
    assert put_prefix[4] == "aws", put_bounded
    assert head_plain.startswith("aws "), head_plain


def _run_cache_population_with_upload_error(aws_stderr, object_exists_after):
    """Drive `_prepare_submodule_cache` over a failing upload.

    The real `S3.put` runs, so how it classifies the AWS error is part of what is under
    test; only the shell beneath it and the existence probe are substituted.
    """
    import ci.praktika.native_jobs as nj
    import ci.praktika.s3 as s3mod
    from ci.praktika.settings import Settings

    archive = pathlib.Path(f"{Settings.TEMP_DIR}/submodules_ba7816bf8f01cfea.tar.zst")
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_bytes(b"x")

    heads = []

    class _FakeShell:
        @staticmethod
        def check(_command, **_kwargs):
            return True

        @staticmethod
        def get_res_stdout_stderr(_command, **_kwargs):
            return 1, "", aws_stderr

    class _FakeS3(nj.S3):
        @staticmethod
        def head_object(path, timeout=None):
            # False on the first call, the cache-miss probe; the later call is the
            # publishability check on a refused conditional write.
            heads.append((path, timeout))
            return object_exists_after if len(heads) > 1 else False

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth, s3mod.Shell
    try:
        nj.Shell = _FakeShell
        s3mod.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        return nj._prepare_submodule_cache(None, cfg), cfg, heads
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth, s3mod.Shell = orig
        archive.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "aws_stderr",
    [
        "An error occurred (AccessDenied) calling PutObject",
        # A refused conditional write. AWS also raises it for a concurrent delete, so it
        # does not by itself mean the object is there.
        "An error occurred (ConditionalRequestConflict) calling PutObject",
        "An error occurred (PreconditionFailed) calling PutObject",
    ],
)
def test_an_upload_that_stored_nothing_publishes_no_hash(aws_stderr):
    """A failed upload must not publish a hash naming an object that is not there.

    Every one of these makes `S3.put` return False or raise, and reading a bare False as
    somebody else's success schedules dependants that restore nothing.
    """
    result, cfg, _heads = _run_cache_population_with_upload_error(
        aws_stderr, object_exists_after=False
    )

    assert result.status == "FAIL", result.status
    assert cfg.submodule_cache_hash == "", cfg.submodule_cache_hash
    assert "concurrently" not in (result.info or ""), result.info


def test_a_lost_race_is_a_success_once_the_object_is_there():
    """The write-once race must stay a success, or a normal collision fails the run.

    Two jobs that both saw a cache miss is ordinary, and the loser's key is already
    populated by the winner, so it has nothing left to do.
    """
    from ci.praktika.settings import Settings

    result, cfg, heads = _run_cache_population_with_upload_error(
        "An error occurred (ConditionalRequestConflict) calling PutObject",
        object_exists_after=True,
    )

    assert result.status == "OK", result.status
    assert cfg.submodule_cache_hash == "ba7816bf8f01cfea", cfg.submodule_cache_hash
    assert "concurrently" in (result.info or ""), result.info
    # This is the only path that reaches the second probe, so it is the only place the
    # confirmation's own deadline can be observed.
    assert len(heads) == 2, heads
    confirm_deadline = heads[1][1]
    assert confirm_deadline, heads
    assert 0 < confirm_deadline <= Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC, heads


def _population_over_a_lost_race():
    """Drive the population to a refused conditional write with the budget spent down.

    Every bounded step consumes exactly the deadline it was handed, which is both the worst
    case and the only physical one: a step that outran its deadline would have been killed.
    Returns the deadline the confirmation was given, which is what decides whether a lost
    race is read as somebody else's success or as nothing having been stored.
    """
    import ci.praktika.native_jobs as nj

    heads = []
    clock = [0.0]

    class _FakeStopwatch:
        start_time = 0.0

        @property
        def duration(self):
            return clock[0]

    class _FakeShell:
        @staticmethod
        def check(command, **_kwargs):
            parts = command.split()
            if parts[:3] == ["timeout", "-s", "KILL"]:
                clock[0] += int(parts[3])
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            heads.append(timeout)
            if timeout is not None:
                clock[0] += timeout if len(heads) == 1 else 0
            # False on the cache-miss probe; the later call is the confirmation, which a
            # deadline of a second or two cannot complete.
            return len(heads) > 1 and timeout > 2

        @staticmethod
        def put(timeout=None, **_kwargs):
            if timeout is not None:
                clock[0] += timeout
            return False

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch
    orig_monotonic = nj._MonotonicStopwatch
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        nj.Utils.Stopwatch = _FakeStopwatch
        nj._MonotonicStopwatch = _FakeStopwatch
        result = nj._prepare_submodule_cache(None, cfg)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth, nj.Utils.Stopwatch = orig
        nj._MonotonicStopwatch = orig_monotonic
    return result, cfg, heads


def test_a_lost_race_survives_a_budget_the_upload_nearly_emptied():
    """The confirmation must keep working time after the steps before it spent the budget.

    Population failure is fatal, so a confirmation squeezed to a second turns an ordinary
    concurrent writer collision into a dropped matrix, even though the winner already stored
    the archive.
    """
    result, cfg, heads = _population_over_a_lost_race()

    assert result.status == "OK", result.status
    assert cfg.submodule_cache_hash, cfg.submodule_cache_hash
    assert len(heads) == 2, heads
    assert heads[1] > 2, heads


@pytest.mark.parametrize("job_elapsed", [0, 1710, 1739])
def test_a_step_starting_inside_the_reserve_still_gets_a_firing_deadline(job_elapsed):
    """Holding the reserve back must not hand a later step a deadline of zero.

    Subtracting the reserve reaches zero a whole reserve before the budget does, and
    `timeout 0` enforces no deadline at all, so the very steps this bound exists to cover
    would be the ones running unbounded. Reached two ways: a budget spent down to inside the
    reserve, and a job whose whole remainder is already smaller than it.
    """
    from ci.praktika.settings import Settings

    # Spends everything it is offered, so later steps start inside the reserve.
    offered, _ = _population_deadlines_after_job_elapsed(
        job_elapsed, spend_offered=True
    )

    assert len(offered) > 1, offered
    assert all(deadline >= 1 for deadline in offered), offered
    assert min(offered) <= Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC, offered


def test_the_producing_steps_never_get_the_confirmation_reserve():
    """The reserve only works if the steps before it cannot spend it.

    Reserving time the upload is still free to consume reserves nothing, so the deadline
    offered to a producing step has to fall short of the remaining budget by the reserve.
    """
    from ci.praktika.settings import Settings

    offered, _ = _population_deadlines_after_job_elapsed(0)

    assert offered, offered
    assert (
        offered[0]
        == Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
        - Settings.SUBMODULE_CACHE_CONFIRM_RESERVE_SEC
    ), offered


def test_the_reserve_does_not_excuse_an_unconfirmable_upload():
    """A refused write that cannot be confirmed must still fail, reserve or not.

    The reserve buys the confirmation time to answer, never permission to skip it: reading
    a bare refusal as somebody else's success publishes a hash naming an object that may
    not be there.
    """
    import ci.praktika.native_jobs as nj

    class _FakeShell:
        @staticmethod
        def check(_command, **_kwargs):
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p, timeout=None):
            return False

        @staticmethod
        def put(**_kwargs):
            return False

    class _Cfg:
        submodule_cache_hash = ""

        def dump(self):
            pass

    cfg = _Cfg()
    orig = nj.Shell, nj.S3, nj.Digest, nj.GHAuth
    try:
        nj.Shell = _FakeShell
        nj.S3 = _FakeS3
        nj.Digest = type("D", (), {"get_submodule_shas": staticmethod(lambda: "abc")})
        nj.GHAuth = type("A", (), {"auth": staticmethod(lambda *a, **k: False)})
        result = nj._prepare_submodule_cache(None, cfg)
    finally:
        nj.Shell, nj.S3, nj.Digest, nj.GHAuth = orig

    assert result.status == "FAIL", result.status
    assert cfg.submodule_cache_hash == "", cfg.submodule_cache_hash


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
