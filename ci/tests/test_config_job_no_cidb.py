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
import sys

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
    """The job cap must exceed the clone's own bound, or the job watchdog fires first.

    The outer watchdog covers the whole job command and starts before any configuration
    work, while the clone's bound starts only when the clone does; if the two are equal the
    job is killed with no result instead of reporting which step ran out of time.
    """
    from ci.praktika.native_jobs import _workflow_config_job
    from ci.praktika.settings import Settings

    # The rest of the job needs its own room inside the cap: the archive upload plus
    # the configuration work that already ran before the clone started.
    assert (
        _workflow_config_job.timeout - Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC
        >= 600
    ), (_workflow_config_job.timeout, Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC)


def test_a_clone_bound_that_crowds_the_job_cap_is_refused():
    """The two durations are set independently, so their relation is enforced on import.

    `SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC` is overridable per repository while the job cap
    is a literal, so nothing but this check stops an override from leaving the clone unable
    to report its own overrun. Asserting the defaults agree cannot show the check is
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


def test_submodule_cache_clone_is_bounded_and_not_retried():
    """The cache-population clone must run under `timeout` exactly once.

    An unbounded or retried clone can outlast the job cap, which kills the job that
    computes the matrix and leaves no result naming the cause.
    """
    import ci.praktika.native_jobs as nj
    from ci.praktika.settings import Settings

    calls = []
    uploads = []

    class _FakeShell:
        @staticmethod
        def check(command, **kwargs):
            calls.append((command, kwargs))
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p):
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
    assert command.startswith(
        f"timeout -s KILL {Settings.SUBMODULE_CACHE_POPULATE_TIMEOUT_SEC} "
    ), command
    # Catches a trailing `|| true`, which would mask the exit code before `strict` sees it.
    assert command.endswith("--jobs 64"), command
    assert kwargs.get("strict") is True, kwargs
    assert kwargs.get("retries", 1) == 1, kwargs
    # A hash is only meaningful once the object it names exists, so the archive must be
    # uploaded, exactly once, and as a conditional create.
    assert len(uploads) == 1, uploads
    assert uploads[0].get("if_none_matched") is True, uploads[0]
    # `no_strict` would turn every upload error into a false success (see
    # run_command_with_retries); a lost conditional race is already exempted there.
    assert uploads[0].get("no_strict") in (None, False), uploads[0]


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
                # `timeout` fired; `strict=True` turns the non-zero exit into a raise.
                raise RuntimeError("command failed, exit code 137")
            return True

    class _FakeS3:
        @staticmethod
        def head_object(_p):
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
        def head_object(path):
            # False on the first call, the cache-miss probe; the later call is the
            # publishability check on a refused conditional write.
            heads.append(path)
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
        return nj._prepare_submodule_cache(None, cfg), cfg
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
    result, cfg = _run_cache_population_with_upload_error(
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
    result, cfg = _run_cache_population_with_upload_error(
        "An error occurred (ConditionalRequestConflict) calling PutObject",
        object_exists_after=True,
    )

    assert result.status == "OK", result.status
    assert cfg.submodule_cache_hash == "ba7816bf8f01cfea", cfg.submodule_cache_hash
    assert "concurrently" in (result.info or ""), result.info


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
