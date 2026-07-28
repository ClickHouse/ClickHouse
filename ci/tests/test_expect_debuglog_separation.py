"""
Guards for the expect-trace / bash-xtrace separation in ``tests/clickhouse-test``.

Background
----------
``clickhouse-test`` used to export ``CLICKHOUSE_BASH_TRACING_FILE`` pointing at
``<basename>.debuglog`` - the very path every ``.expect`` test hands to
``exp_internal -f`` - and ``tests/queries/shell_config.sh`` opened it with ``O_TRUNC`` on
every ``source``.  An ``.expect`` test spawns bash once per ``spawn``, so bash truncated the
file underneath expect and only the last spawn's record survived.  Six couplings keep that
from coming back, and all six are invisible to any functional test:

1. the two artifacts must live at two different paths;
2. the two report blocks must be trimmed independently, the expect trace at the shared
   default and the xtrace at its historical 100-line bound (``set -x`` emits ~2 lines per
   loop iteration, and some ``.sh`` tests loop millions of times);
3. the xtrace block must be suppressed when the file is EMPTY - ``shell_config.sh`` opens it on
   being sourced and writes only once a traced command runs, so a zero-length one carries no
   diagnostics, and reporting its name would add a bare header with an empty body;
4. the xtrace fd must be redirected with ``>>`` in ``shell_config.sh`` and the tracing file
   must be reset exactly once per run by ``clickhouse-test``: truncate-on-source loses every
   spawn but the last, while append preserves them but, without the per-run reset, makes a
   re-run report carry earlier runs' traces;
5. the expect trace must likewise be dropped at launch - ``exp_internal -f`` appends too, and
   bash no longer truncates that path - and NEITHER reset may be gated on ``--trace``, since
   both report blocks key on the files rather than on this run's flags;
6. each artifact's trimmed contents must actually reach the reported description - a block that
   is trimmed and then discarded has the same user-visible effect as a truncated file.

The assertions below read the real code (``runpy`` / ``inspect.getsource`` / the real
``shell_config.sh``) rather than a copy of it, so they fail if any of the six couplings is
undone.  Where a coupling has an observable EFFECT it is asserted by effect rather than by the
presence of a statement: locating a statement proves nothing about whether its result is used, and
locating a reset proves nothing about whether it runs before the test is launched.  So the real
``run_single_test`` is driven with a capturing ``Popen`` stub (what does the child actually
receive, and what do the two artifact files look like at the instant it starts?), and the real
``process_result_impl`` is driven over real artifact files on the STDERR, the TIMEOUT and the
EXIT_CODE return, with payloads sized to straddle the two trim limits.  Those three cover the
three ways a ``proc`` reaches the function - absent, still running, exited non-zero - and so the
three appends a failing ``.expect`` test can hit; the remaining three (EXCEPTION, RESULT_DIFF,
TOO_LONG) need stdout patterns or a reference file that no ``.expect`` test has.  The structural
checks are kept alongside, because each catches mutations the other is blind to.
"""

import ast
import inspect
import os
import re
import runpy
import subprocess
import textwrap
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")
_SHELL_CONFIG = _REPO_ROOT / "tests" / "queries" / "shell_config.sh"

_ct = runpy.run_path(_CLICKHOUSE_TEST)
# Bound under a non-Test* name: pytest would otherwise try to collect it as a test class.
ClickHouseTestCase = _ct["TestCase"]
trim_for_log = _ct["trim_for_log"]
_TestStatus = _ct["TestStatus"]
_FailureReason = _ct["FailureReason"]
# `runpy.run_path` returns a COPY of the executed namespace, while the functions defined in it
# close over the original dict - so patching has to go through a function's own `__globals__`,
# which is that original and is what the function actually reads.
_CT_GLOBALS = ClickHouseTestCase.run_single_test.__globals__

_TRIM_DEFAULT_LIMIT = inspect.signature(trim_for_log).parameters["limit"].default


_HIDDEN_RE = re.compile(r"-{10,}\d+ lines are hidden-{10,}")


def _artifact_blocks(description, tmp_path):
    """Split a failure description into its two artifact blocks, keyed by suffix.

    Each block opens with its own ``<path>:`` header line - that header is part of what
    ``trim_for_log`` is handed, so it counts towards the limit and survives a trim - and the xtrace
    block is appended last, so slicing at the two headers recovers exactly what each
    ``trim_for_log`` call returned.
    """
    expect_header = str(tmp_path / "04615_some_test.expect.debuglog") + ":\n"
    bash_header = str(tmp_path / "04615_some_test.expect.bashlog") + ":\n"
    expect_at = description.index(expect_header)
    bash_at = description.index(bash_header)
    assert expect_at < bash_at, "the xtrace block is no longer appended after the expect trace"
    return {
        "debuglog": description[expect_at:bash_at],
        "bashlog": description[bash_at:],
    }


def _trimmed_line_count(raw, limit):
    """How many lines ``trim_for_log(raw, limit)`` returns, from its own arithmetic.

    ``trim_for_log`` keeps ``lines[:limit//2] + [separator] + lines[-limit//2:]``, so a trimmed
    block carries ``2 * (limit // 2) + 1`` lines regardless of how large the input was.  Derived
    rather than hardcoded, so the assertions below cannot drift away from the function.
    """
    lines = raw.splitlines()
    if len(lines) <= limit:
        return len(lines), False
    return 2 * (limit // 2) + 1, True


def _trim_limit(call_line, argname):
    """The line limit a captured ``trim_for_log`` call passes, as a number.

    An omitted second argument means the function's own declared default, so an explicit
    spelling of that default compares equal to omitting it.  Comparing numerically also keeps
    the assertions from being satisfied by a substring: ``"100" in "10000"`` is true.
    """
    match = re.search(rf"trim_for_log\({argname}(?:,\s*(\d+))?\)", call_line)
    assert match, call_line
    return int(match.group(1)) if match.group(1) else _TRIM_DEFAULT_LIMIT


def _configure(tmp_path, monkeypatch):
    """Drive the real ``configure_testcase_args`` for a case file, without a server.

    ``--database`` is set so the function takes the branch that reuses the suite tmp dir and
    never issues a CREATE DATABASE, so no server is needed.  That branch is not side-effect
    free though: it ``os.environ.setdefault``s ``CLICKHOUSE_DATABASE`` and ``CLICKHOUSE_TMP``,
    and ``ci/tests`` runs in a single pytest process whose later modules subprocess
    ``clickhouse-test`` with this environment.  Both are therefore pinned through
    ``monkeypatch``, which makes those setdefaults no-ops and restores the process
    environment at teardown.
    """
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "test_expect_debuglog_separation")
    monkeypatch.setenv("CLICKHOUSE_TMP", str(tmp_path))
    args = Namespace(
        cloud=False,
        database="test_expect_debuglog_separation",
        client="clickhouse-client",
        trace=True,
    )
    # `self` is only dereferenced (for self.tags) on the args.cloud branch.
    return ClickHouseTestCase.configure_testcase_args(
        object(), args, str(tmp_path / "04615_some_test.expect"), str(tmp_path)
    )


def test_expect_trace_and_bash_xtrace_get_different_files(tmp_path, monkeypatch):
    testcase_args = _configure(tmp_path, monkeypatch)

    expect_path = testcase_args.debug_log_file
    bash_path = testcase_args.bash_tracing_file

    assert expect_path != bash_path, (
        "bash reopened this shared path with O_TRUNC on every source, discarding the records "
        "expect had appended"
    )
    assert expect_path.endswith("/04615_some_test.expect.debuglog"), expect_path
    assert bash_path.endswith("/04615_some_test.expect.bashlog"), bash_path
    # Same basename and same directory - only the suffix distinguishes them, so both are
    # still picked up by the artifact collection and the cleanup.
    assert os.path.dirname(expect_path) == os.path.dirname(bash_path)


def _source_shell_config(tmp_path, tracing_file, marker):
    """Source the REAL ``shell_config.sh`` in a subprocess and run one traced command.

    No server and no ``clickhouse`` binary are needed: the tracing block only reads
    ``CLICKHOUSE_BASH_TRACING_FILE`` and ``BASH_VERSINFO``, and everything above it in the script
    is variable setup.  Returns the subprocess result.
    """
    case = tmp_path / "04615_some_test.sh"
    case.write_text(
        '#!/usr/bin/env bash\n. "$SHELL_CONFIG_PATH"\n: XTRACE-MARKER-"$1"\n',
        encoding="utf-8",
    )
    env = dict(
        os.environ,
        CLICKHOUSE_BASH_TRACING_FILE=str(tracing_file),
        SHELL_CONFIG_PATH=str(_SHELL_CONFIG),
        CLICKHOUSE_TMP=str(tmp_path),
        CLICKHOUSE_DATABASE="test_expect_debuglog_separation",
    )
    return subprocess.run(
        ["bash", str(case), marker], env=env, capture_output=True, text=True, timeout=60
    )


def test_the_real_shell_config_appends_every_spawns_xtrace(tmp_path):
    # The one assertion in this module that exercises the WRITER rather than the wiring. Every
    # other check either reads source text or stubs the child launch, so removing `BASH_XTRACEFD=3`
    # or `set -x`, or breaking the version guard, produces no xtrace at all while leaving them all
    # green. Here the real script runs, so the file's contents are the evidence.
    tracing_file = tmp_path / "04615_some_test.sh.bashlog"

    first = _source_shell_config(tmp_path, tracing_file, "1")
    assert first.returncode == 0, first.stderr

    # Created by the redirection itself - which is what lets `clickhouse-test` REMOVE the file at
    # launch rather than having to leave an empty one behind.
    assert tracing_file.exists(), (
        "sourcing shell_config.sh produced no tracing file, so BASH_XTRACEFD is not wired up"
    )
    written = tracing_file.read_text(encoding="utf-8")
    assert "XTRACE-MARKER-1" in written, written
    # `set -x` writes to fd 3, NOT to stderr: a test's non-empty stderr is itself a failure
    # condition in `process_result_impl`, so a regression that drops the redirection would fail
    # every traced .sh test.
    assert "XTRACE-MARKER-1" not in first.stderr, first.stderr

    # The append half, over a second `source` of the same file - an .expect test reaches this by
    # spawning bash once per `spawn`. With a truncating `3>`, only the last spawn's trace survives.
    second = _source_shell_config(tmp_path, tracing_file, "2")
    assert second.returncode == 0, second.stderr
    both = tracing_file.read_text(encoding="utf-8")
    assert "XTRACE-MARKER-1" in both, both
    assert "XTRACE-MARKER-2" in both, both


def _launch(tmp_path, monkeypatch, on_launch=None, trace=True):
    """Drive the real ``run_single_test`` with ``Popen`` replaced by a capturing stub.

    Returns the ``env`` dict the stub was handed.  ``on_launch`` is called at the moment of the
    launch, before the stubbed child "finishes", so it can observe the state of the two artifact
    files as the real child would see them - which is what turns a presence check into an ordering
    proof.

    Everything the function mutates outside its own arguments is pinned: the six environment
    variables it writes go through ``monkeypatch.setenv``, the one it writes only under ``--trace``
    is removed with ``monkeypatch.delenv`` so an ambient value cannot pose as this run's export
    (``ci/tests`` runs in one pytest process and later modules subprocess ``clickhouse-test``), and
    ``_GROUP_PID_PATH`` is redirected into ``tmp_path`` so the group-pid file is not written into
    ``ci/tmp``.
    """
    testcase_args = _configure(tmp_path, monkeypatch)
    testcase_args.trace = trace
    testcase_args.secure = False
    testcase_args.memory_limit = 0
    testcase_args.timeout = 1
    testcase_args.hide_db_name = False
    testcase_args.replicated_database = False
    testcase_args.shared_catalog = False

    for mutated in (
        "CLICKHOUSE_CLIENT_OPT",
        "CLICKHOUSE_LOG_COMMENT",
        "TSAN_OPTIONS",
        "ASAN_OPTIONS",
        "MSAN_OPTIONS",
        "UBSAN_OPTIONS",
    ):
        monkeypatch.setenv(mutated, "")
    # `run_single_test` starts from `os.environ.copy()` and only ADDS this one under `--trace`, so
    # an ambient value would pass straight through and make the untraced test observe an export
    # this run never performed.
    monkeypatch.delenv("CLICKHOUSE_BASH_TRACING_FILE", raising=False)

    stdout_file = tmp_path / "04615_some_test.stdout"
    stdout_file.write_text("", encoding="utf-8")
    stderr_file = tmp_path / "04615_some_test.stderr"
    stderr_file.write_text("", encoding="utf-8")

    case = SimpleNamespace(
        testcase_args=testcase_args,
        args=testcase_args,
        tags=set(),
        ext=".expect",
        case_file=str(tmp_path / "04615_some_test.expect"),
        stdout_file=str(stdout_file),
        stderr_file=str(stderr_file),
    )

    captured = {}

    def popen_stub(command, **kwargs):
        # A COPY, deliberately: `tests_env` is a live dict, so capturing the reference would let an
        # export that runs after the launch appear to have been delivered to the child.
        env = kwargs.get("env")
        captured["env"] = dict(env) if env is not None else None
        if on_launch is not None:
            on_launch()
        # `os.getpid()` is a pid that certainly exists, so nothing here can ever signal a
        # stranger - but the stub never lets a real signal be sent in the first place: the only
        # consumer is `write_text_atomic`, and the timeout branch that would `killpg` lives in
        # `process_result_impl`, which this test does not call.
        return SimpleNamespace(pid=os.getpid(), returncode=0, wait=lambda _timeout: 0)

    monkeypatch.setitem(_CT_GLOBALS, "Popen", popen_stub)
    monkeypatch.setitem(_CT_GLOBALS, "_GROUP_PID_PATH", tmp_path)

    proc, _total_time = ClickHouseTestCase.run_single_test(case, "warning", "")
    assert proc is not None
    assert "env" in captured, "the launch was never reached"
    return testcase_args, captured["env"]


def _report(testcase_args):
    """Run the real ``process_result_impl`` over the artifacts a ``_launch`` left on disk.

    Unlike ``_describe`` below, which creates its own artifact files from literal text, this takes
    the ``testcase_args`` a launch actually configured - so the two functions are chained over one
    set of paths, which is what makes a cross-run leak observable.  A non-empty ``stderr_file`` and
    no reference file steer it onto the FAIL/STDERR return, which appends ``debug_log``.
    """
    directory = Path(testcase_args.debug_log_file).parent
    stdout_file = directory / "04615_some_test.stdout"
    stdout_file.write_text("", encoding="utf-8")
    stderr_file = directory / "04615_some_test.stderr"
    stderr_file.write_text("a non-empty stderr, so a FAIL path is taken\n", encoding="utf-8")
    stub = SimpleNamespace(
        args=Namespace(cloud=False),
        testcase_args=Namespace(
            debug_log_file=testcase_args.debug_log_file,
            bash_tracing_file=testcase_args.bash_tracing_file,
            stop=False,
            record=False,
            flaky_check=False,
            test_runs=1,
            unified=3,
        ),
        fatal_sanitizer_prefix=str(directory / "no-such-fatal-prefix"),
        stdout_file=str(stdout_file),
        stderr_file=str(stderr_file),
        name="04615_some_test.expect",
        reference_file=None,
        show_whitespaces_in_diff=False,
        suite=None,
        tags=set(),
    )
    return ClickHouseTestCase.process_result_impl(stub, None, 1.0)


def test_the_launched_child_is_told_the_dedicated_xtrace_path(tmp_path, monkeypatch):
    # Asserted on what the CHILD RECEIVES, because every text/AST check in this module is blind to
    # the three couplings that actually deliver it: `Popen` is handed `env=tests_env` (re-pointing
    # it at `os.environ` leaves every other assertion green while `shell_config.sh` is never told
    # where to write, so no xtrace is produced at all); the export sits ABOVE the launch; and each
    # reset runs before it. All three are one-line mutations that silently revert this PR.
    seen = {}

    def observe():
        # Inside the launch: exactly what the real child would find on disk.
        seen["bashlog_exists"] = os.path.exists(bash_seeded)
        seen["debuglog_exists"] = os.path.exists(expect_seeded)

    probe_args = _configure(tmp_path, monkeypatch)
    expect_seeded = probe_args.debug_log_file
    bash_seeded = probe_args.bash_tracing_file
    # Pre-seed BOTH artifacts with a stale marker, as a re-run under `--test-runs` would find them.
    Path(expect_seeded).write_text("STALE-EXPECT-4bd2\n", encoding="utf-8")
    Path(bash_seeded).write_text("STALE-XTRACE-91af\n", encoding="utf-8")

    testcase_args, env = _launch(tmp_path, monkeypatch, on_launch=observe)
    # The seeding above and the launch below configure the case independently, so pin that they
    # agree - otherwise the two observations would be about files the launch never touched.
    assert (testcase_args.debug_log_file, testcase_args.bash_tracing_file) == (
        expect_seeded,
        bash_seeded,
    )

    assert "CLICKHOUSE_BASH_TRACING_FILE" in env, (
        "the launched child is not told where to write its xtrace, so shell_config.sh writes none"
    )
    assert env["CLICKHOUSE_BASH_TRACING_FILE"] == testcase_args.bash_tracing_file, (
        env["CLICKHOUSE_BASH_TRACING_FILE"]
    )
    # The original collision, stated as the property that must NOT hold.
    assert env["CLICKHOUSE_BASH_TRACING_FILE"] != testcase_args.debug_log_file

    # ORDERING, by effect: at the instant the child started, BOTH stale artifacts were already
    # gone. A reset that runs after the launch resets nothing for this run and makes every re-run
    # report the previous run's trace.
    assert seen["bashlog_exists"] is False, (
        "the previous run's xtrace was still in place when the test was launched"
    )
    assert seen["debuglog_exists"] is False, (
        "the previous run's expect trace was still in place when the test was launched"
    )


def test_an_untraced_run_still_drops_both_stale_traces(tmp_path, monkeypatch):
    # `exp_internal -f` traces unconditionally, and the xtrace report block keys on the FILE rather
    # than on this run's flags - so NEITHER reset may depend on `--trace`. Companion to the AST
    # `--trace`-enclosure assertions below, by effect this time.
    #
    # The xtrace half is the trace-on -> trace-off rerun: a traced run fails (so the artifact
    # cleanup, which sits behind the success return, never runs), then the same test is rerun
    # against the same tmp dir without `--trace`. No CI job pairs the two flags that way today -
    # every `clickhouse-test` invocation is either always traced (Stateless and its `Diagnostics`
    # rerun, Fast test) or never traced (`stress.py`, and the upgrade check through it). The reset
    # is unconditional because the report block's condition is, not because a job exercises the
    # transition: the two must agree on what makes an artifact this run's, or the report attributes
    # one run's trace to another. `--test-runs`, `--database`, and a local run all reuse the tmp
    # dir, so the coupling is one flag change away from mattering.
    probe_args = _configure(tmp_path, monkeypatch)
    Path(probe_args.debug_log_file).write_text("STALE-EXPECT-4bd2\n", encoding="utf-8")
    Path(probe_args.bash_tracing_file).write_text("STALE-XTRACE-91af\n", encoding="utf-8")

    seen = {}

    def observe():
        seen["debuglog_exists"] = os.path.exists(probe_args.debug_log_file)
        seen["bashlog_exists"] = os.path.exists(probe_args.bash_tracing_file)

    testcase_args, env = _launch(
        tmp_path, monkeypatch, on_launch=observe, trace=False
    )
    # The seeding and the launch configure the case independently, so pin that they agree.
    assert (testcase_args.debug_log_file, testcase_args.bash_tracing_file) == (
        probe_args.debug_log_file,
        probe_args.bash_tracing_file,
    )

    assert seen["debuglog_exists"] is False, (
        "without --trace the previous run's expect trace survives into this run's report"
    )
    assert seen["bashlog_exists"] is False, (
        "without --trace the previous traced run's xtrace survives into this run's report"
    )
    # And nothing is exported when tracing is off, so shell_config.sh leaves the fd alone.
    assert "CLICKHOUSE_BASH_TRACING_FILE" not in env


def test_an_untraced_rerun_does_not_report_the_previous_runs_xtrace(tmp_path, monkeypatch):
    # End to end over the two real functions, because the reset-at-launch assertions above are
    # blind to what the REPORT ends up containing: the report block guards on the file's existence
    # and size, never on `args.trace`, so a reset that skips an untraced run makes
    # `process_result_impl` attribute the earlier run's xtrace to this one. That is a triager
    # reading a trace produced by a different run of the test.
    traced = _configure(tmp_path, monkeypatch)
    _launch(tmp_path, monkeypatch, trace=True)
    # What the traced child's `shell_config.sh` would have appended, then a failure - so the
    # artifact cleanup behind the success return never removes it.
    Path(traced.bash_tracing_file).write_text(
        "+ [2026-07-28 00:00:00] STALE-XTRACE-91af\n", encoding="utf-8"
    )
    assert "STALE-XTRACE-91af" in _report(traced).description, (
        "fixture check: the traced run's own report must contain its own xtrace"
    )

    # The untraced rerun, same tmp dir and therefore the same two artifact paths.
    untraced, env = _launch(tmp_path, monkeypatch, trace=False)
    assert untraced.bash_tracing_file == traced.bash_tracing_file
    assert "CLICKHOUSE_BASH_TRACING_FILE" not in env

    description = _report(untraced).description
    assert "STALE-XTRACE-91af" not in description, (
        "the untraced rerun reports the previous traced run's xtrace as its own diagnostics"
    )
    # And the artifact is gone rather than merely emptied, so not even a bare header survives.
    assert ".bashlog" not in description, description


def test_the_writer_wiring_matches_the_two_paths():
    # The couplings below are what actually route each writer to its own file, keep each file
    # bounded, and reset each one in time. They are invisible to every functional test AND to the
    # path assertions above: reverting any one of them reintroduces record loss while
    # `configure_testcase_args` still returns two distinct paths.
    shell_config = _SHELL_CONFIG.read_text(encoding="utf-8")
    redirect = re.search(
        r"^\s*exec 3>(>?)\"\$CLICKHOUSE_BASH_TRACING_FILE\"", shell_config, re.M
    )
    assert redirect, "shell_config.sh no longer redirects the xtrace fd to the tracing file"
    # `>>`, not `>`: an .expect test spawns bash once per `spawn` and each spawn sources this
    # file, so a truncating redirect keeps only the last spawn's trace.
    assert redirect.group(1) == ">", redirect.group(0).strip()

    source = inspect.getsource(ClickHouseTestCase.run_single_test)
    exports = [
        line.strip()
        for line in source.splitlines()
        if "CLICKHOUSE_BASH_TRACING_FILE" in line
    ]
    assert len(exports) == 1, exports
    # The xtrace must be pointed at the dedicated file. Pointing it back at `debug_log_file` is
    # the original collision.
    assert "args.bash_tracing_file" in exports[0], exports[0]
    assert "args.debug_log_file" not in exports[0], exports[0]

    # The two at-launch resets are asserted structurally, over the AST, because their POSITION is
    # the property that matters and a line scan cannot see it: both resets stay a single line and
    # keep matching after being moved, so a count assertion alone stays green while the coupling
    # is gone. The same bare-`Name` discipline as in the empty-guard test below applies - match
    # `args.<attr>` on the bare local `args`, so a comment or a decoy inside a nested `def`
    # cannot satisfy the assertion.
    fn = ast.parse(textwrap.dedent(source)).body[0]

    def calls(predicate):
        return [
            node
            for node in ast.walk(fn)
            if isinstance(node, ast.Call) and predicate(node)
        ]

    def first_arg_is_args_attr(node, attr):
        return (
            node.args
            and isinstance(node.args[0], ast.Attribute)
            and node.args[0].attr == attr
            and isinstance(node.args[0].value, ast.Name)
            and node.args[0].value.id == "args"
        )

    launches = calls(lambda node: isinstance(node.func, ast.Name) and node.func.id == "Popen")
    assert len(launches) == 1, [ast.unparse(node) for node in launches]

    # Both artifacts must be RESET at launch, and REMOVED rather than emptied: the expect report
    # block keys on existence alone (an empty file would add a bare header to every failing test),
    # and for the xtrace an emptied file is the unconditional pre-creation that forced the report
    # guard's size check in the first place. Removal is safe because each writer creates its file
    # (`exp_internal -f`; `shell_config.sh`'s `3>>`). The artifact cleanup in `process_result_impl`
    # cannot substitute for either reset: it sits behind the success return only, so a failing test
    # under `--test-runs` would otherwise accumulate every previous run.
    def resets_of(attr):
        removes = calls(
            lambda node: isinstance(node.func, ast.Attribute)
            and node.func.attr == "remove"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and first_arg_is_args_attr(node, attr)
        )
        empties = calls(
            lambda node: isinstance(node.func, ast.Name)
            and node.func.id == "open"
            and first_arg_is_args_attr(node, attr)
            and len(node.args) > 1
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == "w"
        )
        assert len(removes) == 1, [ast.unparse(node) for node in removes]
        assert not empties, [ast.unparse(node) for node in empties]
        return removes[0]

    removals = [resets_of("debug_log_file"), resets_of("bash_tracing_file")]

    # BEFORE the launch, both of them: a reset that runs after `Popen` resets nothing for this run,
    # and under `--test-runs` it makes every re-run report the PREVIOUS run's trace.
    for reset, what in zip(removals, ("expect trace removal", "xtrace removal")):
        assert reset.lineno < launches[0].lineno, (
            f"the {what} no longer runs before the test is launched"
        )

    # And NEITHER reset may sit under the `--trace` guard that exports the xtrace path.
    # `exp_internal -f` traces unconditionally, and the report blocks key on the FILES rather than
    # on this run's flags, so gating either reset on `--trace` leaves a stale artifact behind for
    # every run without it - including a trace-on -> trace-off rerun in the same tmp dir. (Not
    # implied by the lineno assertions above - the guard itself sits before `Popen`.)
    trace_guards = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.If)
        and any(
            isinstance(child, ast.Attribute)
            and child.attr == "trace"
            and isinstance(child.value, ast.Name)
            and child.value.id == "args"
            for child in ast.walk(node.test)
        )
    ]
    assert len(trace_guards) == 1, [ast.unparse(node.test) for node in trace_guards]
    # Node IDENTITY, not equality: two structurally identical calls are distinct nodes, so this
    # asks whether THE resets we located are descendants of the guard, not whether ones like them
    # are.
    trace_guarded = [node for stmt in trace_guards[0].body for node in ast.walk(stmt)]
    for reset, what in zip(removals, ("expect trace", "xtrace")):
        assert not any(node is reset for node in trace_guarded), (
            f"the {what} reset was nested under the --trace guard, so an untraced run keeps "
            "the previous run's artifact"
        )

    # Neither reset may be nested under an EXTENSION test either. `exp_internal -f` and
    # `shell_config.sh` are what write these two files, and both are reached from `.expect` AND
    # `.sh` tests (`03021_output_format_tty.sh` has an expect shebang; `02815_no_throw_in_simple_
    # queries.sh` is bash that sources shell_config.sh and runs expect heredocs on the same path).
    # So keying either reset on `self.ext` makes it a no-op for exactly the tests that have an
    # artifact, while the count, the lineno and the --trace assertions above all stay green.
    ext_guards = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.If)
        and any(
            isinstance(child, ast.Attribute)
            and child.attr == "ext"
            and isinstance(child.value, ast.Name)
            and child.value.id == "self"
            for child in ast.walk(node.test)
        )
    ]
    # Same `is`-identity containment as above: whether THESE resets are descendants, not whether
    # something shaped like them is.
    ext_guarded = [
        node
        for guard in ext_guards
        for stmt in guard.body + guard.orelse
        for node in ast.walk(stmt)
    ]
    for reset, what in zip(removals, ("expect trace reset", "xtrace reset")):
        assert not any(node is reset for node in ext_guarded), (
            f"the {what} was nested under a `self.ext` test, so it does nothing for the .expect "
            "and .sh tests that actually write these artifacts"
        )


def _describe(
    tmp_path,
    expect_text,
    bash_text,
    timed_out=False,
    exit_code=None,
    monkeypatch=None,
):
    """Run the real ``process_result_impl`` over two artifact files and return its TestResult.

    ``process_result_impl`` calls no method on ``self`` (only reads attributes), so a plain stub
    drives it with no server and no subprocess.  A non-empty ``stderr_file`` and no reference file
    steer it onto the FAIL/STDERR return, one of the six returns that append ``debug_log`` to the
    description - so a dropped append empties that block in all six.  ``None`` for either text
    means "do not create that file".

    Three of those six returns are driven, each reached by a different ``proc``:

    * the default (``proc`` is ``None``) drives the generic FAIL/STDERR return;
    * ``timed_out=True`` drives the TIMEOUT return, reached only with a truthy ``proc`` whose
      ``returncode`` is ``None``.  That branch calls ``kill_process_group``, so it needs
      ``monkeypatch`` to replace it with a recorder - the call is asserted, so the patch cannot
      silently mask a refactor that stops killing the group;
    * ``exit_code=<non-zero int>`` drives the EXIT_CODE return, which is the one an ``.expect``
      test actually takes: its ``expect_after ... timeout { exit 1 }`` turns an expect timeout into
      process exit status 1, so the report reads ``return code: 1`` and not ``Timeout``.  The kill
      is guarded on ``returncode is None``, so this arm never reaches it and must not patch it.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    expect_file = tmp_path / "04615_some_test.expect.debuglog"
    bash_file = tmp_path / "04615_some_test.expect.bashlog"
    if expect_text is not None:
        expect_file.write_text(expect_text, encoding="utf-8")
    if bash_text is not None:
        bash_file.write_text(bash_text, encoding="utf-8")
    stdout_file = tmp_path / "04615_some_test.stdout"
    stdout_file.write_text("", encoding="utf-8")
    stderr_file = tmp_path / "04615_some_test.stderr"
    stderr_file.write_text("a non-empty stderr, so a FAIL path is taken\n", encoding="utf-8")

    stub = SimpleNamespace(
        args=Namespace(cloud=False),
        testcase_args=Namespace(
            debug_log_file=str(expect_file),
            bash_tracing_file=str(bash_file),
            stop=False,
            record=False,
            flaky_check=False,
            test_runs=1,
            unified=3,
        ),
        # Points at nothing, so the sanitizer-log glob finds no files.
        fatal_sanitizer_prefix=str(tmp_path / "no-such-fatal-prefix"),
        stdout_file=str(stdout_file),
        stderr_file=str(stderr_file),
        name="04615_some_test.expect",
        reference_file=None,
        show_whitespaces_in_diff=False,
        suite=None,
        tags=set(),
    )
    if exit_code is not None:
        assert not timed_out, "the two proc arms are mutually exclusive"
        assert exit_code != 0, "a zero return code does not take the EXIT_CODE branch"
        # `kill_process_group` is guarded on `returncode is None`, so it is unreachable here and
        # deliberately left unpatched: patching it would hide a refactor that starts killing the
        # group for a process that already exited.
        proc = SimpleNamespace(pid=os.getpid(), returncode=exit_code)
        return ClickHouseTestCase.process_result_impl(stub, proc, 1.0)

    if not timed_out:
        return ClickHouseTestCase.process_result_impl(stub, None, 1.0)

    assert monkeypatch is not None, "the timeout arm has to patch kill_process_group"
    killed = []
    monkeypatch.setitem(
        _CT_GLOBALS,
        "kill_process_group",
        lambda pgid, fatal_log: killed.append(pgid),
    )
    # `returncode is None` is what the real runner leaves behind when `proc.wait(timeout)` raised
    # TimeoutExpired. `os.getpid()` is a pid that certainly exists, so `os.getpgid` resolves - and
    # nothing is signalled, because the only killer is the recorder above.
    proc = SimpleNamespace(pid=os.getpid(), returncode=None)
    result = ClickHouseTestCase.process_result_impl(stub, proc, 1.0)
    assert killed == [os.getpgid(os.getpid())], killed
    return result


def test_both_artifacts_actually_reach_the_failure_description(tmp_path):
    # The assertions below are behavioural on purpose. Every structural check in this module can
    # locate the two `trim_for_log` calls and still pass if the `debug_log += ` in front of one of
    # them is dropped, and a discarded append silently empties that artifact block in EVERY
    # failure report - the exact user-visible symptom this PR exists to fix.
    both = _describe(tmp_path / "both", "EXPECT-MARKER-ff31\n", "BASH-MARKER-a07c\n")
    assert both.status is _TestStatus.FAIL, both.status
    assert "EXPECT-MARKER-ff31" in both.description, both.description
    assert "BASH-MARKER-a07c" in both.description, both.description

    # The empty-xtrace suppression, proved by effect this time: the file exists but is empty, so
    # neither its contents nor its NAME may appear. (The AST test below proves the same property
    # structurally - both stay, because either one alone has a blind spot.)
    empty_bash = _describe(tmp_path / "empty_bash", "EXPECT-MARKER-ff31\n", "")
    assert "EXPECT-MARKER-ff31" in empty_bash.description, empty_bash.description
    assert ".bashlog" not in empty_bash.description, empty_bash.description

    # And symmetrically: a missing expect trace contributes nothing while the xtrace still does.
    no_expect = _describe(tmp_path / "no_expect", None, "BASH-MARKER-a07c\n")
    assert ".debuglog" not in no_expect.description, no_expect.description
    assert "BASH-MARKER-a07c" in no_expect.description, no_expect.description


def test_both_artifacts_reach_the_timeout_description(tmp_path, monkeypatch):
    # The TIMEOUT return has its own `if debug_log:` append, and it is the return that matters most
    # here: 04615 fails `return code: 1` after an expect timeout, and this PR promises the next
    # occurrence yields a complete debuglog. The oracle above only drives the STDERR return, so
    # guarding the artifact assembly with `if proc is None:` would keep it green while stripping
    # the diagnostics from exactly the report a triager reads.
    timed_out = _describe(
        tmp_path / "timeout",
        "EXPECT-MARKER-ff31\n",
        "BASH-MARKER-a07c\n",
        timed_out=True,
        monkeypatch=monkeypatch,
    )
    assert timed_out.status is _TestStatus.FAIL, timed_out.status
    assert timed_out.reason is _FailureReason.TIMEOUT, timed_out.reason
    assert "EXPECT-MARKER-ff31" in timed_out.description, timed_out.description
    assert "BASH-MARKER-a07c" in timed_out.description, timed_out.description


def test_both_artifacts_reach_the_exit_code_description(tmp_path):
    # The EXIT_CODE return has an artifact append of its own, and it is the return the cited
    # failure actually takes: 04615's `expect_after ... timeout { exit 1 }` turns an expect timeout
    # into exit status 1, so the report reads `return code: 1` and never `Timeout`. Neither oracle
    # above reaches this arm - one passes `proc=None`, the other `returncode=None` - so deleting
    # just this branch's append keeps all the other tests green while stripping both diagnostics
    # from precisely the report a triager reads. Guarding the artifact assembly itself on
    # `proc.returncode is None` empties this arm and the timeout arm together.
    failed = _describe(
        tmp_path / "exit_code",
        "EXPECT-MARKER-ff31\n",
        "BASH-MARKER-a07c\n",
        exit_code=1,
    )
    assert failed.status is _TestStatus.FAIL, failed.status
    assert failed.reason is _FailureReason.EXIT_CODE, failed.reason
    assert "EXPECT-MARKER-ff31" in failed.description, failed.description
    assert "BASH-MARKER-a07c" in failed.description, failed.description
    # The stringified return code is what this branch prepends before anything else, so it pins
    # that the assertions above are about this arm and not some other FAIL return that happens to
    # carry the markers.
    assert failed.description.startswith("1"), failed.description


def test_the_two_trim_limits_differ_in_effect(tmp_path, monkeypatch):
    # The limits are also asserted from the call text below, but text cannot see whether the
    # trimmed value is the one that reaches the report: replacing `debug_log += trim_for_log(x)`
    # with `debug_log += x` while leaving `trim_for_log(x)` in place as a discarded expression
    # keeps every marker assertion AND both numeric-limit assertions green with both bounds gone.
    # So the two bounds are measured here on the returned description.
    bash_limit = 100
    # Between the two limits: over the xtrace bound, under the expect default. One payload
    # therefore shows the two limits differing in EFFECT and not merely in call text.
    between = _TRIM_DEFAULT_LIMIT // 2
    assert bash_limit < between < _TRIM_DEFAULT_LIMIT

    expect_body = "".join(f"expect line {i}\n" for i in range(between))
    bash_body = "".join(f"+ traced command {i}\n" for i in range(between))
    between_dir = tmp_path / "between"
    described = _describe(between_dir, expect_body, bash_body)

    blocks = _artifact_blocks(described.description, between_dir)
    # What `trim_for_log` is handed is `<path>:\n` + body + `\n`, so reconstruct it and derive the
    # expected counts from the function's own arithmetic rather than from a magic number.
    bash_expected, bash_trimmed = _trimmed_line_count(
        str(between_dir / "04615_some_test.expect.bashlog") + ":\n" + bash_body + "\n",
        bash_limit,
    )
    expect_expected, expect_trimmed = _trimmed_line_count(
        str(between_dir / "04615_some_test.expect.debuglog") + ":\n" + expect_body + "\n",
        _TRIM_DEFAULT_LIMIT,
    )
    assert bash_trimmed and not expect_trimmed, (bash_trimmed, expect_trimmed)

    assert _HIDDEN_RE.search(blocks["bashlog"]), (
        "the xtrace block is not bounded at all: a looping .sh test would dump every traced line"
    )
    assert len(blocks["bashlog"].splitlines()) == bash_expected, (
        len(blocks["bashlog"].splitlines()),
        bash_expected,
    )
    assert not _HIDDEN_RE.search(blocks["debuglog"]), (
        "the expect trace was cut at the xtrace's 100-line bound, which removes the middle of the "
        "verdict sequence - the part that names the statement that blocked"
    )
    assert len(blocks["debuglog"].splitlines()) == expect_expected, (
        len(blocks["debuglog"].splitlines()),
        expect_expected,
    )

    # And the expect trace IS bounded, at its own larger limit: a payload over the default gets a
    # separator too, so the assertion above is about WHERE the bound is, not about its absence.
    huge = "".join(f"expect line {i}\n" for i in range(_TRIM_DEFAULT_LIMIT + 10))
    over_dir = tmp_path / "over"
    over = _describe(over_dir, huge, "BASH-MARKER-a07c\n")
    over_blocks = _artifact_blocks(over.description, over_dir)
    over_expected, over_trimmed = _trimmed_line_count(
        str(over_dir / "04615_some_test.expect.debuglog") + ":\n" + huge + "\n",
        _TRIM_DEFAULT_LIMIT,
    )
    assert over_trimmed
    assert _HIDDEN_RE.search(over_blocks["debuglog"]), over_blocks["debuglog"][:200]
    assert len(over_blocks["debuglog"].splitlines()) == over_expected, (
        len(over_blocks["debuglog"].splitlines()),
        over_expected,
    )


def test_the_two_report_blocks_are_trimmed_independently():
    source = inspect.getsource(ClickHouseTestCase.process_result_impl)

    expect_calls = [
        line.strip()
        for line in source.splitlines()
        if "trim_for_log(expect_log" in line
    ]
    bash_calls = [
        line.strip() for line in source.splitlines() if "trim_for_log(bash_log" in line
    ]
    assert len(expect_calls) == 1, expect_calls
    assert len(bash_calls) == 1, bash_calls

    # The xtrace keeps the historical 100-line bound; the expect trace must NOT, or the
    # middle of its verdict sequence - the part that names the statement that blocked - is
    # dropped, since one progress-table redraw is one very long line.
    assert _trim_limit(bash_calls[0], "bash_log") == 100, bash_calls[0]
    assert (
        _trim_limit(expect_calls[0], "expect_log") == _TRIM_DEFAULT_LIMIT
    ), expect_calls[0]
    assert _trim_limit(bash_calls[0], "bash_log") != _trim_limit(
        expect_calls[0], "expect_log"
    )


def test_an_empty_bash_xtrace_file_produces_no_block(tmp_path):
    # Two independent properties are asserted: the report block is ENCLOSED by a guard on the
    # tracing file (structurally, via the AST), and that guard's CONDITION rejects an empty
    # file (behaviourally, by evaluating it). A guard that stops enclosing the block is the
    # regression this test is named for, and it leaves the condition itself intact - so
    # checking only the condition would miss it.
    fn = ast.parse(
        textwrap.dedent(inspect.getsource(ClickHouseTestCase.process_result_impl))
    ).body[0]

    # Match the bare local name, not the source text: `process_result_impl` also guards the
    # artifact cleanup on `self.testcase_args.bash_tracing_file` (an attribute, and that guard
    # legitimately only tests existence), and a copy of the guard inside a nested `def` would
    # satisfy a text search while enclosing nothing.
    def mentions_tracing_file(node):
        return any(
            isinstance(child, ast.Name) and child.id == "bash_tracing_file"
            for child in ast.walk(node)
        )

    guards = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.If) and mentions_tracing_file(node.test)
    ]
    assert len(guards) == 1, [ast.unparse(node.test) for node in guards]

    # Containment: the header assignment and the trimmed append must both be INSIDE the guard's
    # body. Moving either out - to a sibling branch, or ahead of the guard - lets an empty or
    # missing .bashlog back into the report path.
    guarded = [node for stmt in guards[0].body for node in ast.walk(stmt)]
    assignments = [
        node
        for node in guarded
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "bash_log"
            for target in node.targets
        )
    ]
    assert len(assignments) == 1, "bash_log is not assigned inside the guard body"
    appends = [
        node
        for node in guarded
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "trim_for_log"
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "bash_log"
    ]
    assert len(appends) == 1, (
        "the xtrace report block is not assembled inside the guard body"
    )

    condition = ast.unparse(guards[0].test)

    empty = tmp_path / "empty.bashlog"
    empty.write_text("", encoding="utf-8")
    written = tmp_path / "written.bashlog"
    written.write_text("+ [ts] some traced command\n", encoding="utf-8")
    missing = tmp_path / "missing.bashlog"

    # Evaluate the real guard expression from the real source.
    def guard(path):
        return bool(eval(condition, {"os": os}, {"bash_tracing_file": str(path)}))

    assert guard(written) is True
    assert guard(empty) is False, (
        "an existence-only guard reports the artifact name of every failing .sql test"
    )
    assert guard(missing) is False

    # Why the size check is needed at all: a header-only block is non-empty after trimming,
    # so it would flip `debug_log` from falsy to truthy and every `if debug_log:` consumer
    # would append a bare filename with an empty body.
    assert trim_for_log(str(empty) + ":\n" + "" + "\n", 100)
