"""
End-to-end tests for the stacktrace helpers in tests/clickhouse-test.

Background
----------
``clickhouse-test`` assigns ``args = parse_args()`` only inside
``if __name__ == "__main__":``.  On macOS, Python's default
multiprocessing start method is ``spawn``, which re-imports the module
in each worker without executing ``__main__`` — so module-level
``args`` is undefined, and any helper that closed over it crashed with
``NameError``.  See the fast_test_arm_darwin failure where the
hung-check path raised ``NameError: name 'args' is not defined`` inside
``get_server_pid``.

These tests reproduce the same import condition by loading
``clickhouse-test`` via ``runpy.run_path`` (which, like spawn, does not
run ``__main__``) and then invoke each public stacktrace helper against
the live ClickHouse server provided by the ``ClickHouseService``
fixture in ``ci/jobs/ci_tests_job.py``.

Pre-fix: NameError inside the fresh import.
Post-fix: the helpers run to completion against a live server.
"""

import argparse
import io
import os
import re
import runpy
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from time import sleep, time

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def _load_clickhouse_test(require_server=True):
    # Mimic a spawn worker: load clickhouse-test without running __main__,
    # so module-level `args` is absent.
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    assert "args" not in ct, (
        "module-level 'args' must not be defined outside __main__; otherwise "
        "the spawn-worker scenario this test reproduces does not apply"
    )

    if require_server:
        # Sanity-check the precondition: the CI tests job started a server.
        assert ct["pgrep"](command="clickhouse-server"), (
            "no clickhouse-server process found — this test expects ClickHouseService "
            "(see ci/jobs/ci_tests_job.py) to be running on localhost:9000"
        )
    return ct


def _make_args():
    # Minimal args namespace: only the fields the helpers and their
    # transitive callees actually read.  Mirrors what __main__ assigns
    # after parse_args() for a local-server, plaintext-TCP,
    # default-database run.
    return argparse.Namespace(
        client="clickhouse-client --port=9000",
        client_option=None,
        secure=False,
        tcp_host="localhost",
        http_port=8123,
        client_options_query_str="",
        replicated_database=False,
        shared_catalog=False,
        force_color=False,
        binary=os.environ.get("CLICKHOUSE_BINARY", "clickhouse"),
        # A reachable server means __main__ collected build flags at startup;
        # a non-ASan set keeps print_c_stacktraces on its lldb path.
        build_flags=set(),
    )


def test_print_c_stacktraces_against_live_server():
    ct = _load_clickhouse_test()
    args = _make_args()

    captured = io.StringIO()
    with redirect_stdout(captured):
        ct["print_c_stacktraces"](args)
    output = captured.getvalue()

    # The function must have located the server PID and reached gdb.
    # Whether the attach itself succeeds depends on the host's
    # `kernel.yama.ptrace_scope` and is not asserted.
    assert "Collecting C stacktraces from main server process" in output, output


def test_print_sql_stacktraces_against_live_server():
    ct = _load_clickhouse_test()
    args = _make_args()

    captured = io.StringIO()
    with redirect_stdout(captured):
        ct["print_sql_stacktraces"](args)
    output = captured.getvalue()

    # The function must have queried system.stack_trace and printed
    # traces.  We don't require a specific thread name — any non-trivial
    # output confirms the round-trip succeeded.
    assert "Collecting stacktraces from system.stack_trace table" in output, output
    assert "trace_str" in output or "thread_name" in output, output


def test_get_stacktraces_tolerates_repeated_client_options():
    # A setting passed via `--client-option` is also copied into
    # `CLICKHOUSE_CLIENT_OPT` at startup, so `get_additional_client_options`
    # returns it twice.  `add_effective_settings` adds
    # `--allow_repeated_settings` while a test runs and
    # `remove_settings_from_env` drops it again, so the collector must supply
    # the flag itself or `clickhouse-client` exits 36 with
    # `cannot be specified more than once`.
    ct = _load_clickhouse_test()
    args = _make_args()
    args.client_option = [
        "max_untracked_memory=1Gi",
        "max_memory_usage_for_user=0",
        "memory_profiler_step=1Gi",
        "ast_fuzzer_runs=0",
    ]

    saved = os.environ.get("CLICKHOUSE_CLIENT_OPT")
    try:
        os.environ["CLICKHOUSE_CLIENT_OPT"] = " ".join(
            "--" + option for option in args.client_option
        )

        # Premise: without this the test would pass trivially if the duplication
        # ever stopped happening, and would no longer cover the fix.
        options = ct["get_additional_client_options"](args).split()
        names = [o.split("=")[0] for o in options if o.startswith("--")]
        repeated = sorted(n for n, count in Counter(names).items() if count > 1)
        assert repeated == [
            "--ast_fuzzer_runs",
            "--max_memory_usage_for_user",
            "--max_untracked_memory",
            "--memory_profiler_step",
        ], repeated
        assert "--allow_repeated_settings" not in options, options

        dump = ct["get_stacktraces_from_clickhouse"](args)
    finally:
        if saved is None:
            os.environ.pop("CLICKHOUSE_CLIENT_OPT", None)
        else:
            os.environ["CLICKHOUSE_CLIENT_OPT"] = saved

    assert dump, "no stacktraces collected: the client rejected the command line"
    assert "thread_name" in dump, dump[:2000]


def test_shell_get_output_keeps_the_partial_dump_on_timeout():
    # subprocess.TimeoutExpired is not a CalledProcessError subclass, so before
    # the fix keep_output_on_error did not cover a timeout and every byte the
    # child had already written was dropped: which is how a timed-out lldb
    # attach produced "Got suspiciously small stacktraces from <pid>:" with
    # nothing after it.
    ct = _load_clickhouse_test(require_server=False)
    printed = "line\n" * 400  # > the 1000-byte print_c_stacktraces guard
    command = f"printf '{'line\\n' * 400}'; sleep 60"

    out = ct["shell_get_output"](command, timeout=2, keep_output_on_error=True)

    # Premise: a matched control on the *other* error path must preserve the
    # same output, or this test cannot tell "timeout is special-cased" from
    # "shell_get_output never returns anything".
    control = ct["shell_get_output"](
        f"printf '{'line\\n' * 400}'; exit 3", timeout=60, keep_output_on_error=True
    )
    assert len(control) > 1000, len(control)

    assert isinstance(out, str), type(out)
    assert len(out) > 1000, len(out)
    assert printed.strip() in out, out[:400]
    # Not the b'...' rendering of raw bytes: real newlines, no escapes.
    assert "\\n" not in out, out[:400]
    assert not out.startswith("b'"), out[:400]
    # The dump is incomplete, and the marker is what stops a consumer from
    # presenting it as a full backtrace.
    assert ct["TRUNCATED_ON_TIMEOUT_MARKER"] in out, out[-200:]

    # Without the flag a timeout still returns "": the diagnostic goes to
    # stderr, and callers that do not persist output are unchanged.
    assert ct["shell_get_output"](command, timeout=2) == ""


def _capture_lldb_budgets(ct, args, pids, elapsed=0.0, dump=None, **kwargs):
    """Run print_c_stacktraces with the collector stubbed, returning the
    per-PID timeouts it was called with (and its stdout)."""
    budgets = []

    def collector(pid, timeout=None):
        budgets.append(timeout)
        if elapsed:
            sleep(elapsed)
        return "x" * 2000 if dump is None else dump

    globals_ = ct["print_c_stacktraces"].__globals__
    saved = (globals_["get_stacktraces_from_lldb"], globals_["get_all_server_pids"])
    globals_["get_stacktraces_from_lldb"] = collector
    globals_["get_all_server_pids"] = lambda _args: list(pids)
    captured = io.StringIO()
    try:
        with redirect_stdout(captured):
            ct["print_c_stacktraces"](args, **kwargs)
    finally:
        (
            globals_["get_stacktraces_from_lldb"],
            globals_["get_all_server_pids"],
        ) = saved
    return budgets, captured.getvalue()


def test_lldb_budget_scales_with_build_flavor():
    # A debug or sanitizer or coverage server needs far longer than 30s to walk;
    # a release server does not, and must keep the tight budget that bounds a
    # genuinely wedged lldb.
    ct = _load_clickhouse_test(require_server=False)
    flags = ct["BuildFlags"]
    release, slow = ct["LLDB_TIMEOUT"], ct["LLDB_SLOW_BUILD_TIMEOUT"]
    assert slow > release, (slow, release)

    args = _make_args()
    for build_flags, expected in (
        ({flags.RELEASE}, release),
        ({flags.DEBUG}, slow),
        ({flags.THREAD}, slow),
        ({flags.MEMORY}, slow),
        ({flags.UNDEFINED}, slow),
        ({flags.WITH_COVERAGE}, slow),
    ):
        args.build_flags = build_flags
        assert ct["lldb_timeout_for_build"](args) == expected, build_flags
        # And the loop passes that value through. It is clamped to what is left
        # of the aggregate ceiling, so compare with a tolerance rather than
        # exactly.
        budgets, _ = _capture_lldb_budgets(ct, args, [4242])
        assert len(budgets) == 1 and abs(budgets[0] - expected) < 1, (
            build_flags,
            budgets,
            expected,
        )


def test_lldb_budget_survives_the_spawn_start_method():
    # tests/clickhouse-test sets multiprocessing "spawn" when CHECK_NAME contains
    # "aarch", and a spawned worker re-imports the module with
    # RELEASE_NON_SANITIZED / SANITIZED back at their defaults while `args` is
    # transferred intact. arm_debug is the dominant source of the 30s timeouts,
    # so a globals-derived budget would apply the release value on exactly the
    # platform this fix exists for: green under fork, inert in CI.
    ct = _load_clickhouse_test(require_server=False)
    globals_ = ct["print_c_stacktraces"].__globals__
    saved = (globals_["RELEASE_NON_SANITIZED"], globals_["SANITIZED"])
    globals_["RELEASE_NON_SANITIZED"] = False
    globals_["SANITIZED"] = False
    try:
        args = _make_args()
        args.build_flags = {ct["BuildFlags"].DEBUG}
        budgets, _ = _capture_lldb_budgets(ct, args, [4242])
    finally:
        globals_["RELEASE_NON_SANITIZED"], globals_["SANITIZED"] = saved

    assert len(budgets) == 1, budgets
    assert abs(budgets[0] - ct["LLDB_SLOW_BUILD_TIMEOUT"]) < 1, budgets


def test_lldb_budget_defaults_to_release_when_build_flags_are_missing():
    # The startup-failure caller runs before `args.build_flags` is assigned, so
    # the flavor lookup must neither raise nor guess on the one path that is
    # already reporting a dead server.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    delattr(args, "build_flags")

    budgets, _ = _capture_lldb_budgets(ct, args, [4242])

    assert len(budgets) == 1, budgets
    assert abs(budgets[0] - ct["LLDB_TIMEOUT"]) < 1, budgets


def test_lldb_pid_loop_honours_the_aggregate_deadline():
    # The loop spans every server process (up to 20 observed in CI), so a
    # per-PID budget alone does not bound it. Exhausting the total must stop the
    # loop and say how many processes went undumped.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.build_flags = {ct["BuildFlags"].DEBUG}
    pids = [111, 222, 333, 444]

    started = time()
    budgets, output = _capture_lldb_budgets(
        ct, args, pids, elapsed=1.0, per_pid_timeout=30, total_timeout=2
    )
    took = time() - started

    assert len(budgets) < len(pids), budgets
    assert took < 30, took
    # Each call is clamped to what is left, so no single attach can overrun the
    # ceiling on its own.
    assert all(b <= 2 for b in budgets), budgets
    skipped = len(pids) - len(budgets)
    assert f"skipping {skipped} of {len(pids)} processes" in output, output
    assert str(pids[-1]) in output.split("skipping")[1], output


def test_truncated_dump_is_not_announced_as_full():
    # A rescued partial backtrace is worth keeping, but the report must not
    # present it as the complete one: a reader who trusts the "full" label
    # concludes the server really had only those threads.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.build_flags = {ct["BuildFlags"].DEBUG}
    body = "frame #0\n" * 300

    _, complete = _capture_lldb_budgets(ct, args, [4242], dump=body)
    _, partial = _capture_lldb_budgets(
        ct, args, [4242], dump=f"{body}\n{ct['TRUNCATED_ON_TIMEOUT_MARKER']}"
    )

    # Premise: both arms must have passed the size guard, or neither says
    # anything about the label.
    for arm, out in (("complete", complete), ("partial", partial)):
        assert "suspiciously small" not in out, (arm, out)
        assert f"saved to {ct['C_STACKTRACES_LOG']}" in out, (arm, out)

    assert "(full lldb backtrace saved to" in complete, complete
    assert "(partial lldb backtrace saved to" in partial, partial
    assert "(full lldb backtrace saved to" not in partial, partial


def test_timeout_handler_keeps_the_tight_lldb_pair():
    # The per-test timeout handler runs after its one-shot alarm has fired, so
    # the alarm cannot bound it and only the job's outer timeout is left. That
    # site therefore keeps today's 30s per-PID value; the abort paths, where no
    # alarm is pending, take the flavor budget. Asserted on the source so
    # neither the outer-deadline risk nor the hung-check coverage can regress.
    # Collapse whitespace so the assertion does not depend on how the call is
    # wrapped across lines.
    source = " ".join(Path(_CLICKHOUSE_TEST).read_text(encoding="utf-8").split())
    calls = re.findall(r"(?<!def )print_c_stacktraces\((.*?)\)", source)
    tight = [c for c in calls if "per_pid_timeout" in c or "total_timeout" in c]
    assert len(tight) == 1, calls
    assert "per_pid_timeout=LLDB_TIMEOUT" in tight[0], tight
    assert "total_timeout=60" in tight[0], tight
    # Every other call site takes the defaults, i.e. the flavor budget.
    plain = [c for c in calls if c.strip() == "args"]
    assert len(plain) == 4, calls


def test_is_asan_build_uses_collected_flags():
    # Normal path: build flags were collected while the server was reachable,
    # so membership in the set decides — no binary query needed.
    ct = _load_clickhouse_test()
    args = _make_args()

    args.build_flags = {ct["BuildFlags"].ADDRESS}
    assert ct["is_asan_build"](args) is True

    args.build_flags = set()
    assert ct["is_asan_build"](args) is False


def test_is_asan_build_falls_back_to_binary_when_flags_missing():
    # Startup-failure path: flags were never collected (server never served a
    # query), so the ASan bit is read from the binary itself rather than from
    # ASAN_OPTIONS. The CI tests job runs a master release build, not an ASan
    # build, so this must resolve to False without raising.
    ct = _load_clickhouse_test()
    args = _make_args()
    delattr(args, "build_flags")

    assert ct["is_asan_build"](args) is False
