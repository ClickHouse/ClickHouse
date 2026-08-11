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
import runpy
import shutil
import subprocess
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def _load_clickhouse_test():
    # Mimic a spawn worker: load clickhouse-test without running __main__,
    # so module-level `args` is absent.
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    assert "args" not in ct, (
        "module-level 'args' must not be defined outside __main__; otherwise "
        "the spawn-worker scenario this test reproduces does not apply"
    )

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


def _lldb_collector_command(ct, pid):
    # The command string get_stacktraces_from_lldb would run, captured instead
    # of executed.
    captured = {}

    def capture(cmd, timeout=None, keep_output_on_error=False):
        captured["cmd"] = cmd
        return ""

    saved = ct["get_stacktraces_from_lldb"].__globals__["shell_get_output"]
    ct["get_stacktraces_from_lldb"].__globals__["shell_get_output"] = capture
    try:
        ct["get_stacktraces_from_lldb"](pid)
    finally:
        ct["get_stacktraces_from_lldb"].__globals__["shell_get_output"] = saved
    return captured["cmd"]


def _internal_breakpoints(command):
    # Swap the expensive backtrace for the observation, so the run is quick and
    # the only thing measured is which internal breakpoints survive.
    command = command.replace(
        "-o 'thread backtrace all'", "-o 'breakpoint list --internal'"
    )
    done = subprocess.run(
        command,
        shell=True,
        executable="/bin/bash",
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return done.stdout + done.stderr


def test_lldb_collector_drops_the_loader_rendezvous_breakpoint():
    # lldb's shared-library-event breakpoint is a software int3 in the running
    # server's text.  Only the debugger that wrote it restores it, so a debugger
    # killed mid-backtrace leaves it behind and the next dlopen in the server
    # raises SIGTRAP, which the daemon reports as a fatal signal.
    ct = _load_clickhouse_test()
    if not shutil.which("lldb"):
        pytest.skip("lldb is not installed")
    pid = ct["get_server_pid"](_make_args())
    assert pid, "no server pid"

    command = _lldb_collector_command(ct, pid)
    drop = ct["LLDB_DROP_RENDEZVOUS_BREAKPOINT"]
    assert f" -o '{drop}'" in command, command
    # Ordering is load-bearing: after the backtrace the breakpoint has already
    # existed for the whole window in which the debugger can be killed.
    assert command.index(drop) < command.index("thread backtrace all"), command

    with_drop = _internal_breakpoints(command)
    without_drop = _internal_breakpoints(command.replace(f" -o '{drop}'", "", 1))

    # Premise: both arms must have attached, or neither observation means
    # anything.  Both must also have detached, since the arm that keeps the
    # breakpoint relies on the detach to restore the byte and must not leave the
    # very defect under test in a live server.
    for arm, out in (("with", with_drop), ("without", without_drop)):
        assert f"Process {pid} stopped" in out, (arm, out[:4000])
        assert f"Process {pid} detached" in out, (arm, out[:4000])
    # And the breakpoint must be present without the drop, or the arms would
    # agree for lack of anything to remove.
    assert "Kind: shared-library-event" in without_drop, without_drop[:4000]

    assert "Kind: shared-library-event" not in with_drop, with_drop[:4000]
    # Selecting by symbol rather than by id: the JIT and sanitizer hooks take
    # the low internal ids, so exactly one breakpoint may disappear.
    kept = [line for line in without_drop.splitlines() if line.startswith("Kind: ")]
    remaining = [line for line in with_drop.splitlines() if line.startswith("Kind: ")]
    assert remaining == [k for k in kept if "shared-library-event" not in k], (
        remaining,
        kept,
    )


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
