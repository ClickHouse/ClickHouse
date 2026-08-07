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
from contextlib import redirect_stdout
from pathlib import Path

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
