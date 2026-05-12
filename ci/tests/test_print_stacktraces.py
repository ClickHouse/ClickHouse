"""
End-to-end test for ``print_stacktraces`` in tests/clickhouse-test.

Background
----------
``clickhouse-test`` assigns ``args = parse_args()`` only inside
``if __name__ == "__main__":``.  On macOS, Python's default multiprocessing
start method is ``spawn``, which re-imports the module in each worker
without executing ``__main__`` — so module-level ``args`` is undefined,
and any helper that closed over it crashed with ``NameError``.  See the
fast_test_arm_darwin failure where the hung-check path raised
``NameError: name 'args' is not defined`` inside ``get_server_pid``.

This test reproduces the same import condition by loading
``clickhouse-test`` via ``runpy.run_path`` (which, like spawn, does not run
``__main__``) and then invokes ``print_stacktraces`` against the live
ClickHouse server provided by the ``ClickHouseService`` fixture in
``ci/jobs/ci_tests_job.py``.  It exercises the full code path:

  * locate the server PID via the portable ``pgrep`` helper,
  * query ``system.stack_trace`` through ``args.client``,
  * format and print the result.

Pre-fix: NameError inside the fresh import.
Post-fix: prints the stack-trace dump and returns.
"""

import argparse
import io
import runpy
from contextlib import redirect_stdout
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def test_print_stacktraces_against_live_server():
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

    # Minimal args namespace: only the fields print_stacktraces and its
    # transitive helpers actually read.  Mirrors what __main__ assigns after
    # parse_args() for a local-server, plaintext-TCP, default-database run.
    args = argparse.Namespace(
        client="clickhouse-client --port=9000",
        client_option=None,
        secure=False,
        tcp_host="localhost",
        http_port=8123,
        client_options_query_str="",
        replicated_database=False,
        shared_catalog=False,
        force_color=False,
    )

    captured = io.StringIO()
    with redirect_stdout(captured):
        ct["print_stacktraces"](args)
    output = captured.getvalue()

    # The function must have queried system.stack_trace and printed traces.
    # We don't require a specific thread name — any non-trivial output
    # confirms the round-trip succeeded.
    assert "Collecting stacktraces" in output, output
    assert "trace_str" in output or "thread_name" in output, output
