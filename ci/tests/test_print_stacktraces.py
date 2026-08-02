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

Transport-matched availability gate
-----------------------------------
``print_sql_stacktraces`` used to pre-check availability with
``check_server_liveness``, which probes HTTP, and then collect over native
TCP via ``args.client``.  Those are different listeners on different ports,
so a server answering TCP but not HTTP was skipped -- and that is exactly
the hung-check signature (process alive, HTTP not responding) under which
the dump is the only source of a ``query_id`` per stuck thread, because
``print_c_stacktraces`` declines to attach a debugger on ASan builds.

The tests below pin three things that no functional test can see:
``print_sql_stacktraces`` collects when HTTP is dead and TCP is live
(asserted on the artifact and its columns, never on stdout chatter, since
the unfixed code also prints a line); each abort site calls it, asserted
per site so reverting one site cannot be masked by another already-correct
caller; and a dead or wedged socket stays bounded, silent-but-for-one-line,
and raises nothing, which is the property that made removing the pre-check
safe.
"""

import argparse
import ast
import contextlib
import inspect
import io
import os
import runpy
import socket
import time
from contextlib import redirect_stderr, redirect_stdout
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


def _closed_port():
    """A port nothing listens on: bound without listen(), so connect() is
    refused immediately instead of racing a port another test may claim."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    return sock, sock.getsockname()[1]


@contextlib.contextmanager
def _accepting_blackhole():
    """A listener that accepts and never answers -- a wedged socket, as
    opposed to a closed one.  Exercises the collector's timeout rather than
    its connection-refused path."""
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    accepted = []

    import threading

    def _accept_loop():
        while True:
            try:
                accepted.append(srv.accept()[0])
            except OSError:
                return

    thread = threading.Thread(target=_accept_loop, daemon=True)
    thread.start()
    try:
        yield srv.getsockname()[1]
    finally:
        srv.close()
        for conn in accepted:
            conn.close()


def _run_sql_dump(ct, args, tmp_path):
    """Invoke print_sql_stacktraces in an isolated cwd and report what it
    produced.  The artifact is written relative to cwd, so a private one
    keeps this parallel-safe and lets the oracle key on the artifact rather
    than on stdout -- the unfixed code prints a line too."""
    stdout, stderr = io.StringIO(), io.StringIO()
    started = time.monotonic()
    with _chdir(tmp_path), redirect_stdout(stdout), redirect_stderr(stderr):
        ct["print_sql_stacktraces"](args)
    return {
        "seconds": time.monotonic() - started,
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
        "artifact": Path(tmp_path) / ct["SQL_STACKTRACES_LOG"],
    }


@contextlib.contextmanager
def _chdir(path):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _abort_site(function_name, marker):
    """Compile the one `if` statement that forms an abort site, straight out
    of the real file.

    Executing the real statement with stubbed callees checks the calls a site
    makes AND their order, which a search for the call's name cannot: it
    would also match the same call in a sibling site, so reverting one site
    would still pass while the other covers for it.
    """
    tree = ast.parse(Path(_CLICKHOUSE_TEST).read_text(encoding="utf-8"))
    function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == function_name
    )
    candidates = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.If) and marker in ast.unparse(node)
    ]
    assert candidates, f"no `if` statement in {function_name} mentions {marker!r}"
    # Innermost match: the outer `if args.hung_check:` also contains the marker.
    site = min(candidates, key=lambda node: len(ast.unparse(node)))
    return compile(
        ast.fix_missing_locations(ast.Module(body=[site], type_ignores=[])),
        _CLICKHOUSE_TEST,
        "exec",
    )


def _record_calls(names):
    calls = []
    namespace = {name: (lambda *_a, n=name, **_k: calls.append(n)) for name in names}
    return calls, namespace


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


def test_sql_stacktraces_collects_when_http_is_dead_but_tcp_is_live(tmp_path):
    # The decisive case: the hung-check signature is a server that no longer
    # answers HTTP while its native TCP listener still serves queries.  Keying
    # the oracle on the artifact is what makes this non-vacuous -- the skipping
    # version also printed a line to stdout.
    ct = _load_clickhouse_test()
    args = _make_args()
    holder, dead_http_port = _closed_port()
    try:
        args.http_port = dead_http_port
        result = _run_sql_dump(ct, args, tmp_path)
    finally:
        holder.close()

    assert result["artifact"].exists(), result["stdout"]
    dump = result["artifact"].read_text(encoding="utf-8", errors="replace")
    assert "thread_name" in dump, dump[:2000]
    assert "query_id" in dump, dump[:2000]


def test_sql_stacktraces_has_no_http_availability_gate():
    # Structural companion to the test above: that one cannot fail if the
    # collector is ever changed to reach the server over HTTP, whereas the
    # mismatch between an HTTP gate and a TCP collector is the defect itself.
    ct = _load_clickhouse_test()
    source = inspect.getsource(ct["print_sql_stacktraces"])
    assert "check_server_liveness" not in source, source


def test_sql_stacktraces_survives_a_closed_socket(tmp_path):
    # Without the pre-check, the collector's own bound and its swallowing of
    # failures are what keep an abort path safe.  A closed socket must produce
    # no artifact, no exception, and one bounded line rather than the
    # `Code: 210` traceback that motivated adding the pre-check originally.
    ct = _load_clickhouse_test()
    args = _make_args()
    http_holder, dead_http_port = _closed_port()
    tcp_holder, dead_tcp_port = _closed_port()
    try:
        args.http_port = dead_http_port
        args.client = f"{args.client.split(' --port=')[0]} --port={dead_tcp_port}"
        result = _run_sql_dump(ct, args, tmp_path)
    finally:
        http_holder.close()
        tcp_holder.close()

    assert not result["artifact"].exists()
    assert "Collected no stacks" in result["stdout"], result["stdout"]
    assert "Traceback" not in result["stderr"], result["stderr"]
    assert len(result["stderr"].splitlines()) <= 10, result["stderr"]


def test_sql_stacktraces_stays_bounded_on_a_wedged_socket(tmp_path):
    # A socket that accepts and never answers is the case a connection-refused
    # test cannot reach: it exercises the 30s timeout that now solely bounds
    # the abort path.  The ceiling is generous so a loaded runner cannot make
    # this flaky while still failing if the bound is removed entirely.
    ct = _load_clickhouse_test()
    args = _make_args()
    http_holder, dead_http_port = _closed_port()
    try:
        args.http_port = dead_http_port
        with _accepting_blackhole() as wedged_tcp_port:
            args.client = (
                f"{args.client.split(' --port=')[0]} --port={wedged_tcp_port}"
            )
            result = _run_sql_dump(ct, args, tmp_path)
    finally:
        http_holder.close()

    assert result["seconds"] < 120, result["seconds"]
    assert not result["artifact"].exists()
    assert "Traceback" not in result["stderr"], result["stderr"]


def test_hung_check_abort_dumps_sql_before_c_stacktraces():
    # Asserted on this site alone: the end-of-run hung-QUERY check and the
    # per-test timeout handler already called both collectors, so a global
    # search would pass even with this site reverted.  Order matters -- the C
    # dump attaches a debugger, and on ASan builds it declines outright.
    ct = _load_clickhouse_test()
    calls, namespace = _record_calls(
        ["print_sql_stacktraces", "print_c_stacktraces", "print"]
    )
    namespace["check_server_liveness"] = lambda *_a, **_k: False
    namespace["args"] = _make_args()
    namespace["args"].hung_check = True
    namespace["stop_testing"] = type("Event", (), {"set": lambda self: None})()

    exec(_abort_site("do_run_tests", "Hung check failed"), namespace)

    assert calls == ["print", "print_sql_stacktraces", "print_c_stacktraces"], calls


def test_server_died_abort_dumps_sql_before_c_stacktraces():
    # The sibling abort site, asserted independently of the hung-check one so
    # that reverting either is caught on its own.
    ct = _load_clickhouse_test()
    calls, namespace = _record_calls(
        ["print_sql_stacktraces", "print_c_stacktraces", "stop_tests"]
    )
    namespace["args"] = _make_args()
    namespace["FailureReason"] = ct["FailureReason"]
    namespace["test_result"] = argparse.Namespace(
        reason=ct["FailureReason"].SERVER_DIED
    )
    namespace["test_case"] = argparse.Namespace(name="some_test")
    namespace["StopTesting"] = ct["StopTesting"]
    namespace["stop_testing"] = type("Event", (), {"set": lambda self: None})()

    try:
        exec(_abort_site("run_tests_array", "FailureReason.SERVER_DIED"), namespace)
    except ct["StopTesting"]:
        pass  # the site ends by raising; the calls before it are the subject

    assert calls == [
        "print_sql_stacktraces",
        "print_c_stacktraces",
        "stop_tests",
    ], calls


def test_sql_stacktraces_writes_nothing_to_the_server_log(tmp_path):
    # Guards the reason a server-side self-dump (SIGTSTP) was rejected: it
    # would log `<Fatal>` lines, and `check_fatal_messages_in_logs` turns any
    # of those into a merge-blocking failure.  This collector must stay purely
    # client-side, so no diagnostic can ever manufacture a blocker.
    ct = _load_clickhouse_test()
    args = _make_args()
    log_dir = Path(_REPO_ROOT) / "ci" / "tmp" / "var" / "log" / "clickhouse-server"
    logs = sorted(log_dir.glob("clickhouse-server*.log")) if log_dir.is_dir() else []
    before = {path: path.stat().st_size for path in logs}

    _run_sql_dump(ct, args, tmp_path)

    for path, offset in before.items():
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            appended = handle.read()
        assert "<Fatal>" not in appended, appended[:2000]
        assert "Received signal" not in appended, appended[:2000]
