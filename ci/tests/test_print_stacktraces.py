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

They also pin the stateless job's upload wiring
(``ci.jobs.functional_tests``): the dumps are written relative to the harness
cwd, which is outside the server log directory that ``prepare_logs`` globs, so
they are uploaded only if the job attaches them by name -- collected but not
attached, a dump dies with the runner and the abort is still undiagnosable.
Both halves are asserted: what the collector returns, and that its result is
attached to the list the result uploads.

The placement of the clear and of the attach is asserted too, because both are
mode-dependent if bound to the wrong guard.  The attach must sit outside any
stage-membership ``if``: per-test-coverage and local runs drop ``COLLECT_LOGS``
entirely, so an attach inside that stage never runs for them.  The clear must
sit outside the ``res`` guard: a setup failure skips the tests yet still reaches
the attach, which would upload a previous job's dump as this run's.
"""

import argparse
import ast
import contextlib
import inspect
import io
import os
import runpy
import socket
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

sys.path.insert(0, str(_REPO_ROOT))

from ci.jobs import functional_tests
from ci.jobs.functional_tests import STACKTRACE_LOGS, collect_stacktrace_logs


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
        # Yield `accepted` too: a caller asserting only on elapsed time cannot
        # tell a wedged socket from a fast failure, which would silently retest
        # the connection-refused path instead of the timeout.
        yield srv.getsockname()[1], accepted
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


def _abort_site(function_name, marker, expected_test):
    """Compile the one `if` statement that forms an abort site, straight out
    of the real file.

    Executing the real statement with stubbed callees checks the calls a site
    makes AND their order, which a search for the call's name cannot: it
    would also match the same call in a sibling site, so reverting one site
    would still pass while the other covers for it.

    `expected_test` is the site's `if` condition, unparsed. Selecting the most
    deeply nested match is a structural property, but nesting alone cannot say
    the winner is the intended statement: an equally nested sibling elsewhere in
    the function would be selected silently, and the test would then keep
    passing while covering a different site.
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
    # Innermost match: enclosing `if`s (`args.hung_check`, the status
    # dispatch) also contain the marker. Keyed on indentation, not on source
    # length -- length is only a proxy, and one added comment inside the
    # intended block can make it exceed its own parent.
    site = max(candidates, key=lambda node: node.col_offset)
    assert ast.unparse(site.test) == expected_test, (
        f"{function_name}: selected the `if` at line {site.lineno} testing "
        f"{ast.unparse(site.test)!r}, expected {expected_test!r} -- the abort "
        "site moved or another equally nested site now matches the marker"
    )
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
        with _accepting_blackhole() as (wedged_tcp_port, accepted):
            # Pin the host to match the fixture's IPv4-only bind: the client's
            # own default resolution of `localhost` may prefer ::1.
            args.client = (
                f"{args.client.split(' --port=')[0]}"
                f" --host=127.0.0.1 --port={wedged_tcp_port}"
            )
            result = _run_sql_dump(ct, args, tmp_path)
            connections = len(accepted)
    finally:
        http_holder.close()

    # Without this, a client that failed fast would leave the 30s bound
    # asserted nowhere while the test still passed.
    assert connections, "the client never connected, so no timeout was exercised"
    assert result["seconds"] < 120, result["seconds"]
    assert not result["artifact"].exists()
    assert "Traceback" not in result["stderr"], result["stderr"]


def test_hung_check_abort_dumps_sql_before_c_stacktraces():
    # Asserted on this site alone: the end-of-run hung-QUERY check and the
    # per-test timeout handler already called both collectors, so a global
    # search would pass even with this site reverted.  Order matters -- the C
    # dump attaches a debugger, and on ASan builds it declines outright.
    # Called for its preconditions; this site's callees are all stubbed below.
    _load_clickhouse_test()
    calls, namespace = _record_calls(
        ["print_sql_stacktraces", "print_c_stacktraces", "print"]
    )
    namespace["check_server_liveness"] = lambda *_a, **_k: False
    namespace["args"] = _make_args()
    namespace["args"].hung_check = True
    namespace["stop_testing"] = type("Event", (), {"set": lambda self: None})()

    exec(
        _abort_site(
            "do_run_tests", "Hung check failed", "not check_server_liveness(args)"
        ),
        namespace,
    )

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
        exec(
            _abort_site(
                "run_tests_array",
                "FailureReason.SERVER_DIED",
                "test_result.reason == FailureReason.SERVER_DIED",
            ),
            namespace,
        )
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
    # Assert rather than skip: an empty glob would make every assertion below
    # a no-op, and this is the invariant that keeps a diagnostic from ever
    # manufacturing a merge blocker. Name the path so a layout drift in
    # ClickHouseService is diagnosable from the failure line alone.
    assert (
        logs
    ), f"no clickhouse-server*.log under {log_dir} -- server log layout changed"
    before = {path: path.stat().st_size for path in logs}

    _run_sql_dump(ct, args, tmp_path)

    for path, offset in before.items():
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            appended = handle.read()
        assert "<Fatal>" not in appended, appended[:2000]
        assert "Received signal" not in appended, appended[:2000]


def test_stacktrace_log_names_match_clickhouse_test(tmp_path):
    # The stateless job attaches the dumps by name, so a rename in
    # clickhouse-test would silently stop them being uploaded.
    ct = _load_clickhouse_test()
    assert set(STACKTRACE_LOGS) == {
        ct["SQL_STACKTRACES_LOG"],
        ct["C_STACKTRACES_LOG"],
    }, STACKTRACE_LOGS


def test_collect_stacktrace_logs_finds_dumps_written_by_the_run(tmp_path):
    # Pins the stateless job's upload wiring: the dumps land relative to the
    # harness cwd, outside the server log dir `prepare_logs` globs, so they are
    # attached only if this returns them.
    ct = _load_clickhouse_test()
    args = _make_args()
    holder, dead_http_port = _closed_port()
    try:
        args.http_port = dead_http_port
        result = _run_sql_dump(ct, args, tmp_path)
    finally:
        holder.close()

    assert result["artifact"].exists(), result["stdout"]
    assert collect_stacktrace_logs(tmp_path) == [str(result["artifact"])]


def test_collect_stacktrace_logs_attaches_nothing_on_a_green_run(tmp_path):
    # The dumps exist only after an abort, so a run that never aborted must
    # attach nothing rather than an empty or missing path.
    assert collect_stacktrace_logs(tmp_path) == []


def test_stale_dumps_are_cleared_before_the_test_stage():
    # A dump left by a previous job in the same workspace would be attached to a
    # green run as if it had aborted: clickhouse-test appends, and the paths are
    # gitignored so `git clean -ffd` in `Runner._pre_run` keeps them.
    source = inspect.getsource(functional_tests.main)
    stage, _, rest = source.partition("JobStages.TEST in stages")
    assert rest, "the TEST stage guard moved"
    # The COLLECT_LOGS guard only bounds the span searched below; the attach
    # itself now lives past it, at main()'s top level.
    clear, _, after_collect = rest.partition("JobStages.COLLECT_LOGS in stages")
    assert after_collect, "the COLLECT_LOGS stage guard moved"
    # Clearing must happen on TEST entry, before any dump this job writes.
    assert "unlink()" in clear, clear[:2000]
    assert "collect_stacktrace_logs" in clear, clear[:2000]
    # Presence alone lets the clear be relocated later in the stage, where it
    # deletes this run's own dumps instead of a previous job's.
    assert clear.index("unlink()") < clear.index("run_tests("), (
        "the stale-dump clear must precede the first run_tests call, else the "
        "job deletes the dumps this run just wrote"
    )


def test_the_stale_dump_clear_is_not_under_the_res_guard():
    # Ordering and presence are not enough: `res` goes False on any setup
    # failure (install, or any step of server start), and the attach is not
    # under `res`. A clear guarded by `res` is therefore skipped exactly on the
    # runs where a previous job's dump would still be attached, reporting an
    # unrelated job's abort stacktrace as this run's.
    main = _main_ast()
    candidates = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.If)
        and "collect_stacktrace_logs" in ast.unparse(node)
        and "unlink" in ast.unparse(node)
    ]
    assert candidates, (
        "no `if` in main() guards the stale-dump clear -- it must be guarded by "
        "stage membership, so that a resume at a later stage keeps the dump on "
        "disk that it was resumed to collect"
    )
    # Innermost: any enclosing `if` also contains the clear's source, and the
    # guard the clear actually runs under is the subject here.
    condition = ast.unparse(max(candidates, key=lambda node: node.col_offset).test)
    assert "res" not in condition.split(), (
        f"the stale-dump clear is guarded by {condition!r}, which tests `res`: a "
        "setup failure then skips the clear while still reaching the attach, so "
        "a previous job's dump is uploaded as this run's"
    )
    assert "JobStages.TEST in stages" in condition, condition


def _main_ast():
    tree = ast.parse(Path(functional_tests.__file__).read_text(encoding="utf-8"))
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )


def _enclosing_statements(main, target):
    """The statements of `main` that lexically contain `target`, outermost first.

    Empty for a top-level statement of `main`.  `ast.walk` cannot answer this:
    it flattens the tree, so it reports that a node exists but not under which
    guards it runs -- which is the whole property here.
    """
    chain = []

    def descend(node, ancestors):
        for child in ast.iter_child_nodes(node):
            if child is target:
                chain.extend(ancestors)
                return True
            deeper = ancestors
            if isinstance(child, ast.stmt):
                deeper = ancestors + [child]
            if descend(child, deeper):
                return True
        return False

    descend(main, [])
    return chain


def test_stacktrace_dumps_reach_the_uploaded_result():
    # The tests above pin what `collect_stacktrace_logs` returns; this pins that
    # those paths are attached, which is the whole point -- a dump that is
    # collected but never attached dies with the runner.
    #
    # Read as a tree, not as text: the properties are positional, so a substring
    # search would keep passing with the attach moved after the result is built.
    main = _main_ast()
    attaches = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.AugAssign)
        and ast.unparse(node.target) == "debug_files"
        and "collect_stacktrace_logs" in ast.unparse(node.value)
    ]
    assert attaches, (
        "no `debug_files += collect_stacktrace_logs(...)` in main() -- the dumps "
        "are then collected but never attached"
    )

    # Stage membership varies by mode: `is_per_test_coverage` and
    # `info.is_local_run` both remove COLLECT_LOGS outright, so an attach inside
    # that stage never runs for the 8 production per-test-coverage jobs. Asserted
    # before the top-level check below so a stage-scoped attach names this reason.
    enclosing = _enclosing_statements(main, attaches[0])
    staged = [
        node
        for node in enclosing
        if isinstance(node, ast.If) and "in stages" in ast.unparse(node.test)
    ]
    assert not staged, (
        f"the attach is inside `if {ast.unparse(staged[0].test)}:` -- that stage "
        "is removed entirely for per-test-coverage and local runs, so their "
        "abort dumps are never uploaded"
    )
    assert not enclosing, (
        "the attach is nested inside "
        f"{ast.unparse(enclosing[-1]).splitlines()[0]!r} rather than being a "
        "top-level statement of main(), so some runs skip it"
    )

    uploads = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "Result.create_from"
        and any(
            keyword.arg == "files" and "debug_files" in ast.unparse(keyword.value)
            for keyword in node.keywords
        )
    ]
    assert uploads, (
        "no `Result.create_from(files=... debug_files ...)` in main() -- the "
        "list the dumps are appended to is no longer the uploaded one"
    )
    assert attaches[0].lineno < uploads[0].lineno, (
        f"the attach at line {attaches[0].lineno} runs after the result is "
        f"built at line {uploads[0].lineno}, so the dumps are not uploaded"
    )
