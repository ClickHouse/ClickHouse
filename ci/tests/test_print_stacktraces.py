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

import pytest

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


def _set_global(ct, name, value):
    # runpy.run_path returns a COPY of the executed namespace, so assigning to
    # ct[name] does not affect what the loaded functions resolve.  Patch the
    # namespace the functions actually close over.
    ct["get_all_server_pids"].__globals__[name] = value


def _fake_pgrep(*tables):
    # Honour the `command=` substring filter exactly as the real pgrep does, so
    # a caller reintroducing that prefilter is visible to these tests.
    #
    # More than one table means the process table CHANGES between calls: call N
    # sees table N, and the last one repeats.  That is what makes a caller
    # taking two independent snapshots visible.
    calls = []

    def pgrep(ppid=None, pgid=None, command=None):
        rows = tables[min(len(calls), len(tables) - 1)]
        calls.append(1)
        out = list(rows)
        if ppid is not None:
            out = [p for p in out if p[1] == ppid]
        if pgid is not None:
            out = [p for p in out if p[2] == pgid]
        if command is not None:
            out = [p for p in out if command in p[3]]
        return out

    return pgrep


def _fake_comm(comms):
    # Stand in for /proc/<pid>/comm.  Anything not listed is a plain serving
    # process; the real helper returns "" for an unreadable pid, which is also
    # "not a watchdog".
    return lambda pid: comms.get(pid, "clickhouse")


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


#
# Server-PID identification.
#
# `pgrep(command="clickhouse-server")` is a substring match on the whole ps
# command column, so a `/bin/sh -c "clickhouse-server ..."` wrapper (CI starts
# the server with shell=True) also matched, and being the parent it sorted
# first.  That mislabelled the shell as "main server" in the lldb dump and made
# get_server_memory_fraction read the shell's RSS, so the memory-pressure
# worker shed could never fire.
#

_CMDLINE_CASES = [
    # (cmdline, binary, expected)
    ("clickhouse-server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    ("/usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    ("clickhouse server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    ("/usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    ("clickhouse --server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    ("/usr/bin/clickhouse --server --config-file /etc/clickhouse-server/config.xml", "clickhouse", True),
    # A custom --binary may be any path; shell_config.sh builds "$CLICKHOUSE_BINARY server".
    ("/opt/ch-build server --config-file /etc/clickhouse-server/config.xml", "/opt/ch-build", True),
    ("/opt/ch-build --server --config-file /etc/clickhouse-server/config.xml", "/opt/ch-build", True),
    # ... but only that binary: a `server` selector under any other argv[0] is not ours.
    ("/opt/other-thing server --config-file /etc/clickhouse-server/config.xml", "/opt/ch-build", False),
    ("podman server --config-file /etc/clickhouse-server/config.xml", "clickhouse", False),
    # Shell wrappers: the defect being fixed.
    ("/usr/bin/dash -c clickhouse-server --config-file /etc/clickhouse-server/config.xml", "clickhouse", False),
    ('/bin/sh -c "clickhouse-server --config-file /etc/clickhouse-server/config.xml"', "clickhouse", False),
    ("bash -c clickhouse-server --config-file /etc/clickhouse-server/config.xml", "clickhouse", False),
    # The watchdog renames argv0 (src/Daemon/BaseDaemon.cpp); it does not serve.
    ("clickhouse-watchdog --config-file /etc/clickhouse-server/config.xml", "clickhouse", False),
    ("clickhouse-watchdog --config-file /etc/clickhouse-server/config.xml", "/opt/ch-build", False),
    # Other multicall entry points.
    ("clickhouse local --query SELECT 1", "clickhouse", False),
    ("clickhouse --local --query SELECT 1", "clickhouse", False),
    ("clickhouse client --port 9000", "clickhouse", False),
    ("chl --query SELECT 1", "clickhouse", False),
    # A bare accepted argv0 with no selector at all: there is no argv[1] to read.
    ("clickhouse", "clickhouse", False),
    ("/opt/ch-build", "/opt/ch-build", False),
    ("", "clickhouse", False),
]


@pytest.mark.parametrize("cmdline,binary,expected", _CMDLINE_CASES)
def test_is_server_cmdline(cmdline, binary, expected):
    ct = _load_clickhouse_test(require_server=False)
    assert ct["_is_server_cmdline"](cmdline, binary) is expected, cmdline


# (rows, binary, comms, expected).  Rows are [pid, ppid, pgid, cmdline] in pid
# order, as pgrep() returns them; comms maps pid -> /proc/<pid>/comm for the
# processes whose short name differs from the default "clickhouse".
_GRAPH_CASES = [
    # CI shape: shell wrapper, watchdog (rename is distinct here), serving child.
    (
        [
            [699, 600, 600, "/usr/bin/dash -c clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
            [701, 699, 600, "clickhouse-watchd --config-file /etc/clickhouse-server/config.xml"],
            [702, 701, 600, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {701: "ClickHouseWatch"},
        [702],
    ),
    # Bare multicall: the watchdog rename is truncated to len("clickhouse"), so
    # the parent presents the same argv as its serving child.
    (
        [
            [800, 700, 700, "sh -c clickhouse server --config-file /etc/clickhouse-server/config.xml"],
            [801, 800, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
            [802, 801, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {801: "ClickHouseWatch"},
        [802],
    ),
    # No watchdog: the lone server must survive even though it has children.
    (
        [
            [900, 700, 700, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
            [901, 900, 700, "some-child-of-the-server"],
        ],
        "clickhouse",
        {},
        [900],
    ),
    # Two independent replicas share a ppid, so both must survive.
    (
        [
            [910, 700, 700, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
            [911, 700, 700, "clickhouse-server --config-file /etc/clickhouse-server/config2.xml"],
        ],
        "clickhouse",
        {},
        [910, 911],
    ),
    # A multicall server whose cmdline does NOT contain "clickhouse-server":
    # the only process carrying that substring is the shell that started it, so
    # a substring prefilter would drop the server and keep only the shell.
    (
        [
            [920, 700, 700, "sh -c exec clickhouse-server-launcher"],
            [921, 920, 700, "/opt/ch-build server --config-file /etc/ch/cfg.xml"],
        ],
        "/opt/ch-build",
        {},
        [921],
    ),
    # Orphaned bare-multicall watchdog: the serving child is already reaped, so
    # the ppid rule alone cannot see it and would report the watchdog as the
    # server.  This is the window in which the abort-path diagnostics run.
    (
        [
            [930, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
            [931, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config2.xml"],
        ],
        "clickhouse",
        {930: "ClickHouseWatch"},
        [931],
    ),
    # ... and the opposite direction: a genuine watchdog-less server with no
    # children at all must still be returned (an over-strict rule would drop it).
    (
        [
            [940, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {},
        [940],
    ),
    # Fail-safe: an orphaned watchdog is the ONLY match.  Reporting nothing
    # makes print_c_stacktraces claim the server crashed, which is worse than
    # dumping the watchdog, so the list must not go empty.
    (
        [
            [950, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {950: "ClickHouseWatch"},
        [950],
    ),
    # comm is inherited across fork, and BaseDaemon renames the parent AFTER
    # forking, so on a watchdog RESTART iteration the fresh serving child
    # inherits "ClickHouseWatch".  comm alone would then reject both and fall
    # back to keeping the watchdog too; the ppid rule is what still resolves it.
    (
        [
            [970, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
            [971, 970, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {970: "ClickHouseWatch", 971: "ClickHouseWatch"},
        [971],
    ),
    # An unreadable /proc/<pid>/comm (the process exited between the ps scan and
    # the read) is not watchdog evidence.
    (
        [
            [960, 700, 700, "clickhouse server --config-file /etc/clickhouse-server/config.xml"],
        ],
        "clickhouse",
        {960: ""},
        [960],
    ),
]


@pytest.mark.parametrize("rows,binary,comms,expected", _GRAPH_CASES)
def test_get_all_server_pids_process_graph(rows, binary, comms, expected):
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.binary = binary
    _set_global(ct, "pgrep", _fake_pgrep(rows))
    _set_global(ct, "_process_comm", _fake_comm(comms))

    assert ct["get_all_server_pids"](args) == expected


def test_process_comm_never_raises_on_a_dead_pid():
    # get_all_server_pids runs on the abort path, where an exception would
    # replace the diagnostics it exists to collect.  pid 0 is never a process.
    ct = _load_clickhouse_test(require_server=False)
    assert ct["_process_comm"](0) == ""


@pytest.mark.skipif(
    not os.path.exists("/proc/self/comm"), reason="no procfs (non-Linux host)"
)
def test_process_comm_reads_our_own_comm():
    # The other direction: the helper must actually read comm, not just swallow
    # everything.  Our own comm is whatever the test runner is called.
    ct = _load_clickhouse_test(require_server=False)
    with open("/proc/self/comm", encoding="utf-8") as f:
        expected = f.read().strip()
    assert ct["_process_comm"](os.getpid()) == expected
    assert expected


def test_print_c_stacktraces_labels_one_main_server_on_a_changing_table():
    # print_c_stacktraces used to call get_server_pid() and get_all_server_pids()
    # separately, each running its own `ps`.  If the table changes in between
    # (here: the server picked by the first scan has exited), the labelling loop
    # finds no pid equal to server_pid and marks EVERY live server "replica" --
    # the same "no main server in the dump" symptom this PR fixes, from a
    # different cause.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.binary = "clickhouse"
    first = [
        [700, 600, 600, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
        [701, 600, 600, "clickhouse-server --config-file /etc/clickhouse-server/config2.xml"],
    ]
    second = [
        [701, 600, 600, "clickhouse-server --config-file /etc/clickhouse-server/config2.xml"],
    ]
    _set_global(ct, "pgrep", _fake_pgrep(first, second))
    _set_global(ct, "_process_comm", _fake_comm({}))
    # lldb is not available (and not the point): return a large-enough payload so
    # the labelled branch is exercised without attaching to anything.
    _set_global(ct, "get_stacktraces_from_lldb", lambda pid, **kw: "frame " * 400)
    _set_global(ct, "_append_stacktrace_log", lambda filename, block: None)

    captured = io.StringIO()
    with redirect_stdout(captured):
        ct["print_c_stacktraces"](args)
    output = captured.getvalue()

    assert output.count("from main server process") == 1, output
    assert "It must have crashed" not in output, output


def test_get_server_pid_agrees_with_get_all_server_pids():
    # The two helpers used to run separate pgrep() calls and disagree: the
    # shell wrapper sorts first, so get_server_pid() returned it while
    # get_all_server_pids() also listed the real server.  Fails on master.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.binary = "clickhouse"
    rows = [
        [699, 600, 600, "/usr/bin/dash -c clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
        [701, 699, 600, "clickhouse-watchd --config-file /etc/clickhouse-server/config.xml"],
        [702, 701, 600, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
    ]
    _set_global(ct, "pgrep", _fake_pgrep(rows))
    # Fake comm too: these pids are low enough to exist on the host, and the
    # real /proc read would make the case depend on what else is running.
    _set_global(ct, "_process_comm", _fake_comm({}))

    all_pids = ct["get_all_server_pids"](args)
    assert 699 not in all_pids, all_pids
    assert ct["get_server_pid"](args) == 702
    assert ct["get_server_pid"](args) == all_pids[0]


def test_get_all_server_pids_finds_the_live_server():
    # Guards the opposite failure mode: a filter so strict that stacktrace
    # collection is silently disabled in a real environment.
    ct = _load_clickhouse_test()
    args = _make_args()

    pids = ct["get_all_server_pids"](args)
    assert pids, "get_all_server_pids() found no server against the live ci/tests server"
    cmdlines = {p[0]: p[3] for p in ct["pgrep"]()}
    for pid in pids:
        assert " -c " not in cmdlines.get(pid, ""), cmdlines.get(pid)


def test_get_server_memory_fraction_reads_the_server_rss():
    # Defect B: the fraction fed to the worker shed came from the shell
    # wrapper's RSS (a few MB) against the server's multi-GB limit, so
    # MEMORY_PRESSURE_THRESHOLD could never be reached and the shed was dead.
    ct = _load_clickhouse_test(require_server=False)
    args = _make_args()
    args.binary = "clickhouse"
    rows = [
        [699, 600, 600, "/usr/bin/dash -c clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
        [702, 699, 600, "clickhouse-server --config-file /etc/clickhouse-server/config.xml"],
    ]
    _set_global(ct, "pgrep", _fake_pgrep(rows))
    _set_global(ct, "_process_comm", _fake_comm({}))

    page = os.sysconf("SC_PAGE_SIZE")
    # The shell's RSS is negligible; the server's is 90% of the limit.
    rss_pages = {699: 920, 702: 9000}
    maximum = 10000 * page

    fn = ct["get_server_memory_fraction"]
    # These caches live on the function object; a stale one would silently make
    # this pass for the wrong reason.
    for attr in ("_max_memory", "_server_pid"):
        if hasattr(fn, attr):
            delattr(fn, attr)
    fn._max_memory = maximum

    def fake_open(path, *a, **kw):
        pid = int(str(path).split("/")[2])
        return io.StringIO(f"99999 {rss_pages[pid]} 0 0 0 0 0")

    fn.__globals__["open"] = fake_open
    try:
        fraction = fn(args)
    finally:
        del fn.__globals__["open"]
        for attr in ("_max_memory", "_server_pid"):
            if hasattr(fn, attr):
                delattr(fn, attr)

    assert fraction == pytest.approx(0.9), fraction


def test_is_asan_build_falls_back_to_binary_when_flags_missing():
    # Startup-failure path: flags were never collected (server never served a
    # query), so the ASan bit is read from the binary itself rather than from
    # ASAN_OPTIONS. The CI tests job runs a master release build, not an ASan
    # build, so this must resolve to False without raising.
    ct = _load_clickhouse_test()
    args = _make_args()
    delattr(args, "build_flags")

    assert ct["is_asan_build"](args) is False
