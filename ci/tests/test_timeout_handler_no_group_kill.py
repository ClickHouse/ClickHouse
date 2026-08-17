"""
A single test's timeout must not abort the whole run.

`timeout_handler` in `tests/clickhouse-test` fires from a per-test SIGALRM. It
used to call `stop_tests()`, whose `killpg` broadcasts SIGTERM to our own
process group - which every parallel worker and the parent share. One test
exceeding its own deadline therefore terminated the entire run, and the job
side relabelled the result as "Server died" (exit -15 is in
`ABORTED_RUN_EXIT_CODES`), hiding a run where every executed test had passed.

The handler must instead kill only the timing-out test's own out-of-group
clients. Both counters are asserted so the test cannot pass vacuously:
broadcasts to our own group must be 0, while the out-of-group child must still
be killed. A third check pins the whole-run callers, which still need the
broadcast.

No server and no wall-clock: the real handler body is exec'd against the real
module globals with `killpg` / `pgrep` / `getpgid` faked.
"""

import inspect
import io
import os
import runpy
import signal
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

_ct = runpy.run_path(_CLICKHOUSE_TEST)
# `runpy.run_path` returns a COPY of the executed namespace, while the functions
# defined in it close over the original dict - so patching has to go through a
# function's own `__globals__`, which is that original.
_CT_GLOBALS = _ct["cleanup_child_processes"].__globals__

_OUR_PGID = 4242
_IN_GROUP_CHILD = 5001  # a sibling worker: same group, must be spared
_OUT_OF_GROUP_CHILD = 5002  # this test's own client (start_new_session=True)
_OUT_OF_GROUP_PGID = 5002


def _timeout_handler_source():
    """The real nested `timeout_handler` body, lifted out of `run_tests_array`."""
    lines = inspect.getsource(_ct["run_tests_array"]).splitlines()
    start = next(
        i for i, l in enumerate(lines) if l.strip().startswith("def timeout_handler")
    )
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = start + 1
    while end < len(lines):
        line = lines[end]
        if line.strip() and (len(line) - len(line.lstrip())) <= indent:
            break
        end += 1
    return textwrap.dedent("\n".join(lines[start:end]))


class _Recorder:
    def __init__(self):
        self.killpg = []  # (pgid, signal) reaching os.killpg
        self.killed_groups = []  # pgid arguments to kill_process_group

    @property
    def own_group_broadcasts(self):
        return [c for c in self.killpg if c[0] == _OUR_PGID]


@pytest.fixture(name="rec")
def _rec(monkeypatch):
    rec = _Recorder()
    pgids = {
        os.getpid(): _OUR_PGID,
        _IN_GROUP_CHILD: _OUR_PGID,
        _OUT_OF_GROUP_CHILD: _OUT_OF_GROUP_PGID,
    }
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: rec.killpg.append((pgid, sig)))
    monkeypatch.setattr(os, "getpgid", lambda pid: pgids[pid])
    monkeypatch.setitem(
        _CT_GLOBALS,
        "kill_process_group",
        lambda pgid, fatal_log: rec.killed_groups.append(pgid),
    )
    monkeypatch.setitem(
        _CT_GLOBALS,
        "pgrep",
        lambda ppid=None, pgid=None, command=None: [
            [_IN_GROUP_CHILD, os.getpid(), _OUR_PGID, "clickhouse-client sibling"],
            [
                _OUT_OF_GROUP_CHILD,
                os.getpid(),
                _OUT_OF_GROUP_PGID,
                "clickhouse-client own",
            ],
        ],
    )
    monkeypatch.setitem(_CT_GLOBALS, "print_sql_stacktraces", lambda args: None)
    monkeypatch.setitem(_CT_GLOBALS, "print_c_stacktraces", lambda args: None)
    return rec


def _run_timeout_handler(monkeypatch):
    # The handler closes over `args` and `cleanup_output` in `run_tests_array`;
    # exec'd standalone it reads them from the globals it is given, which must be
    # the real module globals so the patched helpers above are the ones it calls.
    monkeypatch.setitem(_CT_GLOBALS, "args", None)  # only the stubbed dumps see it
    monkeypatch.setitem(_CT_GLOBALS, "cleanup_output", io.StringIO())
    exec(_timeout_handler_source(), _CT_GLOBALS)  # pylint: disable=exec-used
    handler = _CT_GLOBALS.pop("timeout_handler")
    handler(signal.SIGALRM, None)


def test_per_test_timeout_does_not_signal_our_own_process_group(rec, monkeypatch):
    """The defect: one test's deadline SIGTERMed all 24 workers and the parent."""
    with pytest.raises(TimeoutError):
        _run_timeout_handler(monkeypatch)

    assert rec.own_group_broadcasts == [], (
        f"timeout_handler signalled our own process group {_OUR_PGID}: "
        f"{rec.own_group_broadcasts}. That kills every parallel worker and the "
        "parent, so the run exits -15 and is reported as 'Server died'."
    )


def test_per_test_timeout_still_kills_the_tests_own_clients(rec, monkeypatch):
    """Counter (b): the fix must not disarm the per-test teardown it replaces."""
    with pytest.raises(TimeoutError):
        _run_timeout_handler(monkeypatch)

    assert rec.killed_groups == [_OUT_OF_GROUP_PGID], (
        "timeout_handler must kill the timing-out test's own out-of-group "
        f"clients, got {rec.killed_groups}"
    )


def test_whole_run_teardown_still_broadcasts_to_the_group(rec):
    """`cleanup_child_processes` is the whole-run primitive (SERVER_DIED,
    KeyboardInterrupt, hung check). Extracting the per-child loop must leave its
    broadcast intact."""
    _ct["cleanup_child_processes"](os.getpid())

    assert rec.own_group_broadcasts == [
        (_OUR_PGID, signal.SIGTERM)
    ], f"cleanup_child_processes must still SIGTERM our group, got {rec.killpg}"
    assert rec.killed_groups == [_OUT_OF_GROUP_PGID]
