"""
Contract test for the build attribution of the performance-comparison system
log export (`ci/jobs/scripts/perf/ci_logs_export.py`).

Both servers of a performance comparison export into the same destination
tables on the CI Logs cluster, and those tables have no column that names a
build - `commit_sha` is the only thing that tells the reference build's rows
from the patched build's rows afterwards. It is read from the server itself
(`system.build_options`), which is a query like any other: it can fail while
the rest of the export is perfectly healthy.

That makes this one step the only place in the module where being best effort
is destructive rather than merely lossy. A lookup that degrades to an empty
string does not lose the logs, it exports them as rows that no later query can
attribute to a build - and reports the export as successful. So the lookup is
fail-closed, and these tests pin both halves of it: a hash comes back only when
the server actually answered with one, and every other outcome raises, which is
what makes the caller skip that server with a warning instead of exporting
unattributable rows.
"""

import inspect
import os
import re
import shlex
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as performance_tests
from ci.jobs.scripts.perf import ci_logs_export

# A real build answers with the full 40-character hash of its commit
_SHA = "e161dcbb1bf03c9e0c168fb85ab67a70c6e74a43"

_PORT = 19001


def _fake_client(tmp_path, monkeypatch, stdout="", stderr="", exit_code=0):
    """Put a `clickhouse-client` with the given answer first on PATH.

    The module runs the real binary through `subprocess.run(["clickhouse-client",
    ...])`, so faking the binary - rather than the module's own helpers - is
    what keeps this test on the path that actually runs in CI, including the
    strictness of `_run_client` itself.
    """
    client = tmp_path / "clickhouse-client"
    client.write_text(
        # Always drain the query from stdin: the caller writes it there and
        # would get a broken pipe from a script that exits without reading.
        "#!/bin/bash\n"
        "cat > /dev/null\n"
        f"printf '%s' {shlex.quote(stdout)}\n"
        f"printf '%s' {shlex.quote(stderr)} >&2\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    client.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return client


def test_the_hash_the_server_reports_is_returned(tmp_path, monkeypatch):
    _fake_client(tmp_path, monkeypatch, stdout=f"{_SHA}\n")

    assert ci_logs_export.get_server_commit_sha(_PORT) == _SHA


def test_a_failed_lookup_raises_instead_of_returning_an_empty_hash(
    tmp_path, monkeypatch
):
    # This is the case the export must not paper over: the client exits
    # non-zero, and a non-strict helper would hand the caller "" and let the
    # export proceed.
    _fake_client(
        tmp_path,
        monkeypatch,
        stderr="Code: 209. DB::NetException: Timeout exceeded",
        exit_code=209,
    )

    with pytest.raises(RuntimeError):
        ci_logs_export.get_server_commit_sha(_PORT)


def test_a_build_without_git_information_raises(tmp_path, monkeypatch):
    # `cmake/git.cmake` leaves GIT_HASH empty when git is unavailable at build
    # time, so the query succeeds and reports nothing usable.
    _fake_client(tmp_path, monkeypatch, stdout="\n")

    with pytest.raises(RuntimeError):
        ci_logs_export.get_server_commit_sha(_PORT)


@pytest.mark.parametrize("answer", ["unknown", "N/A", "0", f"{_SHA} dirty"])
def test_an_answer_that_is_not_a_hash_raises(answer, tmp_path, monkeypatch):
    _fake_client(tmp_path, monkeypatch, stdout=f"{answer}\n")

    with pytest.raises(RuntimeError):
        ci_logs_export.get_server_commit_sha(_PORT)


def test_the_export_stage_skips_a_server_whose_build_it_cannot_name():
    """The fail-closed lookup is only fail-closed if the caller acts on it.

    The stage is a closure inside `main`, so its contract is asserted on the
    source: the hash comes from the strict helper (not from the non-strict
    `CHServer.ask`), and a failure skips that server instead of falling through
    to the export with whatever the lookup left behind.
    """
    source = inspect.getsource(performance_tests.main)
    stage = source[source.index("JobStages.EXPORT_LOGS in stages") :]
    stage = stage[: stage.index("JobStages.REPORT in stages")]

    assert "ci_logs_export.get_server_commit_sha(server.port)" in stage
    assert (
        "build_options" not in stage
    ), "the build commit is read here again instead of through the strict helper"
    assert re.search(
        r"except Exception as e:\n(.*\n)*?\s+continue\n", stage
    ), "a server whose build commit cannot be read is not skipped"

    # `commit_sha` must be the value that lookup returned, and nothing else.
    assert "commit_sha=server_sha," in stage
    assert stage.count("commit_sha=") == 1
