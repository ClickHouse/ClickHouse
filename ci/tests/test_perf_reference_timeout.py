"""
Tests for a performance test query that runs out of its time limit on one of the two servers
(`tests/performance/scripts/perf.py`, `reference_timeout_warnings` of `ci/jobs/performance_tests.py`).

A pull request can add a test of a query that only its own change makes fast enough: on the reference server (the
build without the change) the query runs out of the time limit. That is not an error of the test: the query is
measured on the tested server only, as a `partial` query, and the report gets a warning. A time limit exceeded on
the tested server is an error, and so is any other error on the reference server.

`perf.py` runs as it does in the job, as a separate process, against a stand-in for `clickhouse_driver` whose
"servers" answer every query at once, except the ones a test tells to fail.
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.performance_tests import reference_timeout_warnings

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
PERF_PY = os.path.join(REPO_ROOT, "tests/performance/scripts/perf.py")

REFERENCE_PORT = 9001
TESTED_PORT = 9002

TIMEOUT_EXCEEDED = 159
MEMORY_LIMIT_EXCEEDED = 241

# The second query is the one the tests make fail; the others show whether the test went on.
TEST_XML = """
<test>
    <query>SELECT 'first'</query>
    <query>SELECT 'slow'</query>
    <query>SELECT 'last'</query>
</test>
"""

SHELL_TEST_XML = """
<test>
    <query type="shell"><![CDATA[if [ "$CLICKHOUSE_PORT_TCP" = 9001 ] && [ -e "$SLOW_MARKER" ]; then sleep 5; fi; touch "$SLOW_MARKER"]]></query>
    <query>SELECT 'last'</query>
</test>
"""

# `clickhouse_driver`: a query fails with the error code of the first matching rule of `PERF_STUB_FAILURES`
# (server port, substring of the query, substring of the query id).
DRIVER_STUB = '''
import json
import os

from . import errors

FAILURES = json.loads(os.environ.get("PERF_STUB_FAILURES", "[]"))


class LastQuery:
    elapsed = 0.01


class Client:
    def __init__(self, host=None, port=None, settings=None, **kwargs):
        self.port = int(port)
        self.settings = dict(settings or {})
        self.last_query = LastQuery()

    def execute(self, query, params=None, query_id=None, settings=None, **kwargs):
        for failure in FAILURES:
            if failure["port"] == self.port and failure["query"] in query and failure["query_id"] in (query_id or ""):
                raise errors.ServerException("DB::Exception: stub failure", failure["code"])
        return []
'''

DRIVER_ERRORS_STUB = '''
class Error(Exception):
    pass


class ServerException(Error):
    def __init__(self, message, code):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self):
        return f"Code: {self.code}. {self.message}"
'''

# `perf.py` takes only the p-value of the t-test, to decide whether to profile a query.
SCIPY_STATS_STUB = '''
class Result:
    pvalue = 1.0


def ttest_ind(a, b, equal_var=True):
    return Result()
'''


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8")


def _failure(port, query_id, code=TIMEOUT_EXCEEDED):
    return {"port": port, "query": "slow", "query_id": query_id, "code": code}


def _run_perf(tmp_path, failures, xml=TEST_XML, extra_args=()):
    """Run `perf.py` on a test of the queries of `xml` and return the finished process."""
    stubs = tmp_path / "stubs"
    _write(stubs / "clickhouse_driver" / "__init__.py", DRIVER_STUB)
    _write(stubs / "clickhouse_driver" / "errors.py", DRIVER_ERRORS_STUB)
    _write(stubs / "scipy" / "__init__.py", "")
    _write(stubs / "scipy" / "stats.py", SCIPY_STATS_STUB)
    # For shell queries: `perf.py` asks the binary of each server for the options of `clickhouse local`.
    binary = tmp_path / "clickhouse"
    _write(binary, "#!/bin/sh\n")
    binary.chmod(0o755)
    _write(tmp_path / "reference_timeout.xml", xml)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(stubs)
    env["PERF_STUB_FAILURES"] = json.dumps(failures)
    env["SLOW_MARKER"] = str(tmp_path / "slow-marker")
    return subprocess.run(
        [
            sys.executable,
            PERF_PY,
            "--host", "localhost", "localhost",
            "--port", str(REFERENCE_PORT), str(TESTED_PORT),
            "--binary", str(binary),
            "--jemalloc-purge", "disabled",
            *extra_args,
            str(tmp_path / "reference_timeout.xml"),
        ],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _rows(stdout, tag):
    """The fields after the tag of the lines of the raw output that start with `tag`."""
    return [line.split("\t")[1:] for line in stdout.splitlines() if line.split("\t")[0] == tag]


def _servers_of_query(stdout, query_index):
    """The servers the measured runs of a query were written for."""
    return {int(row[2]) for row in _rows(stdout, "query") if row[0] == str(query_index)}


def _job_warnings(tmp_path, stdout):
    """The warnings of the job for a test with this raw output."""
    workdir = tmp_path / "perf_wd"
    workdir.mkdir()
    (workdir / "reference_timeout-raw.tsv").write_text(stdout, encoding="utf-8")
    return reference_timeout_warnings(workdir)


def test_reference_timeout_in_a_measured_run_is_accepted(tmp_path):
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".run0")])

    assert result.returncode == 0, result.stderr
    # The job turns whatever a failed test wrote here into Run Errors rows.
    assert result.stderr == ""
    assert _rows(result.stdout, "reference-timeout") == [["1", "run", "15"]]
    assert _rows(result.stdout, "partial") == [["1", "[1]"]]
    assert _servers_of_query(result.stdout, 1) == {1}
    # At least the minimum of five runs on the tested server.
    assert len([row for row in _rows(result.stdout, "query") if row[0] == "1"]) >= 5
    assert [row[0] for row in _rows(result.stdout, "client-time")] == ["0", "1", "2"]
    # The other queries of the test are compared as usual.
    assert _servers_of_query(result.stdout, 0) == {0, 1}
    assert _servers_of_query(result.stdout, 2) == {0, 1}
    assert not _rows(result.stdout, "run-error")

    assert _job_warnings(tmp_path, result.stdout) == [
        "Query 1 of test 'reference_timeout' ran out of its time limit (15 s) on the reference server in a measured "
        "run: it is measured on the tested server only and is not compared"
    ]


def test_runs_the_reference_completed_before_its_timeout_are_dropped(tmp_path):
    # The servers take turns to go first, and the tested one goes first in the second run.
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".run1")])

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert _rows(result.stdout, "reference-timeout") == [["1", "run", "15"]]
    assert _rows(result.stdout, "partial") == [["1", "[1]"]]
    assert _servers_of_query(result.stdout, 1) == {1}
    run_ids = [row[1] for row in _rows(result.stdout, "query") if row[0] == "1"]
    assert run_ids[:2] == ["reference_timeout.query1.run0", "reference_timeout.query1.run1"]
    # The time of the reference is not in the time of the query: all its runs here take 0.01 s.
    client_time = next(row for row in _rows(result.stdout, "client-time") if row[0] == "1")
    assert float(client_time[2]) == pytest.approx(0.01 * len(run_ids))
    assert float(client_time[1]) < 1


def test_reference_timeout_in_the_prewarm_run_is_accepted(tmp_path):
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".prewarm0")])

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert _rows(result.stdout, "reference-timeout") == [["1", "prewarm", "180"]]
    assert _rows(result.stdout, "partial") == [["1", "[1]"]]
    assert _servers_of_query(result.stdout, 1) == {1}
    assert _servers_of_query(result.stdout, 2) == {0, 1}

    assert _job_warnings(tmp_path, result.stdout) == [
        "Query 1 of test 'reference_timeout' ran out of its time limit (180 s) on the reference server in the prewarm "
        "run: it is measured on the tested server only and is not compared"
    ]


def test_tested_timeout_in_a_measured_run_fails_the_test(tmp_path):
    result = _run_perf(tmp_path, [_failure(TESTED_PORT, ".run0")])

    assert result.returncode != 0
    assert "reference_timeout.query1.run0" in result.stderr
    assert f"Code: {TIMEOUT_EXCEEDED}" in result.stderr
    assert not _rows(result.stdout, "reference-timeout")
    assert not _rows(result.stdout, "partial")
    # The test stops at the failed query.
    assert not _servers_of_query(result.stdout, 2)
    assert _job_warnings(tmp_path, result.stdout) == []


def test_timeout_on_both_servers_fails_the_test(tmp_path):
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".run0"), _failure(TESTED_PORT, ".run0")])

    assert result.returncode != 0
    assert "reference_timeout.query1.run0" in result.stderr
    assert not _rows(result.stdout, "partial")
    assert not _servers_of_query(result.stdout, 2)


def test_tested_timeout_in_the_prewarm_run_fails_the_query(tmp_path):
    result = _run_perf(tmp_path, [_failure(TESTED_PORT, ".prewarm0")])

    assert result.returncode != 0
    assert [row[:2] for row in _rows(result.stdout, "run-error")] == [["1", "1"]]
    assert not _rows(result.stdout, "reference-timeout")
    assert not _servers_of_query(result.stdout, 1)


def test_other_reference_error_in_a_measured_run_fails_the_test(tmp_path):
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".run1", MEMORY_LIMIT_EXCEEDED)])

    assert result.returncode != 0
    assert "reference_timeout.query1.run1" in result.stderr
    assert f"Code: {MEMORY_LIMIT_EXCEEDED}" in result.stderr
    assert not _rows(result.stdout, "reference-timeout")
    assert not _rows(result.stdout, "partial")
    # The runs made before the error are still written, for both servers.
    assert _servers_of_query(result.stdout, 1) == {0, 1}
    assert not _servers_of_query(result.stdout, 2)


def test_reference_timeout_of_a_shell_query_fails_the_test(tmp_path):
    # The script is slow on the reference server from its second run on: the prewarm run passes.
    result = _run_perf(tmp_path, [], xml=SHELL_TEST_XML, extra_args=("--max-query-seconds", "1"))

    assert result.returncode != 0
    assert "reference_timeout.query0.run0" in result.stderr
    assert "timed out" in result.stderr
    assert not _rows(result.stdout, "reference-timeout")
    assert not _rows(result.stdout, "partial")
    assert not _servers_of_query(result.stdout, 1)


def test_reference_timeout_in_a_long_run_is_a_censored_time(tmp_path):
    result = _run_perf(tmp_path, [_failure(REFERENCE_PORT, ".run")], extra_args=("--long",))

    assert result.returncode == 0, result.stderr
    assert not _rows(result.stdout, "reference-timeout")
    assert not _rows(result.stdout, "partial")
    # Each run of the reference is written with the time limit as its time.
    reference_times = {row[3] for row in _rows(result.stdout, "query") if row[0] == "1" and row[2] == "0"}
    assert reference_times == {"15"}
    assert _servers_of_query(result.stdout, 1) == {0, 1}
