"""
Tests that a timeout is only relabelled as infrastructure when it is a docker compose
lifecycle command timing out.

`_mark_infrastructure_errors` rewrites a matching result's status to SKIPPED, and a
SKIPPED result does not fail the job. Two of `INFRASTRUCTURE_ERROR_PATTERNS` are the bare
substrings "timed out after" and "TimeoutExpired", which a test-body timeout renders just
as an orchestration timeout does, so a genuine failure could be reported as SKIPPED.
Measured over 120 days of `Integration tests%` results whose context carried
`subprocess.TimeoutExpired`: 308 FAIL, 12 OK, 6 SKIPPED, and two of those six were
test-body timeouts (`test_tcp_handler_connection_limits`, its own `timeout=15`).

The discriminator is an argv, not a word: `ClickHouseCluster.base_cmd` is
`["docker", "compose", ...]` and `compose_cmd()` returns
`["docker", "compose", "--project-name", ...]`, so every orchestration command including
image pulls (`images_pull_cmd = base_cmd + ["pull"]`) begins with those two entries, and
nothing a test body runs does. Checking for the word "docker" alone cannot discriminate:
`str(subprocess.TimeoutExpired)` for a `docker exec` contains it too.

The match is scoped to the raising `E   <ExcType>: <msg>` lines because an embedded
server stack trace can carry a timeout substring tens of kilobytes away from anything
that timed out.

Fixtures are generated through the real `ResultTranslator.from_pytest_jsonl` rather than
hand-written, because `raise ... from ex` serializes as a two-element chain and the
translator renders both exceptions; text derived by reading the raise site is unfaithful.
"""

import json
import os
import re
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.integration_test_job import (
    INFRASTRUCTURE_ERROR_PATTERNS,
    TIMEOUT_ERROR_PATTERNS,
    _is_infrastructure_error,
    _mark_infrastructure_errors,
)
from ci.praktika.result import Result, ResultTranslator

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

NON_TIMEOUT_PATTERNS = [
    p for p in INFRASTRUCTURE_ERROR_PATTERNS if p not in TIMEOUT_ERROR_PATTERNS
]


# --- fixture construction: real translator, never a hand-written info blob -----------


def _translate(node_id, frames, exc_line, when="call", outcome="failed"):
    """Render `frames` + `exc_line` into a Result the way the CI job sees it.

    `frames` is a list of (path, lineno, source_line). The serialization shape mirrors
    pytest's `longrepr`: reprtraceback.reprentries[].data.{reprfileloc,lines}.
    """
    reprentries = []
    for path, lineno, src in frames:
        reprentries.append(
            {
                "type": "ReprEntry",
                "data": {
                    "lines": [src] if src else [],
                    "reprfileloc": {
                        "path": path,
                        "lineno": lineno,
                        "message": "",
                    },
                },
            }
        )
    reprentries.append(
        {"type": "ReprEntry", "data": {"lines": [exc_line], "reprfileloc": None}}
    )
    entry = {
        "$report_type": "TestReport",
        "nodeid": node_id,
        "when": when,
        "outcome": outcome,
        "longrepr": {
            "reprtraceback": {"reprentries": reprentries},
            "reprcrash": {
                "path": frames[-1][0] if frames else node_id,
                "lineno": frames[-1][1] if frames else 1,
                "message": exc_line,
            },
        },
    }
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write(json.dumps(entry) + "\n")
        f.write(json.dumps({"$report_type": "SessionFinish", "exitstatus": 1}) + "\n")
        path = f.name
    try:
        results = ResultTranslator.from_pytest_jsonl(path)
    finally:
        os.unlink(path)
    # The translator returns the pytest-session root, whose own `info` is empty; the
    # per-test leaf carrying the rendered traceback is nested under `.results`. Returning
    # the root would give every assertion an empty `info`, which the predicate
    # short-circuits to False, so every arm would agree for the wrong reason.
    candidates = results if isinstance(results, list) else [results]
    leaves = []
    stack = list(candidates)
    while stack:
        node = stack.pop()
        children = getattr(node, "results", None) or []
        if children:
            stack.extend(children)
        else:
            leaves.append(node)
    assert leaves, "translator produced no leaf results"
    leaf = next((l for l in leaves if l.name == node_id), leaves[0])
    assert leaf.info, f"fixture produced an empty info for {node_id}"
    return leaf


def _prefix_predicate():
    """The pre-fix `_is_infrastructure_error`, exec'd out of git HEAD.

    Read with `git show`, never `git stash`: `refs/stash` is shared across worktrees.
    """
    res = subprocess.run(
        ["git", "show", "HEAD:ci/jobs/integration_test_job.py"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
        check=False,
    )
    if res.returncode != 0:
        pytest.skip("git show failed")
    src = res.stdout.decode("utf-8")
    pat = re.search(r"^INFRASTRUCTURE_ERROR_PATTERNS = \[.*?^\]", src, re.S | re.M)
    fn = re.search(
        r"^def _is_infrastructure_error.*?(?=^\ndef |\Z)", src, re.S | re.M
    )
    if not pat or not fn:
        pytest.skip("pre-fix predicate not found in HEAD")
    ns = {"Result": Result}
    exec(pat.group(0) + "\n\n" + fn.group(0), ns)
    return ns["_is_infrastructure_error"]


COMPOSE_ARGV_REPR = (
    "'docker', 'compose', '--env-file', '/w/tests/integration/test_x/_instances-gw2/.env', "
    "'--project-name', 'roottestx-gw2', '--file', '/w/docker-compose.yml', 'up', '-d'"
)


# --- (a) a test-body timeout is a failure, not infrastructure -------------------------


@pytest.mark.parametrize("node", ["test_query_count_limit", "test_time_limit"])
def test_body_timeout_is_a_failure(node):
    r = _translate(
        f"test_tcp_handler_connection_limits/test.py::{node}",
        [
            ("test_tcp_handler_connection_limits/test.py", 50, f"    {node}()"),
            (
                "test_tcp_handler_connection_limits/test.py",
                33,
                "    stdout, stderr = proc.communicate(query_string, timeout=15)",
            ),
        ],
        "E   subprocess.TimeoutExpired: Command '['docker', 'exec', '-i', "
        "'roottesttcphandlerconnectionlimits-gw2-node-1', 'clickhouse', 'client']' "
        "timed out after 15 seconds",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r), (
        "a timeout raised by the test's own command must stay a failure"
    )
    r.status = Result.Status.ERROR
    assert not _is_infrastructure_error(r), "same must hold on the ERROR branch"


# --- (b)/(b2) negative controls: real orchestration timeouts stay infrastructure ------


def test_compose_up_timeout_stays_infrastructure():
    r = _translate(
        "test_keeper_profiler/test.py::test_profiler",
        [
            ("test_keeper_profiler/test.py", 31, "    cluster.start()"),
            ("helpers/cluster.py", 3883, "    run_and_check(clickhouse_start_cmd)"),
        ],
        f"E   subprocess.TimeoutExpired: Command '[{COMPOSE_ARGV_REPR}]' "
        "timed out after 300 seconds",
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r), (
        "a compose `up -d` timeout is why this mechanism exists"
    )


def test_compose_teardown_timeout_wrapper_rendering_stays_infrastructure():
    """`run_and_check` re-raises with the argv space-joined, so a predicate written only
    for the repr'd form would silently lose this whole class."""
    r = _translate(
        "test_parallel_replicas_custom_key/test.py::test_custom_key",
        [
            ("test_parallel_replicas_custom_key/test.py", 24, "    cluster.shutdown()"),
            ("helpers/cluster.py", 4284, "    run_and_check(self.base_cmd + ['stop'])"),
        ],
        "E   Exception: Command [docker compose --env-file /w/.env --project-name "
        "roottestx-gw2 stop] timed out after 300s",
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r), "the space-joined rendering must also match"


# --- (c) an incidental substring inside an embedded stack trace is not a timeout ------


def test_timeout_token_only_in_embedded_stack_is_not_infrastructure():
    """The raising exception is ATTEMPT_TO_READ_AFTER_EOF; the timeout substrings live
    thousands of characters away inside a captured server stack trace."""
    noise = "\n".join(
        f"    | {i}. src/Client/ClientBase.cpp:{i}: DB::ClientBase::run() timed out after"
        for i in range(40)
    )
    r = _translate(
        "test_backup_restore_on_cluster/test_huge_concurrent_restore.py::test_huge",
        [
            (
                "test_backup_restore_on_cluster/test_huge_concurrent_restore.py",
                71,
                "    node0.query('BACKUP TABLE tbl ...')",
            ),
            ("helpers/client.py", 269, f"    raise QueryRuntimeException\n{noise}"),
        ],
        "E   helpers.client.QueryRuntimeException: Client failed! Return code: 32, "
        "stderr: Code: 32. DB::Exception: Attempt to read after eof "
        "(ATTEMPT_TO_READ_AFTER_EOF)",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r), (
        "a timeout substring inside an embedded stack trace is not a timeout"
    )


def test_client_timeout_on_raising_line_without_compose_is_not_infrastructure():
    """Excluded by the compose half rather than the E-line half, so asserted separately."""
    r = _translate(
        "test_backup_restore_on_cluster/test_huge_concurrent_restore.py::test_huge",
        [
            (
                "test_backup_restore_on_cluster/test_huge_concurrent_restore.py",
                71,
                "    node0.query('INSERT INTO tbl VALUES (19)')",
            ),
            ("helpers/client.py", 269, "    raise QueryTimeoutExceedException"),
        ],
        "E   helpers.client.QueryTimeoutExceedException: Client timed out!",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)


# --- (c2) a client query timeout raised from a fixture is still a failure -------------


def test_client_query_timeout_from_a_fixture_is_a_failure():
    """Stands for 308 measured rows. A fixture that runs a query is not orchestration,
    so keying on "did this come from a fixture?" would be wrong."""
    r = _translate(
        "test_distributed_index_analysis/test.py::test_primary_key",
        [
            ("test_distributed_index_analysis/test.py", 75, "    bootstrap()"),
            ("helpers/client.py", 241, "    wait_and_read_output()"),
        ],
        "E   subprocess.TimeoutExpired: Command '['/w/ci/tmp/clickhouse', 'client', "
        "'--host', '172.16.2.5', '--port', '9000']' timed out after 120 seconds",
        when="setup",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)


# --- (d) a path with no historical row: a test body calling run_and_check directly ----


def test_direct_run_and_check_from_a_test_body_is_a_failure():
    """No CIDB row exists for this path, which is exactly why it is pinned: a predicate
    validated only on rows that already happened is over-fitted by construction.
    `test_keeper_java_client` runs the client under test through `run_and_check`, so a
    product hang there must not be relabelled."""
    r = _translate(
        "test_keeper_java_client/test.py::test_java_client",
        [
            ("test_keeper_java_client/test.py", 60, "    run_java_test()"),
            ("helpers/cluster.py", 175, "    res = subprocess.run(args, ...)"),
        ],
        "E   subprocess.TimeoutExpired: Command '['docker exec c bash -lc \"java -jar "
        "/tmp/keeper-java-client-test.jar\"']' timed out after 300 seconds",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r), (
        "a hang of the process under test must stay a failure even though the command "
        "mentions docker"
    )


# --- (e) the non-timeout patterns keep today's per-branch behaviour -------------------


@pytest.mark.parametrize("pattern", NON_TIMEOUT_PATTERNS)
def test_non_timeout_patterns_unconditional_on_error_branch(pattern):
    r = _translate(
        "test_x/test.py::test_y",
        [("test_x/test.py", 10, "    do_something()")],
        f"E   RuntimeError: {pattern}",
    )
    r.status = Result.Status.ERROR
    assert _is_infrastructure_error(r), f"{pattern!r} must still match on ERROR"


@pytest.mark.parametrize("pattern", NON_TIMEOUT_PATTERNS)
def test_non_timeout_patterns_still_docker_gated_on_fail_branch(pattern):
    r = _translate(
        "test_x/test.py::test_y",
        [("helpers/cluster.py", 10, "    run_and_check(['docker', 'ps'])")],
        f"E   RuntimeError: {pattern} while running '['docker', 'ps']'",
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r), (
        f"{pattern!r} with docker context must still match on FAIL"
    )


# --- (e2) the FAIL-branch docker gate survives ----------------------------------------


def test_product_failure_asserting_on_a_generic_string_is_not_infrastructure():
    """`test_accept_invalid_certificate` asserts on the literal "Connection reset by
    peer", which is also an infrastructure pattern. The FAIL branch's docker gate is what
    keeps that genuine failure a failure, so it must not be relaxed."""
    r = _translate(
        "test_accept_invalid_certificate/test.py::test_strict_reject_with_config",
        [
            (
                "test_accept_invalid_certificate/test.py",
                124,
                "    assert 'Connection reset by peer' in str(err)",
            )
        ],
        "E   AssertionError: assert 'Connection reset by peer' in 'some other error'",
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r), (
        "a product failure whose assertion text contains an infrastructure pattern, "
        "with no docker context, must stay a failure"
    )


# --- (f) mutation / vacuity: the demonstrating arms must differ from the pre-fix ------


def test_fix_is_not_vacuous_body_timeout_verdict_changed():
    """The bug itself. If the pre-fix predicate already agreed here, the change would be
    a vacuous mutation and this whole file would assert nothing."""
    prefix = _prefix_predicate()
    r = _translate(
        "test_tcp_handler_connection_limits/test.py::test_query_count_limit",
        [
            ("test_tcp_handler_connection_limits/test.py", 50, "    q()"),
            ("test_tcp_handler_connection_limits/test.py", 33, "    proc.communicate()"),
        ],
        "E   subprocess.TimeoutExpired: Command '['docker', 'exec', '-i', 'node-1', "
        "'clickhouse', 'client']' timed out after 15 seconds",
    )
    r.status = Result.Status.FAIL
    assert prefix(r) is True, "pre-fix must have relabelled this (that is the bug)"
    assert _is_infrastructure_error(r) is False, "the fix must stop relabelling it"


def test_negative_control_orchestration_verdict_unchanged():
    """A no-regression arm: correctly identical on both trees."""
    prefix = _prefix_predicate()
    r = _translate(
        "test_keeper_profiler/test.py::test_profiler",
        [("helpers/cluster.py", 3883, "    run_and_check(cmd)")],
        f"E   subprocess.TimeoutExpired: Command '[{COMPOSE_ARGV_REPR}]' "
        "timed out after 300 seconds",
    )
    r.status = Result.Status.FAIL
    assert prefix(r) is True
    assert _is_infrastructure_error(r) is True


def test_over_widening_the_non_timeout_patterns_would_be_caught():
    """Pins that the (e2) arm discriminates: a variant making the 12 non-timeout patterns
    unconditional on FAIL relabels a real product failure."""
    r = _translate(
        "test_accept_invalid_certificate/test.py::test_strict_reject_with_config",
        [("test_accept_invalid_certificate/test.py", 124, "    assert ...")],
        "E   AssertionError: assert 'Connection reset by peer' in 'other'",
    )
    r.status = Result.Status.FAIL
    over_widened = any(p in r.info for p in INFRASTRUCTURE_ERROR_PATTERNS)
    assert over_widened is True, "the over-widened variant would match here"
    assert _is_infrastructure_error(r) is False, "the shipped predicate must not"


# --- status rewriting: a now-failing result must redden the job -----------------------


def test_marking_does_not_skip_a_body_timeout():
    r = _translate(
        "test_tcp_handler_connection_limits/test.py::test_query_count_limit",
        [("test_tcp_handler_connection_limits/test.py", 50, "    q()")],
        "E   subprocess.TimeoutExpired: Command '['docker', 'exec', 'node', "
        "'clickhouse', 'client']' timed out after 15 seconds",
    )
    r.status = Result.Status.FAIL
    assert _mark_infrastructure_errors([r]) == 0
    assert r.status == Result.Status.FAIL, "a real failure must not become SKIPPED"


def test_marking_still_skips_a_compose_timeout():
    r = _translate(
        "test_keeper_profiler/test.py::test_profiler",
        [("helpers/cluster.py", 3883, "    run_and_check(cmd)")],
        f"E   subprocess.TimeoutExpired: Command '[{COMPOSE_ARGV_REPR}]' "
        "timed out after 300 seconds",
    )
    r.status = Result.Status.FAIL
    assert _mark_infrastructure_errors([r]) == 1
    assert r.status == Result.Status.SKIPPED
