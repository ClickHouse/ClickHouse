"""
Tests that a timeout is only relabelled as infrastructure when the wait that expired was
docker's or the registry's, not the server's.

`_mark_infrastructure_errors` rewrites a matching result's status to SKIPPED, and a
SKIPPED result does not fail the job, so anything relabelled here stops being reported.

The discriminator is an argv, not a word. Checking for the word "docker" alone cannot
discriminate: `str(subprocess.TimeoutExpired)` for a `docker exec` contains it too. Nor
is "is this an orchestration command?" sufficient: `docker compose stop` waits for the
server to exit, so its timeout means the server ignored SIGTERM. Hence the subcommand.

The match is scoped to the raising `E   <ExcType>: <msg>` lines because an embedded
server stack trace can carry a timeout substring tens of kilobytes away from anything
that timed out.

Fixtures are generated through the real `ResultTranslator.from_pytest_jsonl`, and
`_translate` emits `longrepr.chain` for a `raise ... from ex`, because pytest gives the
inner exception its own `E ` line: a fixture without the chain cannot exhibit the shape
every `run_and_check` failure actually has. `test_translate_reproduces_real_pytest_output`
pins the rendering against a report-log produced by pytest itself.
"""

import json
import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.integration_test_job import (
    INFRASTRUCTURE_ERROR_PATTERNS,
    TIMEOUT_ERROR_PATTERNS,
    _is_docker_compose_timeout,
    _is_infrastructure_error,
    _is_orchestration_lifecycle_timeout,
    _mark_infrastructure_errors,
    _raising_exception_lines,
    _timed_out_orchestration_verbs,
)
from ci.praktika.result import Result, ResultTranslator

NON_TIMEOUT_PATTERNS = [
    p for p in INFRASTRUCTURE_ERROR_PATTERNS if p not in TIMEOUT_ERROR_PATTERNS
]


# --- fixture construction: real translator, never a hand-written info blob -----------


CAUSE_SEPARATOR = "The above exception was the direct cause of the following exception:"


def _reprtraceback(frames, exc_line):
    reprentries = []
    for frame in frames:
        path, lineno, src = frame[:3]
        # pytest puts `in <funcname>` here; the translator renders it into the frame
        # header. Optional because only the `E ` lines decide anything.
        func = frame[3] if len(frame) > 3 else ""
        reprentries.append(
            {
                "type": "ReprEntry",
                "data": {
                    "lines": [src] if src else [],
                    "reprfileloc": {
                        "path": path,
                        "lineno": lineno,
                        "message": f"in {func}" if func else "",
                    },
                },
            }
        )
    reprentries.append(
        {"type": "ReprEntry", "data": {"lines": [exc_line], "reprfileloc": None}}
    )
    return {"reprentries": reprentries}


def _translate(
    node_id,
    frames,
    exc_line,
    when="call",
    outcome="failed",
    cause_frames=None,
    cause_exc_line=None,
):
    """Render `frames` + `exc_line` into a Result the way the CI job sees it.

    `frames` is a list of (path, lineno, source_line). The serialization shape mirrors
    pytest's `longrepr`: reprtraceback.reprentries[].data.{reprfileloc,lines}.

    When a cause is given, `longrepr.chain` carries both exceptions oldest-first, as
    pytest serializes `raise ... from ex`, and `reprtraceback`/`reprcrash` mirror the
    last entry. `ResultTranslator._chain_of` only treats the chain as authoritative when
    it has more than one entry, so a single exception must not emit one.
    """
    reprtraceback = _reprtraceback(frames, exc_line)
    reprcrash = {
        "path": frames[-1][0] if frames else node_id,
        "lineno": frames[-1][1] if frames else 1,
        "message": exc_line,
    }
    longrepr = {"reprtraceback": reprtraceback, "reprcrash": reprcrash}
    if cause_exc_line is not None:
        cause_frames = cause_frames or frames
        longrepr["chain"] = [
            [
                _reprtraceback(cause_frames, cause_exc_line),
                {
                    "path": cause_frames[-1][0],
                    "lineno": cause_frames[-1][1],
                    "message": cause_exc_line,
                },
                CAUSE_SEPARATOR,
            ],
            [reprtraceback, reprcrash, None],
        ]
    entry = {
        "$report_type": "TestReport",
        "nodeid": node_id,
        "when": when,
        "outcome": outcome,
        "longrepr": longrepr,
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


# A `run_and_check` timeout as pytest itself serializes it: `raise Exception(...) from ex`
# with the argv space-joined in the outer message and repr'd in the inner one.
REAL_CHAIN_SOURCE = '''
import subprocess

ARGV = ["docker", "compose", "--env-file", "/w/.env", "--project-name", "roottestx-gw2",
        "stop"]


def run_and_check_like():
    try:
        raise subprocess.TimeoutExpired(cmd=ARGV, timeout=300)
    except subprocess.TimeoutExpired as ex:
        raise Exception(
            "Command [%s] timed out after 300s\\nstdout:\\n\\nstderr:\\n"
            % " ".join(ARGV)
        ) from ex


def test_teardown():
    run_and_check_like()
'''


def test_translate_reproduces_real_pytest_output():
    """`_translate` must render what pytest renders, or every arm below is measured
    against a shape production cannot produce.

    Asserted by running pytest on a chained failure and comparing the translated `info`
    byte for byte, so this cannot drift with the fixture helper.
    """
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "test_real_chain.py")
        with open(src, "w") as f:
            f.write(REAL_CHAIN_SOURCE)
        report = os.path.join(d, "rl.jsonl")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", src, "--tb=short", "-q", "-p", "no:cacheprovider",
             f"--report-log={report}"],
            cwd=d,
            capture_output=True,
        )
        assert os.path.isfile(report), (
            f"pytest produced no report-log (rc={proc.returncode}): "
            f"{proc.stdout.decode()[-2000:]}{proc.stderr.decode()[-2000:]}"
        )
        real = ResultTranslator.from_pytest_jsonl(report)

    candidates = real if isinstance(real, list) else [real]
    stack, leaves = list(candidates), []
    while stack:
        node = stack.pop()
        children = getattr(node, "results", None) or []
        stack.extend(children) if children else leaves.append(node)
    real_leaf = next(l for l in leaves if l.info)
    # the whole point: pytest gives the inner exception its own `E ` line
    assert len(_raising_exception_lines(real_leaf.info)) == 5

    # derived from the source, not hardcoded: a hardcoded lineno makes this arm fail for
    # a reason that has nothing to do with the rendering it exists to pin
    def lineno_of(needle):
        for i, line in enumerate(REAL_CHAIN_SOURCE.splitlines(), 1):
            if needle in line:
                return i
        raise AssertionError(f"{needle!r} not in the fixture source")

    mine = _translate(
        "test_real_chain.py::test_teardown",
        [
            (
                "test_real_chain.py",
                lineno_of("    run_and_check_like()"),
                "    run_and_check_like()",
                "test_teardown",
            ),
            (
                "test_real_chain.py",
                lineno_of("        raise Exception("),
                "    raise Exception(",
                "run_and_check_like",
            ),
        ],
        "E   Exception: Command [docker compose --env-file /w/.env --project-name "
        "roottestx-gw2 stop] timed out after 300s\nE   stdout:\nE   \nE   stderr:",
        cause_frames=[
            (
                "test_real_chain.py",
                lineno_of("        raise subprocess.TimeoutExpired("),
                "    raise subprocess.TimeoutExpired(cmd=ARGV, timeout=300)",
                "run_and_check_like",
            )
        ],
        cause_exc_line="E   subprocess.TimeoutExpired: Command '['docker', 'compose', "
        "'--env-file', '/w/.env', '--project-name', 'roottestx-gw2', 'stop']' "
        "timed out after 300 seconds",
    )
    assert mine.info == real_leaf.info


# The predicate as it stood before this change, inlined verbatim rather than read out of
# git: in PR CI the checkout already carries the change, so a control derived from the
# working tree or from HEAD compares the new code against itself and asserts nothing.
PREFIX_PREDICATE_SOURCE = """
INFRASTRUCTURE_ERROR_PATTERNS = [
    "timed out after",
    "TimeoutExpired",
    "Cannot connect to the Docker daemon",
    "Error response from daemon",
    "Name or service not known",
    "Temporary failure in name resolution",
    "Network is unreachable",
    "Connection reset by peer",
    "No space left on device",
    "Cannot allocate memory",
    "OCI runtime create failed",
    "toomanyrequests",
    "pull access denied",
    "Got exception pulling images:",
]


def _is_infrastructure_error(result):
    if not result.info:
        return False
    if result.status == Result.Status.ERROR:
        return any(pattern in result.info for pattern in INFRASTRUCTURE_ERROR_PATTERNS)
    if result.status == Result.Status.FAIL:
        has_docker_context = (
            "'docker'" in result.info or "images_pull_cmd" in result.info
        )
        return has_docker_context and any(
            p in result.info for p in INFRASTRUCTURE_ERROR_PATTERNS
        )
    return False
"""


def _prefix_predicate():
    """The pre-fix `_is_infrastructure_error`, from the inlined copy above."""
    ns = {"Result": Result}
    exec(PREFIX_PREDICATE_SOURCE, ns)
    return ns["_is_infrastructure_error"]


def _prefix_patterns():
    ns = {"Result": Result}
    exec(PREFIX_PREDICATE_SOURCE, ns)
    return ns["INFRASTRUCTURE_ERROR_PATTERNS"]


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


COMPOSE_UP_ARGV = [
    "docker",
    "compose",
    "--env-file",
    "/w/.env",
    "--project-name",
    "roottestx-gw2",
    "up",
    "-d",
    "--no-recreate",
]
COMPOSE_STOP_ARGV = [
    "docker",
    "compose",
    "--env-file",
    "/w/.env",
    "--project-name",
    "roottestx-gw2",
    "stop",
]


def _run_and_check_chain(node_id, frames, argv, seconds=300, cause_frames=None):
    """A `run_and_check` timeout as pytest renders it.

    `run_and_check` catches `subprocess.TimeoutExpired` and re-raises
    `Exception(f"Command [{' '.join(args)}] timed out after {timeout}s ...") from ex`, so
    both exceptions reach the report: the repr'd argv on the cause's `E ` line and the
    space-joined argv on the outer one.
    """
    return _translate(
        node_id,
        frames,
        f"E   Exception: Command [{' '.join(argv)}] timed out after {seconds}s"
        "\nE   stdout:\nE   \nE   stderr:",
        cause_frames=cause_frames
        or [("helpers/cluster.py", 222, "    raise Exception(")],
        cause_exc_line=f"E   subprocess.TimeoutExpired: Command '{argv!r}' "
        f"timed out after {seconds} seconds",
    )


def test_chained_compose_up_timeout_stays_infrastructure():
    """The negative control for the subcommand split, on the faithful two-exception shape
    a real `run_and_check` failure has. Without it the split could relabel every
    orchestration timeout as a failure and still look green."""
    r = _run_and_check_chain(
        "test_keeper_profiler/test.py::test_profiler",
        [
            ("test_keeper_profiler/test.py", 31, "    cluster.start()"),
            ("helpers/cluster.py", 4289, "    run_and_check(clickhouse_start_cmd)"),
        ],
        COMPOSE_UP_ARGV,
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r)
    assert _mark_infrastructure_errors([r]) == 1
    assert r.status == Result.Status.SKIPPED


SPACE_JOINED_STOP_E_LINE = (
    "E   Exception: Command [docker compose --env-file /w/.env --project-name "
    "roottestx-gw2 stop] timed out after 300s"
)


def test_space_joined_argv_rendering_is_recognised_as_a_compose_timeout():
    """`run_and_check` re-raises with the argv space-joined, so a predicate written only
    for the repr'd form would silently lose this whole class. Asserted on
    `_is_docker_compose_timeout` directly: recognising the rendering and deciding to
    relabel are separate questions, and only the first one is about the rendering."""
    r = _translate(
        "test_parallel_replicas_custom_key/test.py::test_custom_key",
        [
            ("test_parallel_replicas_custom_key/test.py", 24, "    cluster.shutdown()"),
            ("helpers/cluster.py", 4395, "    run_and_check(self.base_cmd + ['stop'])"),
        ],
        SPACE_JOINED_STOP_E_LINE,
    )
    assert _is_docker_compose_timeout(r.info), (
        "the space-joined rendering must be recognised as a compose command"
    )


def test_image_pull_timeout_in_the_space_joined_rendering_stays_infrastructure():
    """The whole relabelling path for a space-joined row, so the rendering support is
    load-bearing end to end and not only inside `_is_docker_compose_timeout`.
    `images_pull_cmd` is the docker context here, which is what a pull failure carries:
    the FAIL branch accepts either that token or a quoted `'docker'`, and
    `images_pull_cmd = base_cmd + ["pull"]` (cluster.py:3740)."""
    r = _translate(
        "test_keeper_profiler/test.py::test_profiler",
        [
            ("test_keeper_profiler/test.py", 31, "    cluster.start()"),
            ("helpers/cluster.py", 3750, "    run_and_check(images_pull_cmd, ...)"),
        ],
        "E   Exception: Command [docker compose --project-name roottestx-gw2 pull] "
        "timed out after 180s while running images_pull_cmd",
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r), (
        "a pull timeout with docker context must still be infrastructure in the "
        "space-joined rendering"
    )


def test_lifecycle_timeout_without_docker_context_stays_a_failure_on_the_fail_branch():
    """On the FAIL branch the docker-context test is a conjunct, not a disjunct.

    An uncaught `run_and_check` timeout renders only the space-joined argv, so a row can
    name a lifecycle subcommand while carrying neither a quoted `'docker'` nor an
    `images_pull_cmd`. Relabelling on the subcommand alone would accept every such row
    and reintroduce the widening the conjunct exists to prevent.

    Text taken from a real row (`test_ddl_config_hostname::test_ddl_queue_delete_add_replica`,
    recorded FAIL); 35 corpus row/status pairs have this shape.
    """
    r = _translate(
        "test_ddl_config_hostname/test.py::test_ddl_queue_delete_add_replica",
        [
            ("test_ddl_config_hostname/test.py", 18, "    cluster.start()", "started_cluster"),
            ("helpers/cluster.py", 3999, "    run_and_check(minio_start_cmd)", "start"),
            ("helpers/cluster.py", 194, "    raise Exception(", "run_and_check"),
        ],
        "E   Exception: Command [docker compose --project-name "
        "roottestddlconfighostname-gw2 --env-file /w/test_ddl_config_hostname/"
        "_instances-gw2/.env --file /w/compose/docker_compose_minio.yml --verbose up -d] "
        "timed out after 300s\nE   stdout:\nE   \nE   stderr:",
    )
    # the fixture's own precondition: neither docker-context token is present, so the
    # conjunct is the only thing that can reject this row
    assert "'docker'" not in r.info
    assert "images_pull_cmd" not in r.info
    # the row IS orchestration, so the subcommand split is not what rejects it
    assert _is_docker_compose_timeout(r.info)
    assert _is_orchestration_lifecycle_timeout(r.info)
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)
    assert _mark_infrastructure_errors([r]) == 0
    assert r.status == Result.Status.FAIL


def test_product_stop_hang_on_ip_change_is_a_failure():
    """A server that will not exit on SIGTERM makes a `stop` block past the python-side
    budget, and that is a product defect, not infrastructure.

    `restart_instance_with_ip_change` runs `run_and_check(self.base_cmd + ["stop", name])`
    with no `--timeout` and no enclosing `try` (cluster.py:2641), so the wait is bounded
    only by the generated compose template's `stop_grace_period: 10m` (cluster.py:4880),
    which outlives `run_and_check`'s 300 s default, and the timeout propagates to the
    classifier. Reachable from 9 call sites across 3 modules.

    `shutdown()`'s own `stop` (cluster.py:4395) and `down --volumes` (:4443) sit inside
    `try`s, so neither reaches the classifier on its own. The `stop`'s handler re-raises
    through the `kill` at :4401, so a teardown surfaces as the `kill` arm below, carrying
    the `stop` with it as an implicitly chained cause.

    Asserted on the faithful two-exception shape: the cause's `E ` line carries the
    repr'd argv, which supplies the quoted `'docker'` the FAIL branch looks for, so a
    docker-context test cannot separate this from a `compose up`. The subcommand is what
    separates them.
    """
    r = _run_and_check_chain(
        "test_dns_cache/test.py::test_user_access_ip_change",
        [
            ("test_dns_cache/test.py", 381, "    cluster.restart_instance_with_ip_change(node4, node4_ipv6)"),
            ("helpers/cluster.py", 2641, "    run_and_check(self.base_cmd + ['stop', node.name])"),
        ],
        COMPOSE_STOP_ARGV,
    )
    r.status = Result.Status.FAIL
    # the shape's own precondition: the docker gate is satisfied, so it decides nothing
    assert "'docker'" in r.info
    assert not _is_infrastructure_error(r), (
        "a container that will not stop is the product hanging, so the result must "
        "stay a failure"
    )
    assert _mark_infrastructure_errors([r]) == 0
    assert r.status == Result.Status.FAIL


def test_product_kill_hang_is_a_failure():
    """`shutdown()` falls back to `run_and_check(base_cmd + ["kill"])` when `stop` fails
    (cluster.py:4401), and that call is unguarded, so it is the one teardown command whose
    timeout reaches the classifier. `kill` waits on the container dying too. One real
    corpus row carries this shape, with `cluster.py:4284 ... ["kill"]` as its frame."""
    r = _run_and_check_chain(
        "test_parallel_replicas_custom_key/test.py::test_custom_key",
        [
            ("test_parallel_replicas_custom_key/test.py", 24, "    cluster.shutdown()"),
            ("helpers/cluster.py", 4401, "    run_and_check(self.base_cmd + ['kill'])"),
        ],
        ["docker", "compose", "--project-name", "roottestx-gw2", "kill"],
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)


IMPLICIT_CHAIN_SOURCE = '''
import subprocess

STOP = ["docker", "compose", "--project-name", "roottestx-gw2", "stop"]
KILL = ["docker", "compose", "--project-name", "roottestx-gw2", "kill"]


def run_and_check_like(argv):
    try:
        raise subprocess.TimeoutExpired(cmd=argv, timeout=300)
    except subprocess.TimeoutExpired as ex:
        raise Exception(
            "Command [%s] timed out after 300s\\nstdout:\\n\\nstderr:\\n" % " ".join(argv)
        ) from ex


def shutdown_like():
    try:
        run_and_check_like(STOP)
    except Exception:
        run_and_check_like(KILL)


def test_teardown():
    shutdown_like()
'''


def test_teardown_reporting_both_stop_and_kill_is_a_failure():
    """The shape a real teardown produces, generated by pytest rather than assembled.

    `shutdown()` calls `kill` from inside the handler of the `stop`'s `try`
    (cluster.py:4395/4401), so python attaches the `stop` to the `kill` as a context and
    pytest renders four exceptions, each with its own `E ` line. Both subcommands are
    product-sensitive, so the row must stay a failure however many of them it names.
    """
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "test_implicit_chain.py")
        with open(src, "w") as f:
            f.write(IMPLICIT_CHAIN_SOURCE)
        report = os.path.join(d, "rl.jsonl")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", src, "--tb=short", "-q",
             "-p", "no:cacheprovider", f"--report-log={report}"],
            cwd=d,
            capture_output=True,
        )
        assert os.path.isfile(report), (
            f"pytest produced no report-log (rc={proc.returncode}): "
            f"{proc.stdout.decode()[-2000:]}"
        )
        real = ResultTranslator.from_pytest_jsonl(report)

    candidates = real if isinstance(real, list) else [real]
    stack, leaves = list(candidates), []
    while stack:
        node = stack.pop()
        children = getattr(node, "results", None) or []
        stack.extend(children) if children else leaves.append(node)
    r = next(l for l in leaves if l.info)

    # the fixture's own precondition: both subcommands really are on raising lines, so
    # the arm measures the decision and not a rendering that dropped one of them
    assert _timed_out_orchestration_verbs(r.info) == {"stop", "kill"}
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)
    assert _mark_infrastructure_errors([r]) == 0
    assert r.status == Result.Status.FAIL


def test_mixed_lifecycle_and_product_verbs_is_a_failure():
    """A product-sensitive subcommand anywhere in the row means the server is what did not
    respond, so the presence of a lifecycle verb must not outvote it.

    A single result can name several subcommands: `shutdown()` calls `kill` from the
    handler of the `stop`'s `try` (cluster.py:4395/4401), so python chains the two and
    pytest gives each its own `E ` line. That real pair is product+product; this fixture
    mixes a lifecycle verb in instead, which is the direction that could go wrong.
    """
    r = _translate(
        "test_backup_restore_on_cluster/test_huge_concurrent_restore.py::test_huge",
        [
            ("test_backup_restore_on_cluster/test_huge_concurrent_restore.py", 71, "    cluster.shutdown()"),
            ("helpers/cluster.py", 4443, "    subprocess_check_call(self.base_cmd + ['down', '--volumes'])"),
        ],
        "E   Exception: Command [docker compose --project-name roottestx-gw2 down "
        "--volumes] timed out after 300s",
        cause_frames=[("helpers/cluster.py", 4395, "    run_and_check(self.base_cmd + ['stop'])")],
        cause_exc_line="E   subprocess.TimeoutExpired: Command '['docker', 'compose', "
        "'--project-name', 'roottestx-gw2', 'stop']' timed out after 300 seconds",
    )
    r.status = Result.Status.FAIL
    assert _is_docker_compose_timeout(r.info), "both commands are orchestration"
    assert not _is_infrastructure_error(r), (
        "a `stop` that timed out is the server hanging, whatever else the row reports"
    )


def test_unclassified_subcommand_is_not_relabelled():
    """A compose subcommand nobody has classified must not be assumed harmless: the
    default has to be reporting the failure, not suppressing it."""
    r = _run_and_check_chain(
        "test_x/test.py::test_y",
        [("test_x/test.py", 10, "    cluster.do_something()")],
        ["docker", "compose", "--project-name", "roottestx-gw2", "wait"],
    )
    r.status = Result.Status.FAIL
    assert _is_docker_compose_timeout(r.info), "it is still an orchestration command"
    assert not _is_infrastructure_error(r)


def test_docker_login_timeout_stays_infrastructure():
    """`login_to_ecr` runs `run_and_check(["docker", "login", ...])` with no timeout
    (cluster.py:3659-3682) from `start()`, which rethrows, so a registry stall surfaces
    as a per-test result. A registry not answering is what this mechanism exists for:
    `toomanyrequests` and `pull access denied` are its neighbours in the pattern list."""
    r = _run_and_check_chain(
        "test_keeper_profiler/test.py::test_profiler",
        [
            ("test_keeper_profiler/test.py", 31, "    cluster.start()"),
            ("helpers/cluster.py", 3679, "    run_and_check(["),
        ],
        [
            "docker",
            "login",
            "123.dkr.ecr.us-east-1.amazonaws.com",
            "-u",
            "AWS",
            "--password-stdin",
        ],
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r)
    r.status = Result.Status.ERROR
    assert _is_infrastructure_error(r)


def test_compose_start_timeout_stays_infrastructure():
    """`process_integration_nodes` passes the subcommand in as a variable
    (cluster.py:4835), so `start`/`kill` reach compose from the same call site;
    `start_zookeeper_nodes` uses `start`, which only waits for docker."""
    r = _run_and_check_chain(
        "test_keeper_multinode_simple/test.py::test_simple_replicated_table",
        [
            ("test_keeper_multinode_simple/test.py", 90, "    cluster.start_zookeeper_nodes(nodes)"),
            ("helpers/cluster.py", 4835, "    subprocess_check_call(base_cmd + [action] + list(nodes))"),
        ],
        ["docker", "compose", "--project-name", "roottestx-gw2", "start", "zoo1", "zoo2"],
    )
    r.status = Result.Status.FAIL
    assert _is_infrastructure_error(r)


def test_project_name_that_looks_like_a_subcommand_is_not_the_subcommand():
    """`--project-name` consumes the token after it, so the subcommand is the first token
    no option has claimed. A project literally named `up` must not make a `stop` timeout
    look like a lifecycle one."""
    r = _run_and_check_chain(
        "test_x/test.py::test_y",
        [("test_x/test.py", 10, "    cluster.shutdown()")],
        ["docker", "compose", "--project-name", "up", "stop"],
    )
    r.status = Result.Status.FAIL
    assert not _is_infrastructure_error(r)


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


def test_compose_timeout_only_in_embedded_traceback_is_not_infrastructure():
    """Pins the `E `-line scoping itself.

    Shape taken from `test_huge_concurrent_restore`, where a compose-argv
    `TimeoutExpired` from an earlier teardown attempt sits ~7 kB away from the raising
    exception, rendered as `|`-prefixed continuation lines inside one entry rather than
    as a chain. Those real rows embed `stop`/`kill`, which the subcommand split rejects
    anyway; this one embeds `up` so that only the scoping can reject it.
    """
    embedded = "\n".join(
        [
            "    | Traceback (most recent call last):",
            "    |   File helpers/cluster.py:4289, in start",
            "    | subprocess.TimeoutExpired: Command '['docker', 'compose', "
            "'--project-name', 'roottestx-gw2', 'up', '-d']' timed out after 300 seconds",
            "    | During handling of the above exception, another exception occurred:",
        ]
    )
    r = _translate(
        "test_backup_restore_on_cluster/test_huge_concurrent_restore.py::test_huge",
        [
            (
                "test_backup_restore_on_cluster/test_huge_concurrent_restore.py",
                71,
                "    node0.query('BACKUP TABLE tbl ...')",
            ),
            ("helpers/client.py", 269, f"    raise QueryRuntimeException\n{embedded}"),
        ],
        "E   helpers.client.QueryRuntimeException: Client failed! Return code: 32, "
        "stderr: Code: 32. DB::Exception: Attempt to read after eof "
        "(ATTEMPT_TO_READ_AFTER_EOF)",
    )
    r.status = Result.Status.FAIL
    # the embedded text is present and would match if the scoping were dropped
    assert "docker', 'compose'" in r.info and "timed out after" in r.info
    assert not _is_docker_compose_timeout(r.info)
    assert not _is_orchestration_lifecycle_timeout(r.info)
    assert not _is_infrastructure_error(r), (
        "a compose timeout from an earlier attempt, embedded in the traceback of an "
        "unrelated product exception, is not what failed here"
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


def test_prefix_pattern_list_is_the_one_the_current_code_still_carries():
    """The inlined pre-fix copy is only a control while it agrees with the real thing on
    the part this change does not touch. The pattern list is one of those parts, so a
    divergence here means the copy has drifted and every verdict below is measured
    against a predicate that never shipped."""
    assert _prefix_patterns() == INFRASTRUCTURE_ERROR_PATTERNS


def test_prefix_predicate_is_a_plain_substring_search_over_every_pattern():
    """The property that makes the pre-fix predicate the wrong one: each pattern matched
    anywhere in the info, with no notion of where the text came from. Asserted per
    pattern so the copy cannot silently lose one."""
    prefix = _prefix_predicate()
    for pattern in _prefix_patterns():
        r = _translate(
            "test_x/test.py::test_y",
            [("test_x/test.py", 10, "    do_something()")],
            f"E   RuntimeError: {pattern}",
        )
        r.status = Result.Status.ERROR
        assert prefix(r) is True, f"pre-fix must match {pattern!r} unconditionally"


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
