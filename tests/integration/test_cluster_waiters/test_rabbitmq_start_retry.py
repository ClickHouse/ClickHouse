"""Pins that ClickHouseCluster.wait_rabbitmq_to_start recreates a hung RabbitMQ
container and retries, collecting one set of diagnostics per attempt.

The RabbitMQ container occasionally hangs on startup in CI (the entrypoint
produces no output and the Erlang node never registers with epmd), while a
fresh container on the same host starts in seconds. The waiter must therefore
recover by recreating the container instead of failing the whole test module.

The helper under test is loaded out of helpers/cluster.py by AST extraction and
executed against stubs, so these assertions track the shipped source rather than
a copy of it. No Docker, no broker and no ClickHouseCluster instance is needed.
"""

import ast
import os
import types

HELPERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "helpers")
CLUSTER_PY = os.path.normpath(os.path.join(HELPERS_DIR, "cluster.py"))
JOB_PY = os.path.normpath(
    os.path.join(HELPERS_DIR, "..", "..", "..", "ci", "jobs", "integration_test_job.py")
)
FUNC_NAME = "wait_rabbitmq_to_start"

# Stand-ins for the values the real cluster supplies, so the snapshot path the arms
# assert on is the one the waiter builds. TEMP_DIR is cluster.py's own value, which is
# relative to the test directory - the arms below pin that the waiter resolves it, since
# the CI job script that reads the path back runs from the repo root.
TEMP_DIR = "../../ci/tmp"
TEST_CWD = "/repo/tests/integration"
EXPECTED_SNAPSHOT_DIR = "/repo/ci/tmp"
PROJECT_NAME = "stubproject-gw3"
PID = 4242
TOKEN = "RABBITMQ_RECREATE"

# The clock is faked, so every arm is instant and the retry counts below are
# exact. This is the helper's own default deadline.
TIMEOUT = 120.0


class _FakeClock:
    """Advances only when the code under test sleeps, so arms are exact and instant.

    A loop that neither sleeps nor exits would therefore never reach its deadline,
    so reads are capped: the cap fails such a loop immediately instead of leaving
    it to pytest's 900s timeout. The legitimate arms below read the clock at most
    ~250 times (two failing readiness rounds of 122 reads each).
    """

    _MAX_READS = 100000

    def __init__(self):
        self.now = 0.0
        self.reads = 0

    def time(self):
        self.reads += 1
        if self.reads > self._MAX_READS:
            raise AssertionError(
                f"clock read over {self._MAX_READS} times without advancing: "
                "the waiter is spinning without sleeping or exiting"
            )
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _waiter_ast():
    with open(CLUSTER_PY, encoding="utf-8") as f:
        source = f.read()
    module = ast.parse(source)
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == FUNC_NAME:
            return node
    raise AssertionError(f"{FUNC_NAME} not found in {CLUSTER_PY}")


def _module_constant(name):
    """Read a module-level string constant out of a shipped source file by AST, so the
    assertions track what is shipped rather than a copy of it."""
    for path in (CLUSTER_PY, JOB_PY):
        with open(path, encoding="utf-8") as f:
            module = ast.parse(f.read())
        for node in module.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets
            ):
                yield path, ast.literal_eval(node.value)
                break
        else:
            raise AssertionError(f"{name} not found in {path}")


def _run(
    ready_from_attempt,
    collection_ok=True,
    debuginfo_ok=True,
    snapshot_ok=True,
    retries=None,
    project_name=PROJECT_NAME,
    pid=PID,
    cluster=None,
):
    """Execute the shipped waiter against stubs and record what it did.

    ready_from_attempt: 0-based attempt index from which the broker probe starts
    succeeding; None means it never succeeds.
    """
    counts = {"logs": 0, "debuginfo": 0, "recreations": 0}
    run_and_check_calls = []
    warnings = []
    copied = []
    # One ordered list of both kinds of side effect, so their relative order is
    # observable: recording them in two lists made it unassertable.
    events = []

    class _NullFile:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def check_call(*args, **kwargs):
        counts["logs"] += 1
        if not collection_ok:
            raise RuntimeError("stub: docker compose logs failed")

    def rabbitmq_debuginfo(docker_id, cookie):
        counts["debuginfo"] += 1
        if not debuginfo_ok:
            raise RuntimeError("stub: rabbitmq_debuginfo failed")

    def run_and_check(args, **kwargs):
        run_and_check_calls.append(list(args))
        events.append(("run", list(args)))
        return ""

    def check_rabbitmq_is_available(docker_id, cookie):
        if (
            ready_from_attempt is not None
            and counts["recreations"] >= ready_from_attempt
        ):
            return True
        raise RuntimeError("stub: await_startup failed with return code 69")

    def warning(msg, *args):
        # Rendered exactly as logging would, so the assertions below read the same
        # bytes the job script greps out of the per-worker pytest log.
        warnings.append(msg % args if args else msg)

    def copyfile(src, dst):
        if not snapshot_ok:
            raise FileNotFoundError(src)
        copied.append((src, dst))
        events.append(("copy", src, dst))

    namespace = {
        "logging": types.SimpleNamespace(debug=lambda *a, **k: None, warning=warning),
        "subprocess": types.SimpleNamespace(check_call=check_call),
        "rabbitmq_debuginfo": rabbitmq_debuginfo,
        "check_rabbitmq_is_available": check_rabbitmq_is_available,
        "run_and_check": run_and_check,
        "time": _FakeClock(),
        # Real path joining, but no directory is created: these arms touch no filesystem.
        # `abspath` is resolved against the test directory, matching how the real waiter
        # runs (pytest's rootdir is tests/integration), so the arms below see the same
        # absolute path a CI consumer would read.
        "os": types.SimpleNamespace(
            path=types.SimpleNamespace(
                join=os.path.join,
                abspath=lambda p: os.path.normpath(os.path.join(TEST_CWD, p)),
                dirname=os.path.dirname,
            ),
            makedirs=lambda *a, **k: None,
            getpid=lambda: pid,
        ),
        "shutil": types.SimpleNamespace(copyfile=copyfile),
        "temp_dir": TEMP_DIR,
        "RABBITMQ_RECREATE_TOKEN": TOKEN,
        "open": lambda *a, **k: _NullFile(),
    }
    exec(  # pylint:disable=exec-used
        compile(ast.Module(body=[_waiter_ast()], type_ignores=[]), CLUSTER_PY, "exec"),
        namespace,
    )

    def get_instance_docker_id(service):
        counts["recreations"] += 1
        return f"stub-docker-id-{counts['recreations']}"

    if cluster is None:
        cluster = types.SimpleNamespace(
            print_all_docker_pieces=lambda: None,
            get_instance_ip=lambda host: "127.0.0.1",
            get_instance_docker_id=get_instance_docker_id,
            rabbitmq_host="rabbitmq1",
            rabbitmq_docker_id="stub-docker-id-0",
            rabbitmq_cookie="stub-cookie",
            rabbitmq_dir="/tmp",
            rabbitmq_logs_dir="/tmp/logs",
            project_name=project_name,
            rabbitmq_wait_calls=0,
            base_rabbitmq_cmd=["docker", "compose"],
        )
    else:
        # Re-entering the waiter on the same cluster, as `reset_rabbitmq` does.
        cluster.get_instance_docker_id = get_instance_docker_id

    raised = None
    returned = None
    kwargs = {"timeout": TIMEOUT}
    if retries is not None:
        kwargs["retries"] = retries
    try:
        returned = namespace[FUNC_NAME](cluster, **kwargs)
    except RuntimeError as ex:
        raised = str(ex)
    counts["warnings"] = warnings
    counts["copied"] = copied
    counts["events"] = events
    return raised, returned, counts, run_and_check_calls, cluster


def test_healthy_broker_collects_nothing():
    """A broker that comes up on the first attempt is untouched: no diagnostics,
    no recreation."""
    raised, returned, counts, calls, _ = _run(ready_from_attempt=0)
    assert raised is None
    assert returned is True
    assert counts["logs"] == 0
    assert counts["debuginfo"] == 0
    assert counts["recreations"] == 0
    assert calls == []


def test_recreation_heals_a_hung_container():
    """The arm that pins the fix: a container hung on the first attempt no longer
    fails the module - it is recreated and the second attempt succeeds."""
    raised, returned, counts, calls, cluster = _run(ready_from_attempt=1)
    assert raised is None
    assert returned is True
    # One set of diagnostics from the failed first attempt.
    assert counts["logs"] == 1
    assert counts["debuginfo"] == 1
    assert counts["recreations"] == 1
    # The hung container is force-removed and a fresh one is brought up.
    assert ["docker", "rm", "-f", "-v", "stub-docker-id-0"] in calls
    assert ["docker", "compose", "up", "-d", "--renew-anon-volumes"] in calls
    # The waiter probes the fresh container, not the removed one.
    assert cluster.rabbitmq_docker_id == "stub-docker-id-1"


def test_gives_up_after_all_attempts():
    """A broker that never comes up fails with one set of diagnostics per attempt
    and exactly one recreation between the two default attempts."""
    raised, returned, counts, calls, _ = _run(ready_from_attempt=None)
    assert raised == "Cannot wait RabbitMQ container"
    assert returned is None
    assert counts["logs"] == 2
    assert counts["debuginfo"] == 2
    assert counts["recreations"] == 1


def test_collection_failure_is_best_effort():
    """A failing `docker compose logs` neither spins until a second deadline nor
    prevents the recreation and the retry."""
    raised, returned, counts, calls, _ = _run(ready_from_attempt=1, collection_ok=False)
    assert raised is None
    assert returned is True
    # Attempted once, failed, and debuginfo (guarded by the same try) was skipped.
    assert counts["logs"] == 1
    assert counts["debuginfo"] == 0
    assert counts["recreations"] == 1


def test_debuginfo_failure_is_best_effort():
    """A failing rabbitmq_debuginfo also leaves the recreation and retry intact."""
    raised, returned, counts, calls, _ = _run(ready_from_attempt=1, debuginfo_ok=False)
    assert raised is None
    assert returned is True
    assert counts["logs"] == 1
    assert counts["debuginfo"] == 1
    assert counts["recreations"] == 1


def test_token_is_the_same_literal_in_both_files():
    """The waiter emits the token and the CI job script greps for it. They are
    declared separately, so a reword in one alone would silently report zero
    recreations forever."""
    values = dict(_module_constant("RABBITMQ_RECREATE_TOKEN"))
    assert len(values) == 2
    assert len(set(values.values())) == 1, values


def test_recreation_emits_the_token_and_preserves_the_log():
    """A recovered boot failure leaves a machine-readable record: the token names the
    attempt and the preserved broker log, which is copied out before the container -
    and the directory holding its log - are destroyed."""
    _, _, counts, _, _ = _run(ready_from_attempt=1)
    lines = [line for line in counts["warnings"] if TOKEN in line]
    assert len(lines) == 1
    assert "attempt=1" in lines[0]

    # The log is read from the broker's own log directory and written outside it.
    assert len(counts["copied"]) == 1
    src, dst = counts["copied"][0]
    assert src == "/tmp/logs/rabbit.log"
    assert f"snapshot={dst}" in lines[0]
    # Absolute, so the CI job script - which runs from the repo root, not from the test
    # directory - can still find the file it is told about.
    assert os.path.isabs(dst), dst
    assert os.path.dirname(dst) == EXPECTED_SNAPSHOT_DIR, dst
    # First waiter call in this process on this cluster, second attempt.
    assert (
        os.path.basename(dst) == f"rabbit-{PROJECT_NAME}-pid{PID}-call1-attempt1.log"
    ), dst

    # The copy happens while the failed container - and the directory holding the log
    # it reads - still exist. Copying afterwards would preserve the next attempt's log
    # while the count still looked right.
    kinds = [event[0] for event in counts["events"]]
    copy_at = kinds.index("copy")
    removed_at = counts["events"].index(
        ("run", ["docker", "rm", "-f", "-v", "stub-docker-id-0"])
    )
    recreated_at = counts["events"].index(
        ("run", ["docker", "compose", "up", "-d", "--renew-anon-volumes"])
    )
    assert copy_at < removed_at, counts["events"]
    assert copy_at < recreated_at, counts["events"]


def test_snapshot_names_are_unique_per_process_worker_call_and_attempt():
    """S3 upload keys are built from the basename alone, so two preserved logs must
    not share a name: one recreation would overwrite the other's evidence.

    Every recreation in a job competes for one namespace, so the name must vary along
    all four axes that can produce two of them at once: the attempt within a waiter
    call, the waiter call within one cluster (`reset_rabbitmq` re-enters it), the
    cluster, whose project name carries the xdist worker id, and the pytest process,
    since each batch and each rerun is a fresh one sharing `ci/tmp`, and the counter
    lives on the cluster object rather than on disk.
    """
    names = []
    cluster = None
    for retries in (3, 4):
        _, _, counts, _, cluster = _run(
            ready_from_attempt=None, retries=retries, cluster=cluster
        )
        # Every attempt but the first recreates, so each preserves its own log.
        assert len(counts["copied"]) == retries - 1
        names += [os.path.basename(dst) for _, dst in counts["copied"]]
    # Attempt and call axes: across both calls on one cluster, every name is distinct.
    assert len(names) == 5, names
    assert len(set(names)) == len(names), names

    mine = os.path.basename(_run(ready_from_attempt=None)[2]["copied"][0][1])

    # Worker axis: the same attempt on another worker gets another name.
    _, _, other, _, _ = _run(ready_from_attempt=None, project_name="stubproject-gw7")
    other_name = os.path.basename(other["copied"][0][1])
    assert other_name != mine
    assert PROJECT_NAME in mine and "stubproject-gw7" in other_name

    # Process axis: a rerun of the same module is a new process, so it starts over at
    # call 1 with the same project name and would otherwise reuse the same name.
    _, _, rerun, _, _ = _run(ready_from_attempt=None, pid=PID + 1)
    rerun_name = os.path.basename(rerun["copied"][0][1])
    assert rerun_name != mine, (rerun_name, mine)
    assert f"pid{PID}" in mine and f"pid{PID + 1}" in rerun_name


def test_missing_broker_log_still_recreates_and_reports():
    """A boot so early that no broker log exists must still be counted: the token is
    emitted with an empty snapshot rather than the recreation going unreported."""
    _, returned, counts, _, _ = _run(ready_from_attempt=1, snapshot_ok=False)
    assert returned is True
    assert counts["recreations"] == 1
    lines = [line for line in counts["warnings"] if TOKEN in line]
    assert len(lines) == 1
    assert "snapshot= " in lines[0] + " "
    assert counts["copied"] == []
