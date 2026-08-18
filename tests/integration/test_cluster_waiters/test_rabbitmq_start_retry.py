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
# assert on is the one the waiter builds. TEMP_DIR is cluster.py's own value, relative to
# the test directory; TEMP_ABS_DIR is the anchored form the waiter writes through, and is
# evaluated out of the shipped source below rather than retyped here.
TEMP_DIR = "../../ci/tmp"
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


def _cluster_temp_abs_dir():
    """Evaluate cluster.py's own `TEMP_ABS_DIR` expression, from a foreign cwd.

    Evaluated rather than retyped so the arms track the shipped anchor, and evaluated
    from `/` so an expression that silently depends on the cwd cannot agree with it.
    """
    with open(CLUSTER_PY, encoding="utf-8") as f:
        module = ast.parse(f.read())
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "TEMP_ABS_DIR" for t in node.targets
        ):
            namespace = {"p": os.path, "HELPERS_DIR": HELPERS_DIR, "temp_dir": TEMP_DIR}
            cwd = os.getcwd()
            os.chdir("/")
            try:
                return eval(  # pylint:disable=eval-used
                    compile(ast.Expression(body=node.value), CLUSTER_PY, "eval"),
                    namespace,
                )
            finally:
                os.chdir(cwd)
    raise AssertionError(f"TEMP_ABS_DIR not found in {CLUSTER_PY}")


TEMP_ABS_DIR = _cluster_temp_abs_dir()


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
    cwd=None,
    failing_command=None,
):
    """Execute the shipped waiter against stubs and record what it did.

    ready_from_attempt: 0-based attempt index from which the broker probe starts
    succeeding; None means it never succeeds.
    cwd: process working directory to run under; the waiter must not depend on it.
    failing_command: `run_and_check` argv token whose call raises, as a nonzero docker
    command does.
    """
    counts = {"logs": 0, "debuginfo": 0, "recreations": 0}
    run_and_check_calls = []
    warnings = []
    copied = []
    made_dirs = []
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
        if failing_command is not None and failing_command in args:
            # `run_and_check` raises on a nonzero exit unless the caller passes nothrow.
            if not kwargs.get("nothrow"):
                raise Exception(f"stub: {failing_command} exited nonzero")
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
        # The real `os.path`, so a path expression that depends on the process cwd is
        # visible to the arms below; only `makedirs` is stubbed, since these arms touch
        # no filesystem. The cwd itself is varied by `_run(cwd=...)`.
        "os": types.SimpleNamespace(
            path=os.path,
            makedirs=lambda *a, **k: made_dirs.append(a[0]),
            getpid=lambda: pid,
        ),
        "shutil": types.SimpleNamespace(copyfile=copyfile),
        "temp_dir": TEMP_DIR,
        "TEMP_ABS_DIR": TEMP_ABS_DIR,
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
    previous_cwd = os.getcwd()
    if cwd is not None:
        os.chdir(cwd)
    try:
        returned = namespace[FUNC_NAME](cluster, **kwargs)
    except Exception as ex:  # pylint:disable=broad-except
        # Broad: a failing docker command raises a bare `Exception`, and an arm below
        # asserts what the waiter had already recorded when it propagated.
        raised = str(ex)
    finally:
        os.chdir(previous_cwd)
    counts["warnings"] = warnings
    counts["copied"] = copied
    counts["events"] = events
    counts["made_dirs"] = made_dirs
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


def test_token_is_emitted_before_the_container_is_recreated():
    """`docker compose up` can exit nonzero, and `run_and_check` raises when it does, so
    the token has to be on record before the recreation is attempted. Emitting it
    afterwards would lose exactly the events worth reporting: the ones where recovery
    itself broke."""
    raised, returned, counts, _, _ = _run(ready_from_attempt=1, failing_command="up")
    assert raised is not None and "up" in raised
    assert returned is None

    # The recreation was attempted and it failed, yet the record survives the failure.
    lines = [line for line in counts["warnings"] if TOKEN in line]
    assert len(lines) == 1, counts["warnings"]
    assert "attempt=1" in lines[0]
    # And so does the preserved broker log the token names.
    assert len(counts["copied"]) == 1
    assert os.path.basename(counts["copied"][0][1]) in lines[0]


def test_both_files_agree_on_the_directory_holding_the_snapshots():
    """The waiter writes the preserved log and the job script scans for it, and only a
    bare name travels between them, so the directory is an unstated agreement. It is
    derived twice from unrelated anchors - the helper's own location and the repository
    root the job script computes from its cwd - because deriving it from the shipped
    expression alone would compare that expression with itself: pointing the waiter at
    some other directory would then agree, while every attachment was silently lost."""
    # helpers -> tests/integration -> tests -> the checkout.
    repo_root = os.path.normpath(os.path.join(HELPERS_DIR, "..", "..", ".."))
    # `integration_test_job.py` builds `temp_path` as f"{Utils.cwd()}/ci/tmp", and CI
    # runs it from the repository root.
    consumer_dir = os.path.normpath(os.path.join(repo_root, "ci", "tmp"))
    assert os.path.normpath(TEMP_ABS_DIR) == consumer_dir, (TEMP_ABS_DIR, consumer_dir)

    # And the job script really does build it that way, rather than from something the
    # waiter cannot reach.
    with open(JOB_PY, encoding="utf-8") as f:
        job_source = f.read()
    assert 'temp_path = f"{repo_dir}/ci/tmp"' in job_source
    assert "repo_dir = Utils.cwd()" in job_source


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
    # First waiter call in this process on this cluster, second attempt.
    name = f"rabbit-{PROJECT_NAME}-pid{PID}-call1-attempt1.log"
    assert os.path.basename(dst) == name, dst
    # Written into the anchored scan directory the CI job script reads back, and named
    # in the log by that bare name: the directory can contain whitespace, the name
    # cannot, and the job script's field is whitespace-delimited.
    assert dst == os.path.join(TEMP_ABS_DIR, name), dst
    assert f"snapshot={name} " in lines[0], lines[0]
    assert os.path.isabs(dst), dst

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


def test_snapshot_location_does_not_depend_on_the_working_directory():
    """The waiter runs under whatever cwd the caller has: a CI job runs it from the repo
    root, and `pytest tests/integration/...` from the root is the documented native
    workflow, while `temp_dir` is written relative to tests/integration. Resolving it
    against the ambient cwd sends the copy outside the repo, where the directory cannot
    be created, so the broker log this preserves is lost on exactly the runs a developer
    makes by hand."""
    destinations = set()
    created = set()
    for cwd in ("/", os.sep.join([HELPERS_DIR, ".."]), os.path.dirname(HELPERS_DIR)):
        _, _, counts, _, _ = _run(ready_from_attempt=1, cwd=cwd)
        assert len(counts["copied"]) == 1, cwd
        destinations.add(counts["copied"][0][1])
        created.update(counts["made_dirs"])

    assert len(destinations) == 1, destinations
    assert destinations == {
        os.path.join(TEMP_ABS_DIR, f"rabbit-{PROJECT_NAME}-pid{PID}-call1-attempt1.log")
    }, destinations
    # The directory it creates is the one it writes into, and it is inside the checkout.
    assert created == {TEMP_ABS_DIR}, created
    assert os.path.isdir(os.path.dirname(TEMP_ABS_DIR)), TEMP_ABS_DIR


def test_logged_snapshot_field_survives_a_checkout_path_with_spaces():
    """The job script parses `snapshot=` on whitespace, so the field must carry a bare
    file name. A checkout under `/home/alice/ClickHouse Work` would otherwise truncate
    the value at the space, and the preserved log would never be attached."""
    _, _, counts, _, _ = _run(ready_from_attempt=1)
    lines = [line for line in counts["warnings"] if TOKEN in line]
    field = [token for token in lines[0].split() if token.startswith("snapshot=")][0][
        len("snapshot=") :
    ]

    _, dst = counts["copied"][0]
    assert field == os.path.basename(dst), (field, dst)
    # No separator and no whitespace, whatever the directory above it looks like.
    assert os.sep not in field, field
    assert field.split() == [field], field
    # And the field alone is enough to find the file, given the scan directory.
    assert os.path.join(TEMP_ABS_DIR, field) == dst, (field, dst)


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
