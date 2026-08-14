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
FUNC_NAME = "wait_rabbitmq_to_start"

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


def _run(ready_from_attempt, collection_ok=True, debuginfo_ok=True):
    """Execute the shipped waiter against stubs and record what it did.

    ready_from_attempt: 0-based attempt index from which the broker probe starts
    succeeding; None means it never succeeds.
    """
    counts = {"logs": 0, "debuginfo": 0, "recreations": 0}
    run_and_check_calls = []

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
        return ""

    def check_rabbitmq_is_available(docker_id, cookie):
        if (
            ready_from_attempt is not None
            and counts["recreations"] >= ready_from_attempt
        ):
            return True
        raise RuntimeError("stub: await_startup failed with return code 69")

    namespace = {
        "logging": types.SimpleNamespace(
            debug=lambda *a, **k: None, warning=lambda *a, **k: None
        ),
        "subprocess": types.SimpleNamespace(check_call=check_call),
        "rabbitmq_debuginfo": rabbitmq_debuginfo,
        "check_rabbitmq_is_available": check_rabbitmq_is_available,
        "run_and_check": run_and_check,
        "time": _FakeClock(),
        "os": os,
        "open": lambda *a, **k: _NullFile(),
    }
    exec(  # pylint:disable=exec-used
        compile(ast.Module(body=[_waiter_ast()], type_ignores=[]), CLUSTER_PY, "exec"),
        namespace,
    )

    def get_instance_docker_id(service):
        counts["recreations"] += 1
        return f"stub-docker-id-{counts['recreations']}"

    cluster = types.SimpleNamespace(
        print_all_docker_pieces=lambda: None,
        get_instance_ip=lambda host: "127.0.0.1",
        get_instance_docker_id=get_instance_docker_id,
        rabbitmq_host="rabbitmq1",
        rabbitmq_docker_id="stub-docker-id-0",
        rabbitmq_cookie="stub-cookie",
        rabbitmq_dir="/tmp",
        base_rabbitmq_cmd=["docker", "compose"],
    )

    raised = None
    returned = None
    try:
        returned = namespace[FUNC_NAME](cluster, timeout=TIMEOUT)
    except RuntimeError as ex:
        raised = str(ex)
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
