"""Pins that ClickHouseCluster.wait_rabbitmq_to_start collects its RabbitMQ
diagnostics once, not until a second deadline expires.

The helper under test is loaded out of helpers/cluster.py by AST extraction and
executed against stubs, so these assertions track the shipped source rather than
a copy of it. No Docker, no broker and no ClickHouseCluster instance is needed.
"""

import ast
import os
import time
import types

HELPERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "helpers")
CLUSTER_PY = os.path.normpath(os.path.join(HELPERS_DIR, "cluster.py"))
FUNC_NAME = "wait_rabbitmq_to_start"

# A short deadline keeps every arm sub-second. The helper's own default is 120.
TIMEOUT = 1.0


def _waiter_ast():
    with open(CLUSTER_PY, encoding="utf-8") as f:
        source = f.read()
    module = ast.parse(source)
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == FUNC_NAME:
            return node
    raise AssertionError(f"{FUNC_NAME} not found in {CLUSTER_PY}")


def _run(readiness_ok, collection_ok):
    """Execute the shipped waiter against stubs and count collection attempts."""
    counts = {"logs": 0, "debuginfo": 0}

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

    def check_rabbitmq_is_available(docker_id, cookie):
        if readiness_ok:
            return True
        raise RuntimeError("stub: await_startup failed with return code 69")

    namespace = {
        "logging": types.SimpleNamespace(debug=lambda *a, **k: None),
        "subprocess": types.SimpleNamespace(check_call=check_call),
        "rabbitmq_debuginfo": rabbitmq_debuginfo,
        "check_rabbitmq_is_available": check_rabbitmq_is_available,
        "time": time,
        "os": os,
        "open": lambda *a, **k: _NullFile(),
    }
    exec(  # pylint:disable=exec-used
        compile(ast.Module(body=[_waiter_ast()], type_ignores=[]), CLUSTER_PY, "exec"),
        namespace,
    )

    cluster = types.SimpleNamespace(
        print_all_docker_pieces=lambda: None,
        get_instance_ip=lambda host: "127.0.0.1",
        rabbitmq_host="rabbitmq1",
        rabbitmq_docker_id="stub-docker-id",
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
    return raised, returned, counts


def test_collects_once_when_collection_succeeds():
    """The failing path collects exactly one set of diagnostics before raising."""
    raised, returned, counts = _run(readiness_ok=False, collection_ok=True)
    assert raised == "Cannot wait RabbitMQ container"
    assert returned is None
    assert counts["logs"] == 1
    assert counts["debuginfo"] == 1


def test_retries_when_collection_fails():
    """A collection that fails still retries to the deadline.

    This is the arm that pins the break's position: moving it out of the try
    block leaves the count above green but drops this retry.
    """
    raised, returned, counts = _run(readiness_ok=False, collection_ok=False)
    assert raised == "Cannot wait RabbitMQ container"
    assert returned is None
    assert counts["logs"] > 1
    assert counts["debuginfo"] == 0


def test_healthy_broker_collects_nothing():
    """Reachability control, not a witness: a passing run never enters the loop.

    It is green with or without the break, which is the point - it shows the
    changed loop cannot affect a run that succeeds today.
    """
    raised, returned, counts = _run(readiness_ok=True, collection_ok=True)
    assert raised is None
    assert returned is True
    assert counts["logs"] == 0
    assert counts["debuginfo"] == 0


def test_break_is_last_statement_of_try_body():
    """The break must be a direct, final statement of the try body.

    Checked positionally: a membership test over the whole subtree also accepts
    a break placed after the try/except, which silently drops the retry above.
    """
    loops = [n for n in _waiter_ast().body if isinstance(n, ast.While)]
    assert len(loops) == 2, "expected a readiness loop and a collection loop"

    tries = [n for n in loops[1].body if isinstance(n, ast.Try)]
    assert len(tries) == 1, "expected a single try in the collection loop"

    assert isinstance(
        tries[0].body[-1], ast.Break
    ), "the break must be the last statement of the try body"
    assert not any(
        isinstance(n, ast.Break) for n in loops[1].body
    ), "the break must be inside the try body, not a sibling of it"
