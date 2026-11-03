"""Pins that ClickHouseCluster.exec_in_container does not consult the exit code of an
exec it was asked to detach from.

Docker reports `ExitCode: None` for an exec that is still running and a real integer once
it has finished, so inspecting a detached exec returns whichever of the two the inspection
happened to race into. A server that exits quickly - as one started with a config it
rejects does - therefore made the harness raise out of the start call instead of letting
the caller observe the outcome it was waiting for.

The function under test is loaded out of helpers/cluster.py by AST extraction and executed
against stubs, so these assertions track the shipped source rather than a copy of it, and
they are deterministic: no Docker, no container and no ClickHouseCluster instance.
"""

import ast
import os
import types
from typing import Any, Sequence

CLUSTER_PY = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "helpers", "cluster.py")
)
CLASS_NAME = "ClickHouseCluster"
FUNC_NAME = "exec_in_container"

EXEC_ID = {"Id": "exec-1234"}
EXIT_CODE = 36
SENTINEL_OUTPUT = b"detached-output\n"


def _function_ast():
    """Extract ClickHouseCluster.exec_in_container, scoped to its class.

    ClickHouseInstance defines a forwarder of the same name, so the extraction is scoped and
    asserted unique: an unscoped match would silently hand the arms below the wrong function.
    """
    with open(CLUSTER_PY, encoding="utf-8") as f:
        module = ast.parse(f.read())
    classes = [
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == CLASS_NAME
    ]
    assert len(classes) == 1, f"{CLASS_NAME} not found exactly once in {CLUSTER_PY}"
    functions = [
        node
        for node in classes[0].body
        if isinstance(node, ast.FunctionDef) and node.name == FUNC_NAME
    ]
    assert (
        len(functions) == 1
    ), f"{CLASS_NAME}.{FUNC_NAME} not found exactly once in {CLUSTER_PY}"
    return functions[0]


def _run(exit_code, output=b"", **kwargs):
    """Execute the shipped function against stubs and report what it did."""
    counts = {"inspects": 0}

    def exec_create(container_id, cmd, **_):
        return EXEC_ID

    def exec_start(exec_id, detach=False):
        return output

    def exec_inspect(exec_id):
        counts["inspects"] += 1
        return {"ExitCode": exit_code}

    # inspect_container and inspect_image are reached only on the failing path, and without
    # them it would die of an AttributeError before the raise the arms below assert on.
    api = types.SimpleNamespace(
        exec_create=exec_create,
        exec_start=exec_start,
        exec_inspect=exec_inspect,
        inspect_container=lambda container_id: {"Image": "image-1"},
        inspect_image=lambda image_id: {},
    )
    cluster = types.SimpleNamespace(docker_client=types.SimpleNamespace(api=api))

    # `Any` and `Sequence` are the annotations of the shipped signature, evaluated when the
    # def executes because cluster.py does not postpone them.
    namespace = {
        "Any": Any,
        "Sequence": Sequence,
        "logging": types.SimpleNamespace(debug=lambda *a, **k: None),
        "pprint": types.SimpleNamespace(pprint=lambda *a, **k: None),
    }
    exec(  # pylint:disable=exec-used
        compile(
            ast.Module(body=[_function_ast()], type_ignores=[]), CLUSTER_PY, "exec"
        ),
        namespace,
    )

    raised = None
    returned = None
    try:
        returned = namespace[FUNC_NAME](
            cluster, "container-1", ["bash", "-c", f"exit {EXIT_CODE}"], **kwargs
        )
    except Exception as ex:  # pylint:disable=broad-except
        # Broad: the function raises a bare Exception, and the arms assert its message.
        raised = str(ex)
    return raised, returned, counts


def test_a_detached_exec_is_not_failed_by_its_exit_code():
    """The arm that pins the fix: a detached exec that has already exited nonzero is
    returned to the caller, and its exit code is not even fetched."""
    raised, returned, counts = _run(
        EXIT_CODE, detach=True, use_cli=False, get_exec_id=True
    )
    assert raised is None, raised
    assert returned is EXEC_ID
    assert counts["inspects"] == 0


def test_a_detached_exec_returns_its_output_without_inspecting():
    """The other half of the guarded return: a detached caller that did not ask for the exec
    id gets the output object back, and the exit code is still not fetched."""
    raised, returned, counts = _run(
        EXIT_CODE, output=SENTINEL_OUTPUT, detach=True, use_cli=False
    )
    assert raised is None, raised
    assert returned is SENTINEL_OUTPUT
    assert counts["inspects"] == 0


def test_a_waited_exec_still_fails_on_a_nonzero_exit_code():
    """Control: the caller that did wait still gets the failure, so the fix cannot have
    disabled the check for everyone."""
    raised, returned, counts = _run(EXIT_CODE, detach=False, use_cli=False)
    assert raised is not None
    assert f"Return code {EXIT_CODE}" in raised, raised
    assert returned is None
    assert counts["inspects"] == 1


def test_a_waited_exec_returns_its_output():
    """Control: the waited path still decodes and returns the output it captured."""
    raised, returned, counts = _run(0, output=b"ok\n", detach=False, use_cli=False)
    assert raised is None, raised
    assert returned == "ok\n"
    assert counts["inspects"] == 1
