"""Pins that the cluster helper names the one docker failure that is indistinguishable
from a broken server, and names nothing else.

Docker picks a new endpoint's host-side `veth` name at random and only checks it against
the host network namespace, where a running container's peer name is invisible (it has
been renamed to `eth0`). So a new endpoint can be handed the name an older container's
interface will revert to, and destroying that older container makes the bridge driver
delete the interface *by name* - unregistering the host-side `veth` of an unrelated
running container. The victim keeps running with no interface at all, so for the rest of
the module every connection to it fails with `No route to host` and every connection out
of it with `Network is unreachable`, which reads exactly like a server that stopped
answering. One such collision on master: `test_https_replication/test.py::
test_replication_after_partition` on 68afe2720f73, where `br-321583afc1d6: port
6(veth0ca1ff5)` was unregistered by the teardown of an unrelated project's network.

The two arms that matter pull in opposite directions, and both must hold for the
classification in `ci/jobs/integration_test_job.py` to be worth anything: the verdict has
to fire on the real state (otherwise the job stays red on infrastructure), and it must not
fire on a network error alone (otherwise a genuine failure is relabelled `SKIPPED` and
disappears from the report).

The helpers are loaded out of helpers/cluster.py by AST extraction and executed against
stubs, so these assertions track the shipped source rather than a copy of it. No Docker
and no ClickHouseCluster instance is needed.
"""

import ast
import os
import sys
import types

import pytest

HELPERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "helpers")
CLUSTER_PY = os.path.normpath(os.path.join(HELPERS_DIR, "cluster.py"))
JOB_PY = os.path.normpath(
    os.path.join(HELPERS_DIR, "..", "..", "..", "ci", "jobs", "integration_test_job.py")
)

sys.path.insert(0, os.path.normpath(os.path.join(HELPERS_DIR, "..")))
from helpers.client import QueryRuntimeException  # noqa: E402

DESCRIBE = "describe_lost_network_interface"
QUERY = "query"

# The module-level names the helpers read, in the order they need them resolved. Taken
# from the shipped source rather than retyped, so a reworded marker or a renamed loopback
# set cannot drift away from these arms - and so the probe command the arms feed output
# for is the one that will actually be run.
CONSTANTS = [
    "LOST_NETWORK_INTERFACE_ERROR",
    "NETWORK_INTERFACE_PROBE_TOKEN",
    "NETWORK_INTERFACE_PROBE",
    "DISCONNECTED_INTERFACE_NAMES",
    "NETWORK_INTERFACE_PROBE_TIMEOUT",
    "UNREACHABLE_ADDRESS_ERRORS",
]

DOCKER_ID = "roottesthttpsreplication-gw0-node1-1"
INSTANCE_NAME = "node1"
IP_ADDRESS = "172.16.1.7"

# The failure the collision produces on the client side, as the CI report rendered it.
UNREACHABLE = (
    "Client failed! Return code: 210, stderr: Code: 210. DB::NetException: "
    f"Net Exception: No route to host ({IP_ADDRESS}:9000). (NETWORK_ERROR)"
)


def _module_nodes(path, names):
    """The module-level assignments of `names`, in file order."""
    with open(path, encoding="utf-8") as f:
        module = ast.parse(f.read())
    found = [
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id in names for t in node.targets)
    ]
    missing = set(names) - {
        t.id for node in found for t in node.targets if isinstance(t, ast.Name)
    }
    assert not missing, f"{sorted(missing)} not found in {path}"
    return found


def _constants(path=CLUSTER_PY, names=None):
    """Evaluate the shipped constants together, so the ones defined in terms of each
    other (the probe command embeds the token) come out exactly as shipped."""
    namespace = {}
    exec(  # pylint:disable=exec-used
        compile(
            ast.Module(body=_module_nodes(path, names or CONSTANTS), type_ignores=[]),
            path,
            "exec",
        ),
        namespace,
    )
    return namespace


def _func_ast(name):
    with open(CLUSTER_PY, encoding="utf-8") as f:
        module = ast.parse(f.read())
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {CLUSTER_PY}")


class _Recorder:
    """A stub instance carrying the shipped methods, and a record of what they did."""

    def __init__(self, interfaces, ip_address=IP_ADDRESS, token=True, probe_raises=None):
        self.name = INSTANCE_NAME
        self.docker_id = DOCKER_ID
        self.ip_address = ip_address
        self.client = None
        self._interfaces = interfaces
        self._token = token
        self._probe_raises = probe_raises
        self.constants = _constants()
        self.probes = []
        self.warnings = []

        namespace = dict(self.constants)
        namespace["logging"] = types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda msg, *args: self.warnings.append(
                msg % args if args else msg
            ),
        )
        namespace["QueryRuntimeException"] = QueryRuntimeException
        exec(  # pylint:disable=exec-used
            compile(
                ast.Module(
                    body=[_func_ast(DESCRIBE), _func_ast(QUERY)], type_ignores=[]
                ),
                CLUSTER_PY,
                "exec",
            ),
            namespace,
        )
        for func in (DESCRIBE, QUERY):
            setattr(self, func, types.MethodType(namespace[func], self))

    def exec_in_container(self, cmd, **kwargs):
        self.probes.append((list(cmd), kwargs))
        if self._probe_raises is not None:
            raise self._probe_raises
        lines = list(self._interfaces or [])
        if self._token:
            lines.append(self.constants["NETWORK_INTERFACE_PROBE_TOKEN"])
        return "".join(f"{line}\n" for line in lines)


def _client_raising(exception):
    """A `Client` stub whose `query` fails the way the real one does."""

    def query(sql, **kwargs):
        raise exception

    return types.SimpleNamespace(query=query)


def test_a_container_with_only_loopback_is_reported():
    """The arm that pins the fix: the state the moby collision leaves behind is named,
    with the evidence, instead of being reported as a failure of the server."""
    instance = _Recorder(["lo"])
    verdict = instance.describe_lost_network_interface()
    assert instance.constants["LOST_NETWORK_INTERFACE_ERROR"] in verdict
    assert DOCKER_ID in verdict
    assert IP_ADDRESS in verdict
    assert "'lo'" in verdict
    assert instance.warnings == []
    # Probed inside the container, bounded, and as root - `/sys/class/net` is readable by
    # anyone, but `docker exec` otherwise inherits the image's user, which need not exist.
    assert len(instance.probes) == 1
    cmd, kwargs = instance.probes[0]
    assert cmd == ["bash", "-c", instance.constants["NETWORK_INTERFACE_PROBE"]]
    assert kwargs["nothrow"] is True
    assert kwargs["user"] == "root"
    assert kwargs["timeout"] == instance.constants["NETWORK_INTERFACE_PROBE_TIMEOUT"]


def test_a_connected_container_is_not_reported():
    """The counter-arm: an unreachable server whose interface is still attached is a
    failure like any other. Relabelling it would delete a real regression from the
    report, so the verdict must stay silent here."""
    instance = _Recorder(["eth0", "lo"])
    assert instance.describe_lost_network_interface() == ""
    assert len(instance.probes) == 1
    assert instance.warnings == []


def test_an_instance_with_no_address_is_not_probed():
    """Before `start` and after `shutdown` there is no attachment to contradict, so
    there is nothing to conclude - and no reason to spend a `docker exec` on it."""
    instance = _Recorder(["lo"], ip_address=None)
    assert instance.describe_lost_network_interface() == ""
    assert instance.probes == []
    assert instance.warnings == []


def test_a_probe_that_did_not_run_is_not_a_verdict():
    """A container that is already gone, or a daemon too busy to exec, produces no
    interface lines - which must not be read as "the interface was removed"."""
    instance = _Recorder(None, token=False)
    assert instance.describe_lost_network_interface() == ""
    assert instance.warnings == []


def test_an_unmatched_glob_is_not_a_verdict():
    """With no /sys mounted the shipped probe echoes its own pattern. That is not an
    interface, but it is not evidence of absence either."""
    assert _Recorder(["/sys/class/net/*"]).describe_lost_network_interface() == ""


def test_a_failing_probe_reports_nothing_and_says_why():
    """The helper describes an error that has already happened and is about to be
    reported. Its own failure must not take that error's place, but it must not vanish
    either."""
    instance = _Recorder(["lo"], probe_raises=Exception("timed out after 30s"))
    assert instance.describe_lost_network_interface() == ""
    assert len(instance.warnings) == 1
    assert INSTANCE_NAME in instance.warnings[0]
    assert "timed out after 30s" in instance.warnings[0]


def test_query_reports_the_cause_and_keeps_the_original_failure():
    """`query` is where the collision surfaces once the cluster is already up: the
    module's queries just start failing. The report has to carry the cause and the
    original error both, as the same exception type with its fields, because callers
    catch `QueryRuntimeException` and read `returncode`."""
    original = QueryRuntimeException(UNREACHABLE, 210, "Code: 210.")
    instance = _Recorder(["lo"])
    instance.client = _client_raising(original)
    with pytest.raises(QueryRuntimeException) as raised:
        instance.query("SELECT count() FROM test_table")
    assert instance.constants["LOST_NETWORK_INTERFACE_ERROR"] in str(raised.value)
    assert UNREACHABLE in str(raised.value)
    assert raised.value.returncode == 210
    assert raised.value.stderr == "Code: 210."
    assert raised.value.__cause__ is original


def test_query_leaves_a_reachable_container_s_failure_alone():
    """Same unreachable-address error, interface still attached: the original exception
    propagates untouched, so a test asserting on it sees exactly what it did before."""
    original = QueryRuntimeException(UNREACHABLE, 210, "Code: 210.")
    instance = _Recorder(["eth0", "lo"])
    instance.client = _client_raising(original)
    with pytest.raises(QueryRuntimeException) as raised:
        instance.query("SELECT count() FROM test_table")
    assert raised.value is original


def test_query_does_not_probe_an_ordinary_query_error():
    """Every failing query would otherwise pay for a `docker exec`, inside retry loops
    that run twenty of them. Only the two errors the missing interface produces are
    worth investigating."""
    original = QueryRuntimeException("Code: 60. DB::Exception: Table does not exist", 47, "")
    instance = _Recorder(["lo"])
    instance.client = _client_raising(original)
    with pytest.raises(QueryRuntimeException) as raised:
        instance.query("SELECT count() FROM test_table")
    assert raised.value is original
    assert instance.probes == []


@pytest.mark.parametrize("error", _constants()["UNREACHABLE_ADDRESS_ERRORS"])
def test_query_investigates_both_unreachable_address_errors(error):
    """`No route to host` is what the harness sees from outside and `Network is
    unreachable` what a server-to-server query reports from inside the victim, so both
    have to reach the probe."""
    original = QueryRuntimeException(f"Client failed! stderr: {error}", 210, "")
    instance = _Recorder(["lo"])
    instance.client = _client_raising(original)
    with pytest.raises(QueryRuntimeException) as raised:
        instance.query("SELECT count() FROM test_table")
    assert instance.constants["LOST_NETWORK_INTERFACE_ERROR"] in str(raised.value)


def test_query_returns_its_answer_untouched():
    """The wrapper sits on the path of every query in the suite; the passing path must
    stay exactly what it was."""
    instance = _Recorder(["lo"])
    instance.client = types.SimpleNamespace(query=lambda sql, **kwargs: "100\n")
    assert instance.query("SELECT count() FROM test_table") == "100\n"
    assert instance.probes == []


def test_the_marker_matches_the_one_the_ci_job_classifies_on():
    """The marker is a contract between the harness that emits it and the job that
    labels it an infrastructure error. Nothing links the two copies at runtime, so a
    reword on either side would silently turn the classification off."""
    name = "LOST_NETWORK_INTERFACE_ERROR"
    assert (
        _constants(CLUSTER_PY, [name])[name] == _constants(JOB_PY, [name])[name]
    ), f"{name} differs between {CLUSTER_PY} and {JOB_PY}"
