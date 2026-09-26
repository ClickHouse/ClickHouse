"""Pins that the harness names the one docker failure that is indistinguishable from a
broken server, and names nothing else.

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

The verdict reaches a pytest result through one gate,
`ClickHouseInstance.describe_transport_error`, which every request consults - so it is
attached wherever the state surfaces, and not only on the entrypoints that raise. `query`
is one of those; `query_and_get_error` and `query_and_get_answer_with_error` hand the error
back for the test to assert on, and `get_query_request` hands back a handle the test
collects from later. The arms below drive the real `CommandRequest` over a failing command,
so they pin that path rather than a description of it. The HTTP helpers do not go through
`Client` and have their own arms.

Which instance the gate belongs to is decided by the address the failed request named, not
by the client that carried it, and that routing has arms of its own: a `Client` a test
builds straight against a node's IP is covered without being wired up, and
`query(host=...)` investigates the node it was re-aimed at rather than the one the client
belongs to.

`describe_lost_network_interface`, the gate and the HTTP wrapper are loaded out of
helpers/cluster.py by AST extraction and executed against stubs, so these assertions track
the shipped source rather than a copy of it. No Docker and no ClickHouseCluster instance is
needed.
"""

import ast
import os
import shlex
import sys
import types

import pytest
import requests

HELPERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "helpers")
CLUSTER_PY = os.path.normpath(os.path.join(HELPERS_DIR, "cluster.py"))
JOB_PY = os.path.normpath(
    os.path.join(HELPERS_DIR, "..", "..", "..", "ci", "jobs", "integration_test_job.py")
)

sys.path.insert(0, os.path.normpath(os.path.join(HELPERS_DIR, "..")))
import helpers.client as client_module  # noqa: E402
from helpers.client import Client, CommandRequest, QueryRuntimeException  # noqa: E402

DESCRIBE = "describe_lost_network_interface"
GATE = "describe_transport_error"
HTTP = "_http_request_naming_transport_error"
HTTP_ENTRYPOINTS = ["http_query_and_get_answer_with_error", "http_request"]

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
# The node a test re-aims a client at with `query(host=...)`, and the node the harness
# never built a client for at all.
OTHER_IP_ADDRESS = "172.16.1.8"

# What the collision leaves on the client's stderr, as the CI report rendered it.
UNREACHABLE = (
    "Code: 210. DB::NetException: Net Exception: No route to host "
    f"({IP_ADDRESS}:9000). (NETWORK_ERROR)"
)

# A failure of the server itself: the thing the verdict must never be attached to.
ORDINARY = "Code: 60. DB::Exception: Table test.hits does not exist. (UNKNOWN_TABLE)"

# The same collision seen by `requests`, which wraps the errno rather than reporting it.
HTTP_UNREACHABLE = (
    f"HTTPConnectionPool(host='{IP_ADDRESS}', port=8123): Max retries exceeded with url: "
    "/?query=SELECT+1 (Caused by NewConnectionError('<urllib3.connection.HTTPConnection "
    "object at 0x7f9>: Failed to establish a new connection: [Errno 113] No route to "
    "host'))"
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


def _func_ast(name, path=CLUSTER_PY):
    with open(path, encoding="utf-8") as f:
        module = ast.parse(f.read())
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {path}")


class _Recorder:
    """A stub instance carrying the shipped methods, and a record of what they did."""

    def __init__(self, interfaces, ip_address=IP_ADDRESS, token=True, probe_raises=None):
        self.name = INSTANCE_NAME
        self.docker_id = DOCKER_ID
        self.ip_address = ip_address
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
        namespace["requests"] = requests
        exec(  # pylint:disable=exec-used
            compile(
                ast.Module(
                    body=[_func_ast(name) for name in (DESCRIBE, GATE, HTTP)],
                    type_ignores=[],
                ),
                CLUSTER_PY,
                "exec",
            ),
            namespace,
        )
        for func in (DESCRIBE, GATE, HTTP):
            setattr(self, func, types.MethodType(namespace[func], self))

    def exec_in_container(self, cmd, **kwargs):
        self.probes.append((list(cmd), kwargs))
        if self._probe_raises is not None:
            raise self._probe_raises
        lines = list(self._interfaces or [])
        if self._token:
            lines.append(self.constants["NETWORK_INTERFACE_PROBE_TOKEN"])
        return "".join(f"{line}\n" for line in lines)


def _failing_command(stderr, returncode, stdout=""):
    """A command that fails the way `clickhouse-client` does, so the real
    `CommandRequest` can be driven without a server."""
    return [
        "/bin/bash",
        "-c",
        f"printf %s {shlex.quote(stdout)}; printf %s {shlex.quote(stderr)} >&2; "
        f"exit {returncode}",
    ]


def _request(instance, stderr=UNREACHABLE, returncode=210, stdout="", **kwargs):
    """A real `CommandRequest` wired to `instance`'s gate, as `Client` builds them."""
    return CommandRequest(
        _failing_command(stderr, returncode, stdout),
        stdin="",
        describe_transport_error=instance.describe_transport_error,
        **kwargs,
    )


def _raising(exception):
    def request():
        raise exception

    return request


def _connection_error(message):
    return requests.exceptions.ConnectionError(
        message, request="the-request", response="the-response"
    )


# Stands in for the shipped verdict in the arms that pin the routing rather than the
# verdict, so a failure there names the wiring and not the wording.
CAUSE = "a stand-in verdict"


@pytest.fixture(autouse=True)
def _uninstalled_describer():
    """The describer is module state of `helpers/client.py`, which `helpers/cluster.py`
    installs into at import time. These arms install their own, so each one starts from
    nothing installed and leaves nothing behind for the next."""
    client_module.set_transport_error_describer(None)
    yield
    client_module.set_transport_error_describer(None)


def _failing_client():
    """The shipped `Client`, aimed at `IP_ADDRESS`, over a command that fails the way
    `clickhouse-client` does when the interface is gone."""
    client = Client(IP_ADDRESS, command="/bin/bash")
    # `Client` builds `[command, --host, <ip>, --port, <port>, --stacktrace]`, and `host=`
    # rewrites the token after `--host`. Keep that tail exactly as shipped so the arms
    # exercise the real argv, and make `bash` run the failing script instead of reading the
    # flags by putting `-c <script> <argv0>` in front of it.
    client.command = (
        _failing_command(UNREACHABLE, 210) + ["clickhouse-client"] + client.command[1:]
    )
    return client


def _recording_describer_and_client():
    calls = []

    def describer(host, error_text):
        calls.append((host, error_text))
        return CAUSE

    client_module.set_transport_error_describer(describer)
    return calls, _failing_client()


def _registry():
    """The shipped address registry: the mirror, the lookup that reads it, and the
    `ip_address` property that maintains it, executed against a stub instance."""
    with open(CLUSTER_PY, encoding="utf-8") as f:
        module = ast.parse(f.read())

    instance_class = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == "ClickHouseInstance"
    )
    members = [
        node
        for node in instance_class.body
        if (isinstance(node, ast.Assign) and node.targets[0].id == "_ip_address")
        or (isinstance(node, ast.FunctionDef) and node.name == "ip_address")
    ]
    assert len(members) == 3, "expected `_ip_address` and both halves of the property"

    stub = ast.parse(
        f"class Instance:\n"
        f"    name = {INSTANCE_NAME!r}\n"
        f"    def describe_transport_error(self, error_text):\n"
        f"        return f'{{self.name}} says so'\n"
    ).body[0]
    stub.body.extend(members)

    namespace = {}
    exec(  # pylint:disable=exec-used
        compile(
            ast.Module(
                body=_module_nodes(CLUSTER_PY, ["_INSTANCES_BY_ADDRESS"])
                + [_func_ast("describe_transport_error_for_host"), stub],
                type_ignores=[],
            ),
            CLUSTER_PY,
            "exec",
        ),
        namespace,
    )
    return namespace


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


@pytest.mark.parametrize("error", _constants()["UNREACHABLE_ADDRESS_ERRORS"])
def test_the_gate_investigates_both_unreachable_address_errors(error):
    """`No route to host` is what the harness sees from outside the victim and `Network
    is unreachable` what a query inside it reports, so both have to reach the probe."""
    instance = _Recorder(["lo"])
    verdict = instance.describe_transport_error(f"Code: 210. DB::NetException: {error}")
    assert instance.constants["LOST_NETWORK_INTERFACE_ERROR"] in verdict


def test_the_gate_does_not_investigate_an_ordinary_error():
    """Every failing query in the suite goes through the gate, and retry loops run
    twenty of them, so only the two errors a missing interface produces may cost a
    `docker exec`."""
    instance = _Recorder(["lo"])
    assert instance.describe_transport_error(ORDINARY) == ""
    assert instance.probes == []


def test_the_gate_says_nothing_about_a_request_that_did_not_fail():
    """A request that succeeded leaves no stderr, and there is nothing to explain."""
    instance = _Recorder(["lo"])
    assert instance.describe_transport_error("") == ""
    assert instance.probes == []


def test_a_raised_failure_reports_the_cause_and_keeps_its_fields():
    """`query` is where the collision surfaces once the cluster is up: the module's
    queries just start failing. The report has to carry the cause and the original error
    both, as the same exception type with its fields, because callers catch
    `QueryRuntimeException` and read `returncode`."""
    instance = _Recorder(["lo"])
    with pytest.raises(QueryRuntimeException) as raised:
        _request(instance).get_answer()
    marker = instance.constants["LOST_NETWORK_INTERFACE_ERROR"]
    assert str(raised.value).startswith(marker)
    assert UNREACHABLE in str(raised.value)
    assert raised.value.returncode == 210
    assert raised.value.stderr == UNREACHABLE


def test_a_returned_error_carries_the_cause():
    """`query_and_get_error` hands the error back for the test to assert on instead of
    raising it, and that assertion is the only text the CI report will carry. So the
    cause goes into the value - and only when the test was going to fail on it anyway."""
    instance = _Recorder(["lo"])
    error = _request(instance).get_error()
    assert error.startswith(instance.constants["LOST_NETWORK_INTERFACE_ERROR"])
    assert error.endswith(UNREACHABLE)


def test_a_returned_answer_and_error_carries_the_cause():
    """`query_and_get_answer_with_error` reports through the second element, and its
    answer must come back untouched."""
    instance = _Recorder(["lo"])
    stdout, error = _request(instance, stdout="partial\n").get_answer_and_error()
    assert stdout == "partial\n"
    assert error.startswith(instance.constants["LOST_NETWORK_INTERFACE_ERROR"])
    assert error.endswith(UNREACHABLE)


def test_an_ignored_error_is_raised_when_the_interface_is_gone():
    """`ignore_error` promises to ignore what the *server* answers and cannot promise
    more: with the container cut off there is no answer to ignore, and handing back the
    empty stdout would leave the test asserting on nothing, with the run's actual cause
    nowhere in the report."""
    instance = _Recorder(["lo"])
    with pytest.raises(QueryRuntimeException) as raised:
        _request(instance, ignore_error=True).get_answer()
    assert str(raised.value).startswith(
        instance.constants["LOST_NETWORK_INTERFACE_ERROR"]
    )
    assert raised.value.returncode == 210


def test_an_ignored_error_stays_ignored():
    """The counter-arm, for the 74 call sites that pass `ignore_error`: a server error is
    still swallowed, and is not even investigated."""
    instance = _Recorder(["lo"])
    request = _request(instance, stderr=ORDINARY, returncode=47, ignore_error=True)
    assert request.get_answer() == ""
    assert instance.probes == []


def test_a_reachable_container_s_failure_is_untouched():
    """Same unreachable-address error with the interface still attached: the failure is
    reported exactly as it was before, so a test asserting on it is unaffected."""
    instance = _Recorder(["eth0", "lo"])
    with pytest.raises(QueryRuntimeException) as raised:
        _request(instance).get_answer()
    assert str(raised.value) == (
        f"Client failed! Return code: 210, stderr: {UNREACHABLE}"
    )
    assert len(instance.probes) == 1


def test_an_ordinary_failure_is_reported_as_it_was():
    instance = _Recorder(["lo"])
    with pytest.raises(QueryRuntimeException) as raised:
        _request(instance, stderr=ORDINARY, returncode=47).get_answer()
    assert str(raised.value) == f"Client failed! Return code: 47, stderr: {ORDINARY}"
    assert instance.probes == []


def test_a_successful_request_is_not_investigated():
    """The gate sits on the path of every request in the suite; the passing path must
    stay exactly what it was."""
    instance = _Recorder(["lo"])
    assert _request(instance, stderr="", returncode=0, stdout="100\n").get_answer() == (
        "100\n"
    )
    assert _request(
        instance, stderr="", returncode=0, stdout="100\n"
    ).get_answer_and_error() == ("100\n", "")
    assert instance.probes == []


def test_a_request_built_without_the_gate_is_unchanged():
    """`CommandRequest` is also built directly - by `helpers/keeper_utils.py` and by
    tests - with nothing to consult. Those requests must behave as they always did."""
    request = CommandRequest(_failing_command(UNREACHABLE, 210), stdin="")
    with pytest.raises(QueryRuntimeException) as raised:
        request.get_answer()
    assert str(raised.value) == (
        f"Client failed! Return code: 210, stderr: {UNREACHABLE}"
    )


def test_a_client_built_against_a_node_address_consults_the_gate():
    """The gate is keyed on the address a request names, not on the client that carries
    it, so a `Client` a test constructs itself - `test_system_start_stop_listen`,
    `test_server_reload`, `test_introspection_port` and the two TLS modules all do - is
    covered without anything having wired it up. Drives the shipped `Client` over a
    command that fails the way `clickhouse-client` does."""
    calls, client = _recording_describer_and_client()
    error = client.get_query_request("SELECT 1", stdin="").get_error()
    assert calls == [(IP_ADDRESS, UNREACHABLE)]
    assert error == f"{CAUSE} {UNREACHABLE}"


def test_a_request_re_aimed_at_another_node_investigates_that_node():
    """`query(host=...)` sends the request to a different server, and it is that server
    that answered `No route to host`. Probing the instance the client happens to belong to
    would withhold the verdict for every such call site."""
    calls, client = _recording_describer_and_client()
    error = client.get_query_request(
        "SELECT 1", stdin="", host=OTHER_IP_ADDRESS
    ).get_error()
    assert calls == [(OTHER_IP_ADDRESS, UNREACHABLE)]
    assert error == f"{CAUSE} {UNREACHABLE}"
    # ... and the request really went there, rather than only being described as having.
    assert "--host" in client.command


def test_a_client_with_nothing_installed_is_unchanged():
    """`helpers/client.py` is driven without a cluster - by `helpers/keeper_utils.py` and
    by `test_random_inserts` - and must not need one."""
    client_module.set_transport_error_describer(None)
    client = _failing_client()
    assert client.get_query_request("SELECT 1", stdin="").get_error() == UNREACHABLE


def test_an_address_that_belongs_to_no_instance_is_not_explained():
    """Proxies, Keeper nodes and the loopback clients `test_server_reload` builds for a
    manually launched server all go through the same `Client`. None of them is a container
    this harness can probe, and inventing a verdict for them would relabel a real
    failure."""
    namespace = _registry()
    assert namespace["describe_transport_error_for_host"](OTHER_IP_ADDRESS, UNREACHABLE) == ""


def test_an_address_is_registered_and_released_with_the_instance():
    """The lookup is only as good as the mirror behind it: an instance has to appear in it
    when docker gives it an address and disappear when `shutdown` takes it away, or a
    later run would probe a container that no longer exists."""
    namespace = _registry()
    registry = namespace["_INSTANCES_BY_ADDRESS"]
    instance = namespace["Instance"]()
    assert instance.ip_address is None and registry == {}

    instance.ip_address = IP_ADDRESS
    assert registry == {IP_ADDRESS: instance}
    assert namespace["describe_transport_error_for_host"](IP_ADDRESS, UNREACHABLE) == (
        f"{instance.name} says so"
    )

    instance.ip_address = None
    assert registry == {}


def test_an_address_handed_on_to_another_container_follows_it():
    """Docker reuses addresses: a restarted container can be given the one a stopped
    container held. The entry has to name whoever holds it now, and releasing the old
    holder must not take the new one's entry with it."""
    namespace = _registry()
    registry = namespace["_INSTANCES_BY_ADDRESS"]
    old, new = namespace["Instance"](), namespace["Instance"]()
    old.ip_address = IP_ADDRESS
    new.ip_address = IP_ADDRESS
    assert registry == {IP_ADDRESS: new}
    old.ip_address = None
    assert registry == {IP_ADDRESS: new}


@pytest.mark.parametrize("entrypoint", HTTP_ENTRYPOINTS)
def test_every_http_entrypoint_goes_through_the_wrapper(entrypoint):
    """These are the only two places the harness talks HTTP to the server, and they do
    not go through `Client`. A direct `requests` call added back here would bypass the
    gate silently."""
    assert any(
        isinstance(node, ast.Attribute) and node.attr == HTTP
        for node in ast.walk(_func_ast(entrypoint))
    ), f"{entrypoint} does not go through {HTTP}"


def test_an_http_request_reports_the_cause_and_keeps_its_exception():
    """Tests catch `requests.exceptions.ConnectionError` and read its `request` and
    `response`, so only the message may change."""
    instance = _Recorder(["lo"])
    original = _connection_error(HTTP_UNREACHABLE)
    with pytest.raises(requests.exceptions.ConnectionError) as raised:
        instance._http_request_naming_transport_error(_raising(original))
    assert str(raised.value).startswith(
        instance.constants["LOST_NETWORK_INTERFACE_ERROR"]
    )
    assert HTTP_UNREACHABLE in str(raised.value)
    assert raised.value.request == "the-request"
    assert raised.value.response == "the-response"
    assert raised.value.__cause__ is original


def test_an_http_request_to_a_reachable_container_is_untouched():
    instance = _Recorder(["eth0", "lo"])
    original = _connection_error(HTTP_UNREACHABLE)
    with pytest.raises(requests.exceptions.ConnectionError) as raised:
        instance._http_request_naming_transport_error(_raising(original))
    assert raised.value is original


def test_an_http_connection_error_of_another_kind_is_not_investigated():
    """A refused connection is what a stopped server looks like over HTTP, and tests stop
    servers on purpose."""
    instance = _Recorder(["lo"])
    original = _connection_error("[Errno 111] Connection refused")
    with pytest.raises(requests.exceptions.ConnectionError) as raised:
        instance._http_request_naming_transport_error(_raising(original))
    assert raised.value is original
    assert instance.probes == []


def test_an_http_request_that_succeeds_is_returned():
    instance = _Recorder(["lo"])
    assert instance._http_request_naming_transport_error(lambda: "answer") == "answer"
    assert instance.probes == []


def test_the_marker_matches_the_one_the_ci_job_classifies_on():
    """The marker is a contract between the harness that emits it and the job that
    labels it an infrastructure error. Nothing links the two copies at runtime, so a
    reword on either side would silently turn the classification off."""
    name = "LOST_NETWORK_INTERFACE_ERROR"
    assert (
        _constants(CLUSTER_PY, [name])[name] == _constants(JOB_PY, [name])[name]
    ), f"{name} differs between {CLUSTER_PY} and {JOB_PY}"
