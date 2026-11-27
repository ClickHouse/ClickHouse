"""When neither `port` nor `secure`/`no-secure` is specified, clickhouse-client probes both the
plain (9000) and the secure (9440) native ports concurrently and uses the one that answers first.
A server that listens on both ports is reachable either way, so either port will do; what matters
is that a server that answers on one port only is connected to without waiting out the connection
timeout of the other one (e.g. play.clickhouse.com, whose plain port is firewalled), and that TLS
chosen this way is not turned into a failure when the secure port turns out to be unusable."""

import ipaddress
import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Serves both the plain and the secure native port (with a self-signed certificate).
node_both_ports = cluster.add_instance(
    "node_both_ports",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/self-cert.pem",
        "certs/self-key.pem",
        "certs/ca-cert.pem",
    ],
    stay_alive=True,
)

# Serves only the plain port; also used to run the client from, so that the firewall rules
# on node_both_ports can be scoped to this instance's address and not break the test harness.
node_plain_only = cluster.add_instance("node_plain_only")

# Another plain-only server: the second address for the tests where one host resolves to
# several addresses and the connection falls through from a dead address to a working one.
node_extra = cluster.add_instance("node_extra")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def firewall_plain_port(action):
    """Reject or drop packets from node_plain_only to the plain port of node_both_ports."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            action,
        ],
        user="root",
    )


def unfirewall_plain_port(action):
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-D",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            action,
        ],
        user="root",
    )


def redirect_plain_port_to_secure(add, source=None):
    """Redirect connections from `source` (node_plain_only by default) to the plain port (9000)
    of node_both_ports to its secure port (9440). A native (non-TLS) connection to the plain port
    is then answered by the TLS listener, simulating a proxy that accepts TCP on the plain port but
    only serves TLS there."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-t",
            "nat",
            "-A" if add else "-D",
            "PREROUTING",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            (source or node_plain_only).ip_address,
            "-j",
            "REDIRECT",
            "--to-ports",
            "9440",
        ],
        user="root",
    )


def firewall_secure_port(add):
    """Reset connections from node_plain_only to the secure port (9440) of node_both_ports.
    Unlike a plain REJECT, `tcp-reset` also tears down the already established connections
    to that port, so an in-flight query fails and the client has to reconnect."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-t",
            "filter",
            "-A" if add else "-D",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "9440",
            "-s",
            node_plain_only.ip_address,
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        ],
        user="root",
    )


def run_client(server, *args, from_node=None, nothrow=False):
    from_node = from_node or node_plain_only
    return from_node.exec_in_container(
        ["clickhouse", "client", "--host", server.name, "--accept-invalid-certificate"]
        + list(args),
        nothrow=nothrow,
    )


def query_is_secure(server, *args, from_node=None):
    """Runs `SELECT 1` through the client without specifying a port and returns whether the
    connection was established over TLS."""
    query_id = str(uuid.uuid4())
    result = run_client(
        server,
        "--query_id",
        query_id,
        "--query",
        "SELECT 1",
        *args,
        from_node=from_node,
    )
    assert result == "1\n"
    server.query("SYSTEM FLUSH LOGS query_log")
    return int(
        server.query(
            f"SELECT is_secure FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' LIMIT 1"
        )
    )


def test_either_port_when_both_listen():
    # Both ports answer, so whichever of them the probe sees first is used: both work, and waiting to
    # find out whether the other one answers too would only delay the connection.
    assert query_is_secure(node_both_ports) in (0, 1)


def test_plain_port_chosen_when_only_plain_listens():
    assert query_is_secure(node_plain_only, from_node=node_both_ports) == 0


def test_explicit_secure_still_works():
    assert query_is_secure(node_both_ports, "--secure") == 1


def test_secure_port_chosen_when_plain_rejected():
    firewall_plain_port("REJECT")
    try:
        assert query_is_secure(node_both_ports) == 1
    finally:
        unfirewall_plain_port("REJECT")


def test_secure_port_chosen_when_plain_dropped():
    # Packets to the plain port silently disappear, as with a typical cloud firewall
    # (this is how play.clickhouse.com behaves). The client must not wait for the plain
    # connection attempt to time out: the ports are probed concurrently, so the whole
    # query has to finish well within the 10 seconds connect timeout.
    firewall_plain_port("DROP")
    try:
        start = time.time()
        assert query_is_secure(node_both_ports) == 1
        assert time.time() - start < 8
    finally:
        unfirewall_plain_port("DROP")


def test_secure_port_chosen_when_plain_serves_tls():
    # A proxy in front of the server accepts TCP on the plain port but only speaks TLS there. Both
    # probes see their TCP connection accepted, so either port may be chosen, and the connection ends
    # up over TLS either way: directly when the secure port answered first, and after the native
    # protocol fails against the TLS listener when it was the plain port.
    redirect_plain_port_to_secure(add=True)
    try:
        assert query_is_secure(node_both_ports) == 1
    finally:
        redirect_plain_port_to_secure(add=False)


def swallow_established_packets(port, add):
    """Let the TCP handshake through, but drop everything that is sent afterwards, so that the
    connection to this port is accepted and then never answered, as by an unresponsive server."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A" if add else "-D",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            str(port),
            "-s",
            node_plain_only.ip_address,
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED",
            "-j",
            "DROP",
        ],
        user="root",
    )


def test_an_unresponsive_server_is_not_waited_for_twice():
    # A server that accepts the connection on the plain port and then does not answer is not a TLS
    # listener on the plain port: it is simply unresponsive, and its secure port is not going to
    # answer either. Retrying it there would double the time the client waits before it reports the
    # failure, which is what the automatic choice is supposed to avoid in the first place. The
    # secure port refuses connections outright here, so the probe chooses the plain port with the
    # secure port as the protocol-level fallback candidate; the timeout on the plain port must not
    # trigger that fallback (only a protocol-level failure does). The timeouts are lowered through a
    # client configuration file, because the connection timeouts are read from the configuration and
    # not from the settings.
    node_plain_only.exec_in_container(
        [
            "bash",
            "-c",
            "printf '<clickhouse><send_timeout>5</send_timeout><receive_timeout>5</receive_timeout>"
            "<handshake_timeout_ms>5000</handshake_timeout_ms></clickhouse>' > /tmp/timeouts.xml",
        ],
    )
    swallow_established_packets(9000, add=True)
    firewall_secure_port(add=True)
    try:
        output = node_plain_only.exec_in_container(
            [
                "bash",
                "-c",
                f"clickhouse client --host {node_both_ports.name} --accept-invalid-certificate"
                " --config-file /tmp/timeouts.xml --query 'SELECT 1' 2>&1 || true",
            ]
        )
        assert "SOCKET_TIMEOUT" in output, output
        assert "also failed to connect" not in output, output
    finally:
        firewall_secure_port(add=False)
        swallow_established_packets(9000, add=False)


def test_automatic_choice_is_not_applied_to_the_other_addresses():
    # The port and the TLS mode chosen automatically for one address must be remembered for that
    # address only. Here the client first connects to `node_both_ports`, whose plain port is
    # firewalled, so TLS on the secure port is chosen automatically. Then that server is killed, and
    # the client fails over to `node_plain_only`, which listens on the plain port only: the choice has
    # to be made for it from scratch. If the detected port and TLS mode were kept in the global
    # configuration instead, the client would try the plain-only address on the secure port with TLS
    # and give up, even though its plain port is healthy.
    firewall_plain_port("REJECT")

    def kill_the_first_server():
        time.sleep(5)
        node_both_ports.stop_clickhouse(kill=True)

    killer = threading.Thread(target=kill_the_first_server)
    killer.start()
    try:
        # The first query runs long enough to be interrupted by the kill; `--ignore-error` makes the
        # client proceed to the second query, which reconnects, failing over to the second address.
        output = node_plain_only.exec_in_container(
            [
                "bash",
                "-c",
                f"clickhouse client --host {node_both_ports.name} --host {node_plain_only.name}"
                " --accept-invalid-certificate --ignore-error --max_block_size 1 --max_threads 1"
                " --query \"SELECT sleepEachRow(1) FROM numbers(60) FORMAT Null; SELECT 'reconnected'\" 2>&1 || true",
            ]
        )
        assert "reconnected" in output
    finally:
        killer.join()
        unfirewall_plain_port("REJECT")
        node_both_ports.start_clickhouse()


def test_automatic_choice_is_forgotten_after_a_failed_connection():
    # The automatic choice is remembered for the address it was made for, so that a reconnect does not
    # probe the ports again. But it is only valid while the same endpoints stay reachable: the host can
    # re-resolve to another backend (`Connection::connect` drops the DNS cache entries for it after a
    # connect-level failure) that serves the other port. Here the plain port is unreachable at first, so
    # TLS on the secure port is chosen; then the transport changes underneath the session: the secure
    # port starts resetting connections (which also breaks the running query) and the plain port becomes
    # reachable. The client must forget the remembered secure port and rediscover the healthy plain one,
    # instead of retrying TLS forever.
    firewall_plain_port("REJECT")
    plain_firewalled = True

    def change_the_transport():
        nonlocal plain_firewalled
        time.sleep(5)
        firewall_secure_port(add=True)
        unfirewall_plain_port("REJECT")
        plain_firewalled = False

    switcher = threading.Thread(target=change_the_transport)
    switcher.start()
    try:
        # The first query is interrupted by the reset of the secure connection; `--ignore-error` makes
        # the client proceed to the next queries, each of which reconnects.
        output = node_plain_only.exec_in_container(
            [
                "bash",
                "-c",
                f"clickhouse client --host {node_both_ports.name}"
                " --accept-invalid-certificate --ignore-error --max_block_size 1 --max_threads 1"
                " --query \"SELECT sleepEachRow(1) FROM numbers(60) FORMAT Null;"
                " SELECT 'first-retry'; SELECT 'reconnected'\" 2>&1 || true",
            ]
        )
        assert "reconnected" in output
        assert query_is_secure(node_both_ports) == 0
    finally:
        switcher.join()
        firewall_secure_port(add=False)
        if plain_firewalled:
            unfirewall_plain_port("REJECT")


def probe_client_and_blackhole():
    """A client instance and an address for the black hole below. The resolver sorts the addresses
    of a host by the longest prefix they share with the source address (RFC 3484 rule 9), and the
    tests need the black hole sorted deterministically in front of the address of the server, so
    the black hole has to share a strictly longer prefix with the client than the server does. When
    the server is the closest possible neighbour of the client (their addresses differ in the
    lowest bit only), no such black hole exists, so the client is chosen between two instances:
    the server cannot be the closest neighbour of both."""
    server = int(ipaddress.IPv4Address(node_both_ports.ip_address))
    other_instances = {
        int(ipaddress.IPv4Address(node_plain_only.ip_address)),
        int(ipaddress.IPv4Address(node_extra.ip_address)),
    }
    # The black hole does not have to be a free address: the packets to it are dropped before they
    # leave the client, so it may be the address of a live container, as long as it is not the
    # server the test connects to. A free address is still preferred when there is a choice.
    for allow_other_instances in (False, True):
        for client in (node_plain_only, node_extra):
            client_address = int(ipaddress.IPv4Address(client.ip_address))
            server_prefix = 32 - (client_address ^ server).bit_length()
            # A candidate beats the server when it agrees with the client in more than
            # `server_prefix` leading bits, i.e. differs from it only below that.
            for delta in range(1, 1 << max(0, 31 - server_prefix)):
                candidate = client_address ^ delta
                # The lowest and the highest address of a subnet, and the conventional address of
                # the gateway (the network address plus one), are never used as the black hole.
                if candidate & 0xFF in (0, 1, 255) or candidate == server:
                    continue
                if candidate in other_instances and not allow_other_instances:
                    continue
                return client, str(ipaddress.IPv4Address(candidate))
    raise RuntimeError("Cannot find an address for the black hole")


def assert_the_black_hole_is_resolved_first(client, hostname, blackhole):
    """The elapsed-time assertions below prove nothing when the healthy address is sorted in front
    of the black hole, so check the order the resolver actually returns instead of relying on the
    reasoning in probe_client_and_blackhole alone."""
    resolved = client.exec_in_container(["getent", "ahosts", hostname])
    assert resolved.split()[0] == blackhole, (
        f"The resolver has to return the black hole {blackhole} first:\n{resolved}"
    )


def blackhole_address(client, address, add):
    """Silently drop everything the client sends to this address, so that connecting to it stalls
    until the connection timeout instead of failing right away."""
    client.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A" if add else "-D",
            "OUTPUT",
            "-d",
            address,
            "-j",
            "DROP",
        ],
        user="root",
    )


def test_the_probed_address_is_used_for_the_connection():
    # A host can resolve to several addresses, and the connection tries them one by one, so every
    # unresponsive address in front of the list costs a whole connection timeout. The probing knows
    # which address has answered, and the connection has to start with it: otherwise the automatic
    # choice reintroduces exactly the timeout it is supposed to avoid.
    #
    # Here `multiaddress` resolves to a black hole first and to a healthy server second, and the
    # connect timeout is raised to 60 seconds, so the query cannot possibly finish in time unless
    # the address that answered during the probing is the one the client connects to.
    #
    # The session is then broken and re-established in the same client process: a reconnect does not
    # probe the ports again, so the address that answered has to be remembered along with the port
    # and the TLS mode, or the second connection walks the addresses from the start again.
    #
    # An `INSERT` that the server rejects while it receives the data makes the client disconnect
    # (the protocol is out of sync at that point), and `--ignore-error` lets it proceed to the next
    # query, which reconnects. This needs no help from the network or from the server process, so
    # the reconnect happens at a well-defined moment.
    node_both_ports.query(
        "CREATE TABLE IF NOT EXISTS reconnect_trigger (x UInt8, CONSTRAINT c CHECK x < 5)"
        " ENGINE = Memory"
    )
    client, blackhole = probe_client_and_blackhole()
    blackhole_address(client, blackhole, add=True)
    client.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s multiaddress\\n%s multiaddress\\n' {blackhole} {node_both_ports.ip_address} >> /etc/hosts",
        ],
        user="root",
    )
    try:
        assert_the_black_hole_is_resolved_first(client, "multiaddress", blackhole)
        start = time.time()
        output = client.exec_in_container(
            [
                "bash",
                "-c",
                "clickhouse client --host multiaddress --accept-invalid-certificate"
                " --connect_timeout 60 --async_insert 0 --ignore-error"
                " --query \"SELECT 'ready';"
                " INSERT INTO reconnect_trigger VALUES (10);"
                " SELECT 'reconnected'\" < /dev/null 2>&1 || true",
            ]
        )
        elapsed = time.time() - start
        assert "ready" in output, output
        assert "VIOLATED_CONSTRAINT" in output, output
        assert "reconnected" in output, output
        assert elapsed < 30, f"Connecting took {elapsed} seconds: {output}"
    finally:
        node_both_ports.query("DROP TABLE IF EXISTS reconnect_trigger")
        # `/etc/hosts` is bind-mounted into the container, so it can only be rewritten in place:
        # `sed -i` renames its temporary file over it and fails with `Device or resource busy`.
        client.exec_in_container(
            [
                "bash",
                "-c",
                "grep -v multiaddress /etc/hosts > /tmp/hosts && cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
            ],
            user="root",
        )
        blackhole_address(client, blackhole, add=False)


def firewall_plain_port_of(server, add, action, extra=()):
    """Apply an iptables rule on `server` to packets from node_plain_only to its plain port."""
    server.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A" if add else "-D",
            "INPUT",
            "-p",
            "tcp",
            *extra,
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            action,
        ],
        user="root",
    )


def test_the_remembered_address_is_refreshed_when_the_connection_falls_through():
    # The address that answered is remembered so that a reconnect does not walk the resolved
    # addresses from the start. But the remembered address can die, and `Connection::connect`
    # then falls through to another resolved address of the same host and succeeds there. The
    # remembered address has to be refreshed after every successful connect, and not only when
    # the ports are probed: otherwise it stays stale forever, and every following reconnect
    # waits out a whole connection timeout on the dead address again, even though the previous
    # reconnect already learned the working one.
    #
    # Here `refreshedhost` resolves to `node_both_ports` and `node_extra`. The secure port of
    # `node_both_ports` is rejected for the whole test, so that the automatic choice settles on
    # the plain transport, which both servers speak. The first connection goes to
    # `node_both_ports` (the plain port of the other one is rejected during the probing). Then new
    # connections to it start being silently dropped (the established session is not affected)
    # and `node_extra` becomes reachable. An `INSERT` rejected by a constraint makes the client
    # disconnect and reconnect on the next query (see the test above): the first reconnect pays
    # one connection timeout on the dead address and falls through to `node_extra`; the second
    # reconnect has to go to `node_extra` right away, without waiting out the timeout again.
    for server in (node_both_ports, node_extra):
        server.query(
            "CREATE TABLE IF NOT EXISTS refresh_trigger (x UInt8, CONSTRAINT c CHECK x < 5)"
            " ENGINE = Memory"
        )
    node_plain_only.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s refreshedhost\\n%s refreshedhost\\n' {node_both_ports.ip_address} {node_extra.ip_address} >> /etc/hosts",
        ],
        user="root",
    )
    firewall_secure_port(add=True)
    firewall_plain_port_of(node_extra, add=True, action="REJECT")
    extra_rejected = True
    first_address_dropped = False

    def swap_the_addresses():
        nonlocal extra_rejected, first_address_dropped
        time.sleep(5)
        # New connections stall (as with a typical cloud firewall); the established one lives on.
        firewall_plain_port_of(node_both_ports, add=True, action="DROP", extra=("--syn",))
        first_address_dropped = True
        firewall_plain_port_of(node_extra, add=False, action="REJECT")
        extra_rejected = False

    switcher = threading.Thread(target=swap_the_addresses)
    switcher.start()
    try:
        start = time.time()
        output = node_plain_only.exec_in_container(
            [
                "bash",
                "-c",
                "clickhouse client --host refreshedhost --accept-invalid-certificate"
                " --connect_timeout 15 --async_insert 0 --ignore-error"
                " --max_block_size 1 --max_threads 1"
                " --query \"SELECT sleepEachRow(1) FROM numbers(8) FORMAT Null;"
                " INSERT INTO refresh_trigger VALUES (10);"
                " SELECT 'in-between';"
                " INSERT INTO refresh_trigger VALUES (10);"
                " SELECT 'reconnected'\" < /dev/null 2>&1 || true",
            ]
        )
        elapsed = time.time() - start
        assert "in-between" in output, output
        assert "reconnected" in output, output
        # One connection timeout is unavoidable: the first reconnect discovers that the remembered
        # address is dead. The second one must not pay it again (~24 s against ~39 s if it does).
        assert elapsed < 32, f"The queries took {elapsed} seconds: {output}"
    finally:
        switcher.join()
        if first_address_dropped:
            firewall_plain_port_of(node_both_ports, add=False, action="DROP", extra=("--syn",))
        if extra_rejected:
            firewall_plain_port_of(node_extra, add=False, action="REJECT")
        firewall_secure_port(add=False)
        # `/etc/hosts` is bind-mounted into the container, so it can only be rewritten in place.
        node_plain_only.exec_in_container(
            [
                "bash",
                "-c",
                "grep -v refreshedhost /etc/hosts > /tmp/hosts && cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
            ],
            user="root",
        )
        for server in (node_both_ports, node_extra):
            server.query("DROP TABLE IF EXISTS refresh_trigger")


def test_an_untrusted_certificate_falls_back_to_the_plain_port():
    # The certificate of `node_both_ports` is self-signed, so a client that does not pass
    # `--accept-invalid-certificate` (as every other test here does) rejects it. TLS was not asked for,
    # it was chosen automatically whenever the secure port answered the probe first, so this must not be
    # an error: the client falls back to the plain port, which is what it would have connected to if
    # there were no automatic choice at all. Self-signed certificates on the secure port of a server
    # that also serves the plain port are common, and such deployments have to keep working - so the
    # connection ends up on the plain port either way, whichever port answered first.
    query_id = str(uuid.uuid4())
    result = node_plain_only.exec_in_container(
        [
            "clickhouse",
            "client",
            "--host",
            node_both_ports.name,
            "--query_id",
            query_id,
            "--query",
            "SELECT 1",
        ]
    )
    assert result == "1\n"
    node_both_ports.query("SYSTEM FLUSH LOGS query_log")
    assert (
        int(
            node_both_ports.query(
                f"SELECT is_secure FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' LIMIT 1"
            )
        )
        == 0
    )


def test_the_fallback_keeps_the_address_the_probe_reached():
    # The fallback from an unusable secure port must not start over from the first address the host
    # resolves to: the probe already found which address answers, and walking the resolved addresses
    # again costs a whole connection timeout for every unresponsive one in front of it - exactly the
    # delay the probing exists to avoid.
    #
    # `multiaddress` resolves to a black hole first and to the dual-port server second, the connect
    # timeout is raised to 60 seconds, and the certificate is not accepted, so when the secure port is
    # the one that answered first, the client has TLS rejected and has to reach the plain port of the
    # address it already probed. (When the plain port answered first, the query simply runs on it: the
    # elapsed time then proves the same property for the address that answered.)
    client, blackhole = probe_client_and_blackhole()
    blackhole_address(client, blackhole, add=True)
    client.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s multiaddress\\n%s multiaddress\\n' {blackhole} {node_both_ports.ip_address} >> /etc/hosts",
        ],
        user="root",
    )
    try:
        assert_the_black_hole_is_resolved_first(client, "multiaddress", blackhole)
        start = time.time()
        output = client.exec_in_container(
            [
                "bash",
                "-c",
                "clickhouse client --host multiaddress --connect_timeout 60"
                " --query \"SELECT 'fell-back'\" < /dev/null 2>&1 || true",
            ]
        )
        elapsed = time.time() - start
        assert "fell-back" in output, output
        assert elapsed < 30, f"Connecting took {elapsed} seconds: {output}"
    finally:
        # `/etc/hosts` is bind-mounted into the container, so it can only be rewritten in place.
        client.exec_in_container(
            [
                "bash",
                "-c",
                "grep -v multiaddress /etc/hosts > /tmp/hosts && cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
            ],
            user="root",
        )
        blackhole_address(client, blackhole, add=False)


def test_the_probe_leaves_no_extra_connection_on_the_plain_port():
    # The connection the probe established to the chosen port is handed over to the client instead of
    # being discarded and opened again. Otherwise every automatically detected connection would leave a
    # short-lived session on the server that sends no data, which the server logs as
    # `Client has not sent any data.` and counts against `max_connections`.
    #
    # `node_plain_only` serves the plain port only, which is the common case: the secure probe is
    # refused, and the connection of the plain probe becomes the connection of the client.
    marker = "Client has not sent any data"
    before = int(node_plain_only.count_in_log(marker))
    assert (
        run_client(node_plain_only, "--query", "SELECT 1", from_node=node_both_ports)
        == "1\n"
    )
    assert int(node_plain_only.count_in_log(marker)) == before


def test_the_probe_leaves_no_extra_connection_on_the_other_addresses():
    # The addresses of a port are attempted one at a time, so a host that resolves to several reachable
    # backends - a load balancer, most typically - is connected to on one of them only. Connecting to all
    # of them at once would leave a session that sends nothing on every backend but the winner, on every
    # single automatically detected connect.
    #
    # `twohealthy` resolves to both plain-only servers, and neither may end up with such a session.
    marker = "Client has not sent any data"
    before = {
        server.name: int(server.count_in_log(marker))
        for server in (node_plain_only, node_extra)
    }
    node_both_ports.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s twohealthy\\n%s twohealthy\\n' {node_plain_only.ip_address} {node_extra.ip_address} >> /etc/hosts",
        ],
        user="root",
    )
    try:
        assert (
            node_both_ports.exec_in_container(
                ["clickhouse", "client", "--host", "twohealthy", "--query", "SELECT 1"]
            )
            == "1\n"
        )
        for server in (node_plain_only, node_extra):
            assert int(server.count_in_log(marker)) == before[server.name], (
                f"{server.name} was left with a connection that sends nothing"
            )
    finally:
        # `/etc/hosts` is bind-mounted into the container, so it can only be rewritten in place.
        node_both_ports.exec_in_container(
            [
                "bash",
                "-c",
                "grep -v twohealthy /etc/hosts > /tmp/hosts && cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
            ],
            user="root",
        )


def test_explicit_port_is_not_upgraded():
    # With an explicit port or an explicit `no-secure` there is no automatic choice.
    firewall_plain_port("REJECT")
    try:
        for extra_args in ("--port 9000", "--no-secure"):
            output = node_plain_only.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"clickhouse client --host {node_both_ports.name} {extra_args} --query 'SELECT 1' 2>&1 || true",
                ]
            )
            assert output.strip() != "1"
            assert "refused" in output.lower()
    finally:
        unfirewall_plain_port("REJECT")
