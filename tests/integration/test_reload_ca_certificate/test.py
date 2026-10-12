"""
Hot reload of the trusted CA certificates (`openSSL.*.caConfig`).

The certificates in certs/ are produced by certs/generate_certs.sh:
two unrelated root CAs (ca1, ca2) and one leaf certificate issued by each of them (cert1, cert2).
Every instance starts with ca.crt = ca1 as the trusted CA and node.crt/node.key = cert1 as its own certificate,
and the tests overwrite these files in the container to rotate them without restarting anything.
"""

import time
import uuid

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG_DIR = "/etc/clickhouse-server/config.d"
CERT_FILES = [
    "certs/ca.crt",
    "certs/ca1.crt",
    "certs/ca2.crt",
    "certs/node.crt",
    "certs/node.key",
    "certs/cert1.crt",
    "certs/cert1.key",
    "certs/cert2.crt",
    "certs/cert2.key",
]

# Serves HTTPS and acts as a TLS client towards itself.
node = cluster.add_instance("node", main_configs=["configs/ssl.xml"] + CERT_FILES)

# Does not set `loadDefaultCAFile`, and OpenSSL's default CA file is ca2 for this instance.
node_with_default_cas = cluster.add_instance(
    "node_with_default_cas",
    main_configs=["configs/ssl_with_default_cas.xml"] + CERT_FILES,
    env_variables={"SSL_CERT_FILE": f"{CONFIG_DIR}/ca2.crt"},
)

# Three nodes with embedded Keeper talking Raft over TLS. `loadDefaultCAFile` is not set for them: Keeper assumes `false` for
# the Raft connections then, unlike everything else, and their CA certificates have to be reloaded all the same.
keeper_nodes = [
    cluster.add_instance(f"node{i}", main_configs=[f"configs/keeper{i}.xml", "configs/ssl_with_default_cas.xml"] + CERT_FILES) for i in (1, 2, 3)
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_trusted_cas(instance, *cas):
    """Overwrite ca.crt (the configured `caConfig`) with the given CA certificates."""
    sources = " ".join(f"{CONFIG_DIR}/{ca}.crt" for ca in cas)
    instance.exec_in_container(["bash", "-c", f"cat {sources} > {CONFIG_DIR}/ca.crt.tmp && mv {CONFIG_DIR}/ca.crt.tmp {CONFIG_DIR}/ca.crt"])


def set_own_certificate(instance, cert):
    """Overwrite node.crt/node.key (the configured `certificateFile`/`privateKeyFile`) with the given leaf certificate."""
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"cp {CONFIG_DIR}/{cert}.crt {CONFIG_DIR}/node.crt.tmp && mv {CONFIG_DIR}/node.crt.tmp {CONFIG_DIR}/node.crt && "
            f"cp {CONFIG_DIR}/{cert}.key {CONFIG_DIR}/node.key.tmp && mv {CONFIG_DIR}/node.key.tmp {CONFIG_DIR}/node.key",
        ]
    )


@pytest.fixture(autouse=True)
def restore_certificates(started_cluster):
    yield
    for instance in [node, node_with_default_cas] + keeper_nodes:
        set_trusted_cas(instance, "ca1")
        set_own_certificate(instance, "cert1")
        instance.query("SYSTEM RELOAD CONFIG")
    for instance in keeper_nodes:
        kill_raft_connections(instance)
    ku.wait_nodes(cluster, keeper_nodes)


def https_request_with_client_certificate(cert, instance=node):
    """Query the HTTPS port of `instance` presenting the given client certificate. Returns the response, or None if the TLS handshake failed."""
    result = instance.exec_in_container(
        [
            "bash",
            "-c",
            f"curl --silent --show-error --cacert {CONFIG_DIR}/ca1.crt --cert {CONFIG_DIR}/{cert}.crt --key {CONFIG_DIR}/{cert}.key "
            f"'https://localhost:8443/?query=SELECT%201' 2>&1 || echo CURL_FAILED",
        ]
    )
    return None if "CURL_FAILED" in result else result


def assert_eventually(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    assert predicate(), description


def test_server_reloads_ca(started_cluster):
    """The CAs used to verify client certificates follow the content of `caConfig` without a restart or an explicit reload."""
    assert https_request_with_client_certificate("cert1") == "1\n"
    assert https_request_with_client_certificate("cert2") is None

    # Trust both CAs: the file change alone triggers the reload.
    set_trusted_cas(node, "ca1", "ca2")
    assert_eventually(lambda: https_request_with_client_certificate("cert2") == "1\n", "cert2 is accepted after ca2 was added")
    assert https_request_with_client_certificate("cert1") == "1\n"

    # Drop the old CA: certificates issued by it are not accepted anymore.
    set_trusted_cas(node, "ca2")
    assert_eventually(lambda: https_request_with_client_certificate("cert1") is None, "cert1 is rejected after ca1 was removed")
    assert https_request_with_client_certificate("cert2") == "1\n"

    assert node.contains_in_log("Reloaded CA certificates")


def test_client_reloads_ca(started_cluster):
    """The CAs used to verify server certificates of outgoing connections follow the content of `caConfig`."""
    # `node` connects to its own HTTPS port, which serves cert1. `Connection: close` rules out reusing a pooled connection.
    query = "SELECT * FROM url('https://localhost:8443/?query=SELECT%201', 'TSV', 'x UInt8', headers('Connection'='close'))"

    set_trusted_cas(node, "ca2")
    node.query("SYSTEM RELOAD CONFIG")
    error = node.query_and_get_error(query)
    assert "certificate verify failed" in error, error

    set_trusted_cas(node, "ca1")
    node.query("SYSTEM RELOAD CONFIG")
    assert node.query(query) == "1\n"


def test_default_cas_are_kept(started_cluster):
    """With `loadDefaultCAFile` left at its default, both the CAs from `caConfig` and the default ones are trusted, before and after a reload."""
    # cert1 is trusted through `caConfig` (ca.crt = ca1), cert2 through the default CA file (SSL_CERT_FILE = ca2).
    assert https_request_with_client_certificate("cert1", node_with_default_cas) == "1\n"
    assert https_request_with_client_certificate("cert2", node_with_default_cas) == "1\n"

    reloads = int(node_with_default_cas.count_in_log("Reloaded CA certificates").strip())
    set_trusted_cas(node_with_default_cas, "ca1")  # same content, new modification time
    node_with_default_cas.query("SYSTEM RELOAD CONFIG")
    assert int(node_with_default_cas.count_in_log("Reloaded CA certificates").strip()) > reloads

    assert https_request_with_client_certificate("cert1", node_with_default_cas) == "1\n"
    assert https_request_with_client_certificate("cert2", node_with_default_cas) == "1\n"


def test_system_certificates_follows_reload(started_cluster):
    """`system.certificates` shows the CA certificates that are currently used, also after `caConfig` is changed to another file."""
    query = "SELECT path, subject LIKE '%Test Root CA {}%' FROM system.certificates WHERE NOT default"
    assert node.query(query.format(1)) == f"{CONFIG_DIR}/ca.crt\t1\n"

    node.replace_in_config(f"{CONFIG_DIR}/ssl.xml", f"{CONFIG_DIR}/ca.crt", f"{CONFIG_DIR}/ca2.crt")
    try:
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query(query.format(2)) == f"{CONFIG_DIR}/ca2.crt\t1\n"
    finally:
        node.replace_in_config(f"{CONFIG_DIR}/ssl.xml", f"{CONFIG_DIR}/ca2.crt", f"{CONFIG_DIR}/ca.crt")
        node.query("SYSTEM RELOAD CONFIG")


def test_ca_directory(started_cluster):
    """`caConfig` can be a directory with certificates named by their subject hash. Replacing a certificate in it is noticed too."""
    ca_dir = f"{CONFIG_DIR}/ca_dir"
    node.exec_in_container(["bash", "-c", f"mkdir -p {ca_dir} && cp {CONFIG_DIR}/ca1.crt {ca_dir}/$(openssl x509 -noout -subject_hash -in {CONFIG_DIR}/ca1.crt).0"])
    node.replace_in_config(f"{CONFIG_DIR}/ssl.xml", f"{CONFIG_DIR}/ca.crt", ca_dir)
    try:
        node.query("SYSTEM RELOAD CONFIG")
        assert https_request_with_client_certificate("cert1") == "1\n"
        assert https_request_with_client_certificate("cert2") is None

        # Overwrite the only file in place: the set of file names in the directory does not change.
        node.exec_in_container(["bash", "-c", f"cat {CONFIG_DIR}/ca2.crt > {ca_dir}/$(openssl x509 -noout -subject_hash -in {CONFIG_DIR}/ca1.crt).0"])
        node.query("SYSTEM RELOAD CONFIG")
        assert https_request_with_client_certificate("cert1") is None
    finally:
        node.replace_in_config(f"{CONFIG_DIR}/ssl.xml", ca_dir, f"{CONFIG_DIR}/ca.crt")
        node.exec_in_container(["rm", "-rf", ca_dir])
        node.query("SYSTEM RELOAD CONFIG")


def kill_raft_connections(instance):
    instance.exec_in_container(
        ["bash", "-c", "ss --kill -tn state established '( dport = :9234 or sport = :9234 )' > /dev/null"], nothrow=True
    )


def raft_port_accepts_client_certificate(instance, target, cert):
    """Whether the Raft port of `target` completes a TLS handshake with a client that presents `cert`."""
    # With TLS 1.2 the server verifies the client certificate before the handshake completes on the client side,
    # so a rejected certificate reliably shows up as a failed handshake in s_client.
    result = instance.exec_in_container(
        [
            "bash",
            "-c",
            f"openssl s_client -brief -tls1_2 -connect {target.name}:9234 -cert {CONFIG_DIR}/{cert}.crt -key {CONFIG_DIR}/{cert}.key "
            f"</dev/null 2>&1 || true",
        ]
    )
    return "CONNECTION ESTABLISHED" in result


def raft_port_certificate_issuer(instance, target):
    return instance.exec_in_container(
        [
            "bash",
            "-c",
            f"openssl s_client -connect {target.name}:9234 </dev/null 2>/dev/null | openssl x509 -noout -issuer 2>/dev/null || true",
        ]
    ).strip()


def check_keeper_cluster_works(path):
    connections = []
    try:
        for instance in keeper_nodes:
            connections.append(ku.get_fake_zk(cluster, instance.name))
        connections[0].create(path, b"data")
        for connection in connections:
            connection.sync(path)
            assert connection.get(path)[0] == b"data"
    finally:
        for connection in connections:
            connection.stop()
            connection.close()


def reload_and_reconnect_raft():
    for instance in keeper_nodes:
        instance.query("SYSTEM RELOAD CONFIG")
    for instance in keeper_nodes:
        kill_raft_connections(instance)
    ku.wait_nodes(cluster, keeper_nodes)


def test_keeper_raft_reloads_ca(started_cluster):
    """Rotate the CA of the Raft connections between Keeper nodes without restarting them."""
    run = uuid.uuid4().hex
    ku.wait_nodes(cluster, keeper_nodes)
    check_keeper_cluster_works(f"/before_{run}")
    assert "Test Root CA 1" in raft_port_certificate_issuer(keeper_nodes[0], keeper_nodes[1])

    # 1. Trust the new CA in addition to the old one.
    for instance in keeper_nodes:
        set_trusted_cas(instance, "ca1", "ca2")
    reload_and_reconnect_raft()
    check_keeper_cluster_works(f"/both_cas_{run}")

    # 2. Switch the nodes to certificates issued by the new CA.
    for instance in keeper_nodes:
        set_own_certificate(instance, "cert2")
    reload_and_reconnect_raft()
    check_keeper_cluster_works(f"/new_certs_{run}")
    for target in keeper_nodes[1:]:
        assert "Test Root CA 2" in raft_port_certificate_issuer(keeper_nodes[0], target)

    # 3. Stop trusting the old CA.
    for instance in keeper_nodes:
        set_trusted_cas(instance, "ca2")
    reload_and_reconnect_raft()
    check_keeper_cluster_works(f"/new_ca_{run}")

    assert raft_port_accepts_client_certificate(keeper_nodes[0], keeper_nodes[1], "cert2")
    assert not raft_port_accepts_client_certificate(keeper_nodes[0], keeper_nodes[1], "cert1")
