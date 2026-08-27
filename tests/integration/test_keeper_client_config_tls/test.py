#!/usr/bin/env python3

import os

import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/enable_secure_keeper.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=False,
    use_keeper=False,
    stay_alive=True,
)

# Separate cluster for mTLS: Keeper requires a client certificate.
cluster_mtls = ClickHouseCluster(__file__)
node_mtls = cluster_mtls.add_instance(
    "node_mtls",
    main_configs=[
        "configs/enable_secure_keeper.xml",
        "configs/ssl_conf_mtls.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
        "configs/client.crt",
        "configs/client.key",
        "configs/client.key.enc",
        "configs/client.combined.pem",
    ],
    with_zookeeper=False,
    use_keeper=False,
    stay_alive=True,
)

SECURE_PORT = 10181
PLAIN_PORT = 9181

# Paths where client configs are copied inside the container.
CONFIG_SSL_WITH_CA = "/tmp/keeper_client_ssl_ca.xml"
CONFIG_SSL_NO_CA = "/tmp/keeper_client_ssl_no_ca.xml"
CONFIG_ZK_SECURE_NODE_WITH_CA = "/tmp/keeper_client_zk_secure_node_ca.xml"
CONFIG_ZK_SECURE_NODE_NO_CA = "/tmp/keeper_client_zk_secure_node_no_ca.xml"
CONFIG_MTLS_WITH_CERT = "/tmp/keeper_client_mtls_with_cert.xml"
CONFIG_MTLS_NO_CERT = "/tmp/keeper_client_mtls_no_cert.xml"
CONFIG_MTLS_ENCRYPTED_KEY = "/tmp/keeper_client_mtls_encrypted_key.xml"
CONFIG_MTLS_COMBINED_PEM = "/tmp/keeper_client_mtls_combined_pem.xml"

# The server cert is self-signed, so it doubles as its own CA.
CA_PATH = "/etc/clickhouse-server/config.d/server.crt"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node1)

        for name, dest in (
            ("keeper_client_ssl_ca.xml", CONFIG_SSL_WITH_CA),
            ("keeper_client_ssl_no_ca.xml", CONFIG_SSL_NO_CA),
            ("keeper_client_zk_secure_node_ca.xml", CONFIG_ZK_SECURE_NODE_WITH_CA),
            ("keeper_client_zk_secure_node_no_ca.xml", CONFIG_ZK_SECURE_NODE_NO_CA),
        ):
            node1.copy_file_to_container(
                os.path.join(os.path.dirname(__file__), "configs", name), dest
            )

        yield cluster
    finally:
        cluster.shutdown()


def run_keeper_client(args: str, query: str = "ls '/keeper'", nothrow: bool = False) -> str:
    """Invoke keeper-client pointing at the TLS port; `args` is appended after --secure."""
    return node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure {args} -q \"{query}\"",
        ],
        privileged=True,
        nothrow=nothrow,
    )


def run_keeper_client_from_config(config_path: str, query: str = "ls '/keeper'", nothrow: bool = False) -> str:
    """Invoke keeper-client using a zookeeper-node config; host/port/TLS come entirely from the file."""
    return node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -c {config_path} -q \"{query}\"",
        ],
        privileged=True,
        nothrow=nothrow,
    )


# Group A — <openSSL><client> config-file TLS (CLI --secure flag, CA from config)

def test_ca_from_config_file(started_cluster):
    """--secure without --tls-* reads the CA from <openSSL><client><caConfig> in the config file."""
    data = run_keeper_client(f"-c {CONFIG_SSL_WITH_CA}")
    assert "api_version" in data


def test_missing_ca_in_config_fails(started_cluster):
    """--secure with a config that has no caConfig cannot verify the self-signed server certificate."""
    data = run_keeper_client(f"-c {CONFIG_SSL_NO_CA}", nothrow=True)
    assert "api_version" not in data


def test_ca_from_cli_flag(started_cluster):
    """Explicit --tls-ca-file still works and takes precedence over the config file."""
    data = run_keeper_client(f"--tls-ca-file {CA_PATH}")
    assert "api_version" in data


def test_cli_ca_flag_overrides_config_ca(started_cluster):
    """When both a config file (no caConfig) and --tls-ca-file are given, the CLI flag wins."""
    data = run_keeper_client(f"-c {CONFIG_SSL_NO_CA} --tls-ca-file {CA_PATH}")
    assert "api_version" in data


def test_accept_invalid_certificate(started_cluster):
    """--accept-invalid-certificate bypasses certificate verification entirely, even with no CA."""
    data = run_keeper_client("--accept-invalid-certificate")
    assert "api_version" in data


# Group B — <zookeeper><node><secure>1</secure> config-file TLS (no --secure CLI flag)

def test_secure_node_in_zookeeper_config_with_ca(started_cluster):
    """A <zookeeper><node> with <secure>1</secure> triggers TLS; the CA comes from <openSSL><client><caConfig>."""
    data = run_keeper_client_from_config(CONFIG_ZK_SECURE_NODE_WITH_CA)
    assert "api_version" in data


def test_secure_node_in_zookeeper_config_without_ca_fails(started_cluster):
    """A <zookeeper><node> with <secure>1</secure> but no caConfig fails certificate verification."""
    data = run_keeper_client_from_config(CONFIG_ZK_SECURE_NODE_NO_CA, nothrow=True)
    assert "api_version" not in data


# Group C — Four-letter-word commands over TLS

def test_four_letter_ruok_over_tls_cli(started_cluster):
    """ruok returns imok when the TLS context comes from the CLI --tls-ca-file flag."""
    data = run_keeper_client(f"--tls-ca-file {CA_PATH}", query="ruok")
    assert data.strip() == "imok"


def test_four_letter_mntr_over_tls_cli(started_cluster):
    """mntr returns keeper metrics when TLS is configured via --tls-ca-file."""
    data = run_keeper_client(f"--tls-ca-file {CA_PATH}", query="mntr")
    assert "zk_version" in data


def test_four_letter_ruok_over_tls_from_config(started_cluster):
    """ruok returns imok when TLS is triggered by <zookeeper><node><secure>1</secure> and CA from config."""
    data = run_keeper_client_from_config(CONFIG_ZK_SECURE_NODE_WITH_CA, query="ruok")
    assert data.strip() == "imok"


def test_four_letter_ruok_accept_invalid_certificate(started_cluster):
    """ruok succeeds when --accept-invalid-certificate is used, even with no CA provided."""
    data = run_keeper_client("--accept-invalid-certificate", query="ruok")
    assert data.strip() == "imok"


# Group D — Client certificate authentication (mTLS)
#
# Keeper is configured with verificationMode=strict so it demands a client cert.
# The keeper-client gets its cert/key exclusively from <openSSL><client> in the
# config file — no CLI flags. privateKeyPassphraseHandler.name is intentionally
# absent, which exercises the default KeyConsoleHandler path (no prompt fires
# because the key is unencrypted).

@pytest.fixture(scope="module")
def started_cluster_mtls():
    try:
        cluster_mtls.start()
        keeper_utils.wait_until_connected(cluster_mtls, node_mtls)

        for name, dest in (
            ("keeper_client_mtls_with_cert.xml", CONFIG_MTLS_WITH_CERT),
            ("keeper_client_mtls_no_cert.xml", CONFIG_MTLS_NO_CERT),
            ("keeper_client_mtls_encrypted_key.xml", CONFIG_MTLS_ENCRYPTED_KEY),
            ("keeper_client_mtls_combined_pem.xml", CONFIG_MTLS_COMBINED_PEM),
        ):
            node_mtls.copy_file_to_container(
                os.path.join(os.path.dirname(__file__), "configs", name), dest
            )

        yield cluster_mtls
    finally:
        cluster_mtls.shutdown()


def test_client_cert_from_config_connects(started_cluster_mtls):
    """Client cert/key from <openSSL><client> satisfies Keeper's strict client-auth requirement."""
    data = node_mtls.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure -c {CONFIG_MTLS_WITH_CERT} -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert "api_version" in data


def test_missing_client_cert_rejected_by_mtls_keeper(started_cluster_mtls):
    """Keeper with strict client-auth rejects a connection that presents no client certificate."""
    data = node_mtls.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure -c {CONFIG_MTLS_NO_CERT} -q \"ls '/keeper'\"",
        ],
        privileged=True,
        nothrow=True,
    )
    assert "api_version" not in data


def test_encrypted_key_with_keyfilehandler(started_cluster_mtls):
    """privateKeyPassphraseHandler.name=KeyFileHandler decrypts the key using the password from the config."""
    data = node_mtls.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure -c {CONFIG_MTLS_ENCRYPTED_KEY} -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert "api_version" in data


def test_combined_pem_cert_key_in_single_file(started_cluster_mtls):
    """When certificateFile is absent, tls_cert_file falls back to privateKeyFile (combined-PEM path)."""
    data = node_mtls.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure -c {CONFIG_MTLS_COMBINED_PEM} -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert "api_version" in data
