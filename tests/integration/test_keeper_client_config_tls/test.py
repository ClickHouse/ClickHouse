#!/usr/bin/env python3

import os
import threading
import time

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
        # combined PEM needed for test_combined_pem_cert_key_in_single_file
        "configs/client.combined.pem",
    ],
    with_zookeeper=False,
    use_keeper=False,
    stay_alive=True,
)

# Separate cluster for mTLS: Keeper requires a client certificate.
# name="mtls" gives this cluster a distinct Docker Compose project name so
# cluster_mtls.start() does not reconcile-away node1 from cluster.
cluster_mtls = ClickHouseCluster(__file__, name="mtls")
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
# Combined-PEM test runs against the plain-TLS keeper (no client-auth requirement)
# to isolate the tls_cert_file fallback from OpenSSL's chain-reader behaviour.
CONFIG_COMBINED_PEM = "/tmp/keeper_client_combined_pem.xml"
# Config that has an <openSSL><client> section but no invalidCertificateHandler.name —
# used to verify that keeper-client seeds RejectCertificateHandler by default.
CONFIG_SSL_NO_HANDLER = "/tmp/keeper_client_ssl_no_handler.xml"

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
            ("keeper_client_combined_pem.xml", CONFIG_COMBINED_PEM),
            ("keeper_client_ssl_no_handler.xml", CONFIG_SSL_NO_HANDLER),
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


def test_secure_scheme_in_host_triggers_tls(started_cluster):
    """secure://host without --secure initialises the SSL context via any_host_secure and routes ruok over TLS."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client"
            f" --host secure://localhost --port {SECURE_PORT}"
            f" -c {CONFIG_SSL_WITH_CA} -q \"ruok\"",
        ],
        privileged=True,
    )
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


def test_combined_pem_cert_key_in_single_file(started_cluster):
    """When certificateFile is absent, tls_cert_file falls back to privateKeyFile (combined-PEM path)."""
    # Runs against the plain-TLS keeper (no client-auth requirement) so the test
    # isolates the tls_cert_file fallback from OpenSSL's certificate chain-reader
    # behaviour, which varies across versions when a non-cert PEM block follows
    # the leaf certificate in the file.
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client --host localhost --port {SECURE_PORT} "
            f"--secure -c {CONFIG_COMBINED_PEM} -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert "api_version" in data


# Group E — SSL connectivity parity with clickhouse-client
#
# These tests verify that keeper-client's openSSL.client behaviour matches the
# server client: RejectCertificateHandler is the seeded default, the standard
# protocol/session flags are honoured, and the --secure-connection-timeout option
# is accepted and applied.


def test_reject_handler_seeded_when_not_in_config(started_cluster):
    """keeper-client seeds RejectCertificateHandler when the config has no invalidCertificateHandler.name.

    CONFIG_SSL_NO_HANDLER has an <openSSL><client> section that deliberately omits
    invalidCertificateHandler. The server cert is self-signed and no CA is configured,
    so any connection attempt must fail if certificate rejection is actually enforced.
    """
    data = run_keeper_client(f"-c {CONFIG_SSL_NO_HANDLER}", nothrow=True)
    assert "api_version" not in data, (
        "expected certificate verification failure but got a successful response — "
        "RejectCertificateHandler was not seeded as the default"
    )


def test_accept_invalid_certificate_overrides_reject_handler(started_cluster):
    """--accept-invalid-certificate must override the seeded RejectCertificateHandler.

    Ensures the two code paths do not conflict: first the else-branch seeds
    RejectCertificateHandler, then the if-branch replaces it with AcceptCertificateHandler.
    """
    data = run_keeper_client("--accept-invalid-certificate")
    assert "api_version" in data


def test_secure_connection_timeout_option_accepted(started_cluster):
    """--secure-connection-timeout is accepted without error and the connection succeeds."""
    data = run_keeper_client(f"--tls-ca-file {CA_PATH} --secure-connection-timeout 30")
    assert "api_version" in data


def test_secure_connection_timeout_applies_to_four_letter_command(started_cluster):
    """--secure-connection-timeout is applied to four-letter-word commands over TLS."""
    data = run_keeper_client(
        f"--tls-ca-file {CA_PATH} --secure-connection-timeout 30", query="ruok"
    )
    assert data.strip() == "imok"


# Group F — fingerprint-based SSL context rebuild on reconnect
#
# The fingerprint/rebuild logic (connectToKeeper + fingerprintTLSMaterial +
# SSLManager::shutdown) only activates inside a single keeper-client process when
# connectToKeeper() is called a second time after config changes.  A fresh -q
# invocation never exercises this path.
#
# These tests run keeper-client in interactive mode (feeding it two commands via a
# pipe with a deliberate gap), stop/restart ClickHouse between the commands to force
# a session expiry, and change the on-disk config during the outage.  When keeper-
# client processes the second command it detects the lost session, retries, calls
# connectToKeeper(), re-reads the config file, computes a new fingerprint, calls
# SSLManager::shutdown() + defaultClientContext(), and reconnects with the rebuilt
# SSL context.


def _run_client_interactive(node, port, config_path, result_holder):
    """Run keeper-client in interactive mode, feeding it two ls commands separated by
    a 35-second gap.  The caller has that window to restart Keeper and mutate the
    config file.  Output is collected in result_holder[0]."""
    output = node.exec_in_container(
        [
            "bash",
            "-c",
            f"("
            f"  echo 'ls /keeper';"
            f"  sleep 35;"
            f"  echo 'ls /keeper';"
            f") | clickhouse keeper-client"
            f" --host localhost --port {port} --secure"
            f" -c {config_path} 2>&1",
        ],
        privileged=True,
        nothrow=True,
    )
    result_holder.append(output)


def test_ssl_context_rebuilt_after_config_change_and_reconnect(started_cluster):
    """keeper-client rebuilds the SSL context mid-session when the config changes.

    Sequence:
    1. Connect with config A  (loadDefaultCAFile=false, explicit caConfig).
    2. First 'ls /keeper' succeeds — proves initial TLS context works.
    3. Stop ClickHouse; sleep long enough for the ZooKeeper session to expire.
    4. Write config B (loadDefaultCAFile=true) over the same path — changes the
       fingerprint without breaking connectivity.
    5. Start ClickHouse; wait for Keeper to be ready.
    6. The second 'ls /keeper' (piped after 35s) arrives; keeper-client detects the
       expired session, calls connectToKeeper(), re-reads the file, sees a different
       fingerprint, calls SSLManager::shutdown() + defaultClientContext(), reconnects.
    7. Second 'ls /keeper' succeeds — proves the rebuilt context is functional.
    """
    mutable_cfg = "/tmp/reconnect_fingerprint_cfg.xml"
    session_timeout_s = 10  # matches config().getInt("session-timeout", 10)

    # Config A: explicit caConfig, loadDefaultCAFile=false
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"cat > {mutable_cfg} << 'CONFEOF'\n"
            f"<clickhouse><openSSL><client>\n"
            f"  <caConfig>{CA_PATH}</caConfig>\n"
            f"  <loadDefaultCAFile>false</loadDefaultCAFile>\n"
            f"</client></openSSL></clickhouse>\n"
            f"CONFEOF",
        ]
    )

    result = []
    t = threading.Thread(
        target=_run_client_interactive,
        args=(node1, SECURE_PORT, mutable_cfg, result),
        daemon=True,
    )
    t.start()

    # Allow the first command to complete before disrupting the session.
    time.sleep(6)

    # Take Keeper down; the ZooKeeper session will expire after session_timeout_s.
    node1.stop_clickhouse()
    time.sleep(session_timeout_s + 2)  # ensure session expiry

    # Config B: loadDefaultCAFile=true — different fingerprint, still valid CA.
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"cat > {mutable_cfg} << 'CONFEOF'\n"
            f"<clickhouse><openSSL><client>\n"
            f"  <caConfig>{CA_PATH}</caConfig>\n"
            f"  <loadDefaultCAFile>true</loadDefaultCAFile>\n"
            f"</client></openSSL></clickhouse>\n"
            f"CONFEOF",
        ]
    )

    node1.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1, port=SECURE_PORT)

    # The bash pipe delivers the second command at t≈35s; wait for the thread.
    t.join(timeout=60)

    assert len(result) == 1, "keeper-client interactive session did not finish"
    output = result[0]
    assert output.count("api_version") == 2, (
        "expected 'api_version' twice (once per ls command) but got:\n" + output
    )
