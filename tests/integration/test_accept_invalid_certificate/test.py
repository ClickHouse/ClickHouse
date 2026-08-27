import os
import os.path
import tempfile

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAX_RETRY = 5
CA_CERT = f"{SCRIPT_DIR}/certs/ca-cert.pem"

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/self-key.pem",
        "certs/self-cert.pem",
        "certs/ca-cert.pem",
    ],
    with_zookeeper=False,
)


node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/ssl_config_strict.xml",
        "certs/self-key.pem",
        "certs/self-cert.pem",
        "certs/ca-cert.pem",
    ],
    with_zookeeper=False,
)


node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/ssl_config_ca_signed.xml",
        "certs/client-key.pem",
        "certs/client-cert.pem",
        "certs/ca-cert.pem",
    ],
    with_zookeeper=False,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


config_default = """<clickhouse>
</clickhouse>"""

config_accept = """<clickhouse>
    <accept-invalid-certificate>1</accept-invalid-certificate>
</clickhouse>"""

config_connection_accept = """<clickhouse>
    <connections_credentials>
        <connection>
            <name>{ip_address}</name>
            <accept-invalid-certificate>1</accept-invalid-certificate>
        </connection>
    </connections_credentials>
</clickhouse>"""

# node2 presents a certificate this CA signs, so the chain is valid and only the name can fail.
config_ca_signed = """<clickhouse>
    <openSSL>
        <client>
            <caConfig>{caConfig}</caConfig>
            <loadDefaultCAFile>false</loadDefaultCAFile>
        </client>
    </openSSL>
</clickhouse>"""

config_ca_signed_sni_override = """<clickhouse>
    <tls-sni-override>client</tls-sni-override>
    <openSSL>
        <client>
            <caConfig>{caConfig}</caConfig>
            <loadDefaultCAFile>false</loadDefaultCAFile>
        </client>
    </openSSL>
</clickhouse>"""

config_ca_signed_no_extended_verification = """<clickhouse>
    <openSSL>
        <client>
            <caConfig>{caConfig}</caConfig>
            <loadDefaultCAFile>false</loadDefaultCAFile>
            <extendedVerification>false</extendedVerification>
        </client>
    </openSSL>
</clickhouse>"""


def execute_query_native(node, query, config):
    fd, config_path = tempfile.mkstemp(
        prefix="client_", suffix=".xml", dir=f"{SCRIPT_DIR}/configs"
    )
    try:
        with os.fdopen(fd, "w") as f:
            f.write(config)

        client = Client(
            node.ip_address,
            9440,
            command=cluster.client_bin_path,
            secure=True,
            config=config_path,
        )

        return client.query(query)
    finally:
        try:
            os.remove(config_path)
        except FileNotFoundError:
            pass


def test_default():
    with pytest.raises(Exception) as err:
        execute_query_native(instance, "SELECT 1", config_default)
    assert "certificate verify failed" in str(err.value)


def test_accept():
    assert execute_query_native(instance, "SELECT 1", config_accept) == "1\n"


def test_connection_accept():
    assert (
        execute_query_native(
            instance,
            "SELECT 1",
            config_connection_accept.format(ip_address=f"{instance.ip_address}"),
        )
        == "1\n"
    )


def test_strict_reject():
    with pytest.raises(Exception) as err:
        execute_query_native(node1, "SELECT 1", "<clickhouse></clickhouse>")
    assert "certificate verify failed" in str(err.value)


def test_strict_reject_with_config():
    with pytest.raises(Exception) as err:
        execute_query_native(node1, "SELECT 1", config_accept)
    # Accept both error messages due to race condition in SSL handshake:
    # - "alert certificate required": TCP layer transmits Alert before close() executes
    # - "Connection reset by peer": close() executes before TCP layer transmits Alert from send buffer
    # Race condition: send() is async (returns after copying to kernel buffer), close() may execute
    # before TCP layer actually sends the Alert packet, causing RST to be sent instead
    assert "alert certificate required" in str(err.value) or "Connection reset by peer" in str(err.value)


def test_strict_connection_reject():
    with pytest.raises(Exception) as err:
        execute_query_native(
            node1,
            "SELECT 1",
            config_connection_accept.format(ip_address=f"{instance.ip_address}"),
        )
    assert "certificate verify failed" in str(err.value)


def test_hostname_mismatch_rejected_by_default():
    with pytest.raises(Exception) as err:
        execute_query_native(node2, "SELECT 1", config_ca_signed.format(caConfig=CA_CERT))
    assert "Unacceptable certificate" in str(err.value)


def test_hostname_match_accepted():
    assert (
        execute_query_native(
            node2, "SELECT 1", config_ca_signed_sni_override.format(caConfig=CA_CERT)
        )
        == "1\n"
    )


def test_extended_verification_disabled():
    assert (
        execute_query_native(
            node2,
            "SELECT 1",
            config_ca_signed_no_extended_verification.format(caConfig=CA_CERT),
        )
        == "1\n"
    )
