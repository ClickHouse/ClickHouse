import asyncio
import json
import logging
import os
import pytest

from helpers.cluster import ClickHouseCluster, run_and_check
from helpers.config_cluster import nats_user, nats_pass

from . import common as nats_helpers

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/nats.xml", "configs/macros.xml"],
    user_configs=["configs/users.xml"],
    with_nats=True,
    clickhouse_path_dir="clickhouse_path",
    # The other modules let the broker certificate pass unverified, which makes `nats_ca_file`
    # unobservable. Verification stays on here, so the broker is reachable only through the CA.
    env_variables={"CLICKHOUSE_NATS_TLS_SECURE": "1"},
)

CA_FILE = "/etc/clickhouse-server/nats_ca.pem"
CLIENT_CERT_FILE = "/etc/clickhouse-server/nats_client_cert.pem"
CLIENT_KEY_FILE = "/etc/clickhouse-server/nats_client_key.pem"


# Helpers


async def publish_messages(cluster_inst, subject, messages=()):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        await nc.publish(subject, message.encode())
    await nc.flush()

    await nc.close()


def generate_client_certificate(cert_dir):
    """Signs a client certificate with the CA that also signed the broker certificate."""
    ca_dir = os.path.join(cert_dir, "ca")
    client_dir = os.path.join(cert_dir, "client")
    os.makedirs(client_dir, exist_ok=True)

    key = os.path.join(client_dir, "client-key.pem")
    request = os.path.join(client_dir, "client-req.pem")
    cert = os.path.join(client_dir, "client-cert.pem")
    extensions = os.path.join(client_dir, "client-ext.cnf")

    with open(extensions, "w") as extensions_file:
        extensions_file.write(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=clientAuth\n"
        )

    run_and_check(
        ["openssl", "req", "-newkey", "rsa:4096", "-nodes", "-batch",
         "-keyout", key, "-out", request,
         "-subj", "/O=ClickHouse/CN=clickhouse-nats-client"]
    )
    run_and_check(
        ["openssl", "x509", "-req", "-days", "3650", "-in", request,
         "-CA", os.path.join(ca_dir, "ca-cert.pem"),
         "-CAkey", os.path.join(ca_dir, "ca-key.pem"),
         "-CAcreateserial", "-extfile", extensions, "-out", cert]
    )

    return cert, key


# Fixtures


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()

        client_cert, client_key = generate_client_certificate(cluster.nats_cert_dir)
        instance.copy_file_to_container(
            os.path.join(cluster.nats_cert_dir, "ca", "ca-cert.pem"), CA_FILE
        )
        instance.copy_file_to_container(client_cert, CLIENT_CERT_FILE)
        instance.copy_file_to_container(client_key, CLIENT_KEY_FILE)

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def nats_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    yield  # run test

    instance.query("DROP DATABASE test")


# Tests


def test_nats_ca_file(nats_cluster):
    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'ca_file',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_ca_file = '{CA_FILE}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = [json.dumps({"key": i, "value": i}) for i in range(20)]
    asyncio.run(publish_messages(nats_cluster, "ca_file", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 20)


def test_nats_client_certificate(nats_cluster):
    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'client_certificate',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_ca_file = '{CA_FILE}',
                     nats_client_cert_file = '{CLIENT_CERT_FILE}',
                     nats_client_key_file = '{CLIENT_KEY_FILE}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = [json.dumps({"key": i, "value": i}) for i in range(20)]
    asyncio.run(publish_messages(nats_cluster, "client_certificate", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 20)


def test_nats_untrusted_server_certificate(nats_cluster):
    # The broker certificate is signed by a CA of its own, so without `nats_ca_file` there is
    # nothing to verify it against.
    assert "CANNOT_CONNECT_NATS" in instance.query_and_get_error(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'untrusted',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_startup_connect_tries = 1;
        """
    )


def test_nats_unreadable_ca_file(nats_cluster):
    error = instance.query_and_get_error(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'unreadable_ca',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_ca_file = '/etc/clickhouse-server/no_such_ca.pem',
                     nats_startup_connect_tries = 1;
        """
    )
    assert "Cannot load NATS trusted CA certificates" in error


def test_nats_unreadable_client_certificate(nats_cluster):
    error = instance.query_and_get_error(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'unreadable_client_certificate',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_ca_file = '{CA_FILE}',
                     nats_client_cert_file = '/etc/clickhouse-server/no_such_cert.pem',
                     nats_client_key_file = '{CLIENT_KEY_FILE}',
                     nats_startup_connect_tries = 1;
        """
    )
    assert "Cannot load NATS client certificate chain" in error


def test_nats_client_certificate_without_key(nats_cluster):
    error = instance.query_and_get_error(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'certificate_without_key',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_ca_file = '{CA_FILE}',
                     nats_client_cert_file = '{CLIENT_CERT_FILE}';
        """
    )
    assert "must be specified together" in error


def test_nats_certificates_without_secure(nats_cluster):
    error = instance.query_and_get_error(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'certificates_without_secure',
                     nats_format = 'JSONEachRow',
                     nats_ca_file = '{CA_FILE}';
        """
    )
    assert "without nats_secure" in error


def test_nats_hiding_client_key_file(nats_cluster):
    table_name = "test_hiding_client_key_file"
    instance.query(
        f"""
        CREATE TABLE test.{table_name} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{table_name}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}',
                     nats_ca_file = '{CA_FILE}',
                     nats_client_cert_file = '{CLIENT_CERT_FILE}',
                     nats_client_key_file = '{CLIENT_KEY_FILE}';
        """
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(
        f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE test.{table_name}%'"
    )
    assert "nats_client_key_file = \\'[HIDDEN]\\'" in message
    # The certificates themselves are public, so only the private key is masked.
    assert CLIENT_CERT_FILE in message
    assert CA_FILE in message


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
