import asyncio
import json
import os
import pytest

from helpers.cluster import ClickHouseCluster

from . import common as nats_helpers

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/nats.xml", "configs/macros.xml", "configs/named_collection_tls.xml"],
    user_configs=["configs/users.xml"],
    with_nats=True,
    clickhouse_path_dir="clickhouse_path",
    env_variables={
        # The other modules let the broker certificate pass unverified, which makes `nats_ca_file`
        # unobservable. Verification stays on here, so the broker is reachable only through the CA.
        "CLICKHOUSE_NATS_TLS_SECURE": "1",
        # The broker also demands a client certificate, so a table without one cannot connect.
        "NATS_TLS_CLIENT_AUTH": "--tlsverify --tlscacert=/etc/certs/ca-cert.pem",
    },
)

CA_FILE = "/etc/clickhouse-server/nats_ca.pem"
CLIENT_CERT_FILE = "/etc/clickhouse-server/nats_client_cert.pem"
CLIENT_KEY_FILE = "/etc/clickhouse-server/nats_client_key.pem"


# Helpers


def create_table_query(collection, subject):
    return f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS({collection}, nats_subjects = '{subject}');
        """


async def publish_messages(cluster_inst, subject, messages):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    for message in messages:
        await nc.publish(subject, message.encode())
    await nc.flush()
    await nc.close()


# Fixtures


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()

        certs = cluster.nats_cert_dir
        for host_path, container_path in [
            (os.path.join(certs, "ca", "ca-cert.pem"), CA_FILE),
            (os.path.join(certs, "client", "client-cert.pem"), CLIENT_CERT_FILE),
            (os.path.join(certs, "client", "client-key.pem"), CLIENT_KEY_FILE),
        ]:
            instance.copy_file_to_container(host_path, container_path)

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


def test_nats_certificates(nats_cluster):
    # The CA verifies the broker certificate and the client certificate satisfies the broker.
    instance.query(create_table_query("nats_tls", "certificates"))
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = [json.dumps({"key": i, "value": i}) for i in range(20)]
    asyncio.run(publish_messages(nats_cluster, "certificates", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 20)


def test_nats_without_client_certificate(nats_cluster):
    # The broker rejects a client that presents no certificate, so the settings are load-bearing.
    error = instance.query_and_get_error(create_table_query("nats_tls_ca_only", "no_client_certificate"))
    assert "CANNOT_CONNECT_NATS" in error


def test_nats_untrusted_server_certificate(nats_cluster):
    # Without the CA the broker certificate cannot be verified, so `nats_ca_file` is load-bearing.
    error = instance.query_and_get_error(create_table_query("nats_tls_cert_only", "untrusted"))
    assert "CANNOT_CONNECT_NATS" in error


def test_nats_unreadable_certificate_files(nats_cluster):
    ca_error = instance.query_and_get_error(create_table_query("nats_tls_unreadable_ca", "unreadable_ca"))
    assert "Cannot load NATS trusted CA certificates" in ca_error

    chain_error = instance.query_and_get_error(create_table_query("nats_tls_unreadable_chain", "unreadable_chain"))
    assert "Cannot load NATS client certificate chain" in chain_error


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
