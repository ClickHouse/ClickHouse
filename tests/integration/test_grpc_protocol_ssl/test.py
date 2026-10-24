import os
import sys

import grpc
import pytest

from helpers.cluster import ClickHouseCluster

script_dir = os.path.dirname(os.path.realpath(__file__))
pb2_dir = os.path.join(script_dir, "pb2")
if pb2_dir not in sys.path:
    sys.path.append(pb2_dir)
import clickhouse_grpc_pb2  # Execute pb2/generate.py to generate these modules.
import clickhouse_grpc_pb2_grpc

# The test cluster is configured with certificate for that host name, see 'server-ext.cnf'.
# The client have to verify server certificate against that name. Client uses SNI
SSL_HOST = "integration-tests.clickhouse.com"
GRPC_PORT = 9100
CONFIG_D = "/etc/clickhouse-server/config.d"
DEFAULT_ENCODING = "utf-8"


# Utilities

config_dir = os.path.join(script_dir, "./configs")
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/grpc_config.xml",
        "configs/server-key.pem",
        "configs/server-cert.pem",
        "configs/ca-cert.pem",
        "configs/second-server-key.pem",
        "configs/second-server-cert.pem",
        "configs/second-server-public-key.pem",
    ],
    # Bug in TSAN reproduces in this test https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
    env_variables={
        "TSAN_OPTIONS": "report_atomic_races=0 " + os.getenv("TSAN_OPTIONS", default="")
    },
)


def get_grpc_url(instance=node):
    return f"{instance.ip_address}:{GRPC_PORT}"


def create_secure_channel(ca_cert_file="ca-cert.pem"):
    ca_cert = open(os.path.join(config_dir, ca_cert_file), "rb").read()
    client_key = open(os.path.join(config_dir, "client-key.pem"), "rb").read()
    client_cert = open(os.path.join(config_dir, "client-cert.pem"), "rb").read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(
        get_grpc_url(),
        credentials,
        # A local subchannel pool makes every channel open its own connection, so it gets the current server certificate.
        options=(
            ("grpc.ssl_target_name_override", SSL_HOST),
            ("grpc.use_local_subchannel_pool", 1),
        ),
    )
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel


def create_insecure_channel():
    channel = grpc.insecure_channel(get_grpc_url())
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel


def create_secure_channel_with_wrong_client_certificate():
    ca_cert = open(os.path.join(config_dir, "ca-cert.pem"), "rb").read()
    client_key = open(os.path.join(config_dir, "wrong-client-key.pem"), "rb").read()
    client_cert = open(os.path.join(config_dir, "wrong-client-cert.pem"), "rb").read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(get_grpc_url(), credentials)
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel


def query(query_text, channel):
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query_text)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField("exception"):
        raise Exception(result.exception.display_text)
    return result.output.decode(DEFAULT_ENCODING)


def replace_server_certificate(cert_file, key_file):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cp {CONFIG_D}/{cert_file} {CONFIG_D}/server-cert.pem && cp {CONFIG_D}/{key_file} {CONFIG_D}/server-key.pem",
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")


def secure_channel_works(ca_cert_file):
    try:
        with create_secure_channel(ca_cert_file) as channel:
            return query("SELECT 'ok'", channel) == "ok\n"
    except grpc.FutureTimeoutError:
        return False


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        yield cluster

    finally:
        cluster.shutdown()


# Actual tests


def test_secure_channel():
    with create_secure_channel() as channel:
        assert query("SELECT 'ok'", channel) == "ok\n"


def test_insecure_channel():
    with pytest.raises(grpc.FutureTimeoutError):
        with create_insecure_channel() as channel:
            query("SELECT 'ok'", channel)


def test_wrong_client_certificate():
    with pytest.raises(grpc.FutureTimeoutError):
        with create_insecure_channel() as channel:
            query("SELECT 'ok'", channel)


def test_reload_certificate():
    node.exec_in_container(["bash", "-c", f"cp {CONFIG_D}/server-cert.pem {CONFIG_D}/server-key.pem /tmp/"])
    try:
        replace_server_certificate("second-server-cert.pem", "second-server-key.pem")
        assert secure_channel_works("second-ca-cert.pem")
        assert not secure_channel_works("ca-cert.pem")

        # A public key matching the certificate is not a usable private key, the previous one is still served.
        node.exec_in_container(["bash", "-c", f"cp {CONFIG_D}/second-server-public-key.pem {CONFIG_D}/server-key.pem"])
        node.query("SYSTEM RELOAD CONFIG")
        assert secure_channel_works("second-ca-cert.pem")
    finally:
        node.exec_in_container(["bash", "-c", f"cp /tmp/server-cert.pem /tmp/server-key.pem {CONFIG_D}/"])
        node.query("SYSTEM RELOAD CONFIG")

    assert secure_channel_works("ca-cert.pem")
