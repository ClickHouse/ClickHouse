import os
import sys

import grpc
import pytest

from helpers.cluster import ClickHouseCluster, run_and_check

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
    ],
    # Bug in TSAN reproduces in this test https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
    env_variables={
        "TSAN_OPTIONS": "report_atomic_races=0 " + os.getenv("TSAN_OPTIONS", default="")
    },
)


def get_grpc_url(instance=node):
    return f"{instance.ip_address}:{GRPC_PORT}"


def create_secure_channel():
    ca_cert = open(os.path.join(config_dir, "ca-cert.pem"), "rb").read()
    client_key = open(os.path.join(config_dir, "client-key.pem"), "rb").read()
    client_cert = open(os.path.join(config_dir, "client-cert.pem"), "rb").read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(
        get_grpc_url(),
        credentials,
        options=(("grpc.ssl_target_name_override", SSL_HOST),),
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
