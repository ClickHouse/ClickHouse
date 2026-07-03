import os.path
from os import remove

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAX_RETRY = 5

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


def execute_query_native(node, query, config):
    config_path = f"{SCRIPT_DIR}/configs/client.xml"

    file = open(config_path, "w")
    file.write(config)
    file.close()

    client = Client(
        node.ip_address,
        9440,
        command=cluster.client_bin_path,
        secure=True,
        config=config_path,
    )

    try:
        result = client.query(query)
        remove(config_path)
        return result
    except:
        remove(config_path)
        raise


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
    assert "alert certificate required" in str(err.value)


def test_strict_connection_reject():
    with pytest.raises(Exception) as err:
        execute_query_native(
            node1,
            "SELECT 1",
            config_connection_accept.format(ip_address=f"{instance.ip_address}"),
        )
    assert "certificate verify failed" in str(err.value)
