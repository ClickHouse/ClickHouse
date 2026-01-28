import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.query(
            """CREATE TABLE distributed (id UInt32) ENGINE =
            Distributed('test_cluster', 'default', 'replicated')"""
        )

        node.query(
            """CREATE TABLE distributed2 (id UInt32) ENGINE =
            Distributed('test_cluster2', 'default', 'replicated')"""
        )

        yield cluster

    finally:
        cluster.shutdown()


base_config = """
<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
        <test_cluster2>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster2>
    </remote_servers>
</clickhouse>
"""

test_config1 = """
<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
        <test_cluster2>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster2>
    </remote_servers>
</clickhouse>
"""

test_config2 = """
<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
    </remote_servers>
</clickhouse>
"""

test_config3 = """
<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
        <test_cluster2>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node_2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster2>
        <test_cluster3>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node_1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster3>
    </remote_servers>
</clickhouse>
"""


def send_repeated_query(table, count=5):
    for i in range(count):
        node.query_and_get_error(
            "SELECT count() FROM {} SETTINGS receive_timeout=1, handshake_timeout_ms=1".format(
                table
            )
        )


def get_errors_count(cluster, host_name="node_1"):
    return int(
        node.query(
            "SELECT errors_count FROM system.clusters WHERE cluster='{}' and host_name='{}'".format(
                cluster, host_name
            )
        )
    )


def set_config(config):
    node.replace_config("/etc/clickhouse-server/config.d/remote_servers.xml", config)
    node.query("SYSTEM RELOAD CONFIG")


def test_simple_reload(started_cluster):
    send_repeated_query("distributed")

    assert get_errors_count("test_cluster") > 0

    node.query("SYSTEM RELOAD CONFIG")

    assert get_errors_count("test_cluster") > 0


def test_update_one_cluster(started_cluster):
    send_repeated_query("distributed")
    send_repeated_query("distributed2")

    assert get_errors_count("test_cluster") > 0
    assert get_errors_count("test_cluster2") > 0

    set_config(test_config1)

    assert get_errors_count("test_cluster") == 0
    assert get_errors_count("test_cluster2") > 0

    set_config(base_config)


def test_delete_cluster(started_cluster):
    send_repeated_query("distributed")
    send_repeated_query("distributed2")

    assert get_errors_count("test_cluster") > 0
    assert get_errors_count("test_cluster2") > 0

    set_config(test_config2)

    assert get_errors_count("test_cluster") > 0

    result = node.query("SELECT * FROM system.clusters WHERE cluster='test_cluster2'")
    assert result == ""

    set_config(base_config)


def test_add_cluster(started_cluster):
    send_repeated_query("distributed")
    send_repeated_query("distributed2")

    assert get_errors_count("test_cluster") > 0
    assert get_errors_count("test_cluster2") > 0

    set_config(test_config3)

    assert get_errors_count("test_cluster") > 0
    assert get_errors_count("test_cluster2") > 0

    result = node.query("SELECT * FROM system.clusters WHERE cluster='test_cluster3'")
    assert result != ""

    set_config(base_config)
