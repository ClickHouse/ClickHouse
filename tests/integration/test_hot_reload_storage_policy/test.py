import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.d/storage_configuration.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


old_disk_config = """
<clickhouse>
    <storage_configuration>
        <disks>
            <disk0>
                <path>/var/lib/clickhouse/disk0/</path>
            </disk0>
        </disks>
        <policies>
            <default_policy>
                <volumes>
                    <volume0>
                        <disk>disk0</disk>
                    </volume0>
                </volumes>
            </default_policy>
        </policies>
    </storage_configuration>
    <remote_servers>
        <default>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>9000</port>
                </replica>
            </shard>
        </default>
    </remote_servers>
</clickhouse>
"""

new_disk_config = """
<clickhouse>
    <storage_configuration>
        <disks>
            <disk0>
                <path>/var/lib/clickhouse/disk0/</path>
            </disk0>
            <disk1>
                <path>/var/lib/clickhouse/disk1/</path>
            </disk1>
        </disks>
        <policies>
            <default_policy>
                <volumes>
                    <volume1>
                        <disk>disk1</disk>
                    </volume1>
                    <volume0>
                        <disk>disk0</disk>
                    </volume0>
                </volumes>
            </default_policy>
        </policies>
    </storage_configuration>
    <remote_servers>
        <default>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>9000</port>
                </replica>
            </shard>
        </default>
    </remote_servers>
</clickhouse>
"""


def set_config(node, config):
    node.replace_config("/etc/clickhouse-server/config.d/config.xml", config)
    node.query("SYSTEM RELOAD CONFIG")
    # to give ClickHouse time to refresh disks
    time.sleep(1)


def test_hot_reload_policy(started_cluster):
    node.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = MergeTree() PARTITION BY d ORDER BY tuple() SETTINGS storage_policy = 'default_policy'"
    )
    node.query("SYSTEM STOP MERGES t")
    node.query("INSERT INTO TABLE t VALUES (1, 'foo')")

    set_config(node, new_disk_config)

    # After reloading new policy with new disk, merge tree tables should reinitialize the new disk (create relative path, 'detached' folder...)
    # and as default policy is `least_used`, at least one insertion should come to the new disk
    node.query("INSERT INTO TABLE t VALUES (1, 'foo')")
    node.query("INSERT INTO TABLE t VALUES (1, 'bar')")

    num_disks = int(
        node.query(
            "SELECT uniqExact(disk_name) FROM system.parts WHERE database = 'default' AND table = 't'"
        )
    )

    assert (
        num_disks == 2
    ), "Node should write data to 2 disks after reloading disks, but got {}".format(
        num_disks
    )

    # If `detached` is not created this query will throw exception
    node.query("ALTER TABLE t DETACH PARTITION 1")

    node.query("DROP TABLE t")


def test_hot_reload_policy_distributed_table(started_cluster):
    # Same test for distributed table, it should reinitialize the storage policy and data volume
    # We check it by trying an insert and the distribution queue must be on new disk

    # Restart node first
    set_config(node, old_disk_config)
    node.restart_clickhouse()

    node.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = MergeTree PARTITION BY d ORDER BY tuple()"
    )
    node.query(
        "CREATE TABLE t_d (d Int32, s String) ENGINE = Distributed('default', 'default', 't', d%20, 'default_policy')"
    )

    node.query("SYSTEM STOP DISTRIBUTED SENDS t_d")
    node.query(
        "INSERT INTO TABLE t_d SETTINGS prefer_localhost_replica = 0 VALUES (2, 'bar') (12, 'bar')"
    )
    # t_d should create queue on disk0
    queue_path = node.query("SELECT data_path FROM system.distribution_queue")

    assert (
        "disk0" in queue_path
    ), "Distributed table should create distributed queue on disk0 (disk1), but the queue path is {}".format(
        queue_path
    )

    node.query("SYSTEM START DISTRIBUTED SENDS t_d")

    node.query("SYSTEM FLUSH DISTRIBUTED t_d")

    set_config(node, new_disk_config)

    node.query("SYSTEM STOP DISTRIBUTED SENDS t_d")
    node.query(
        "INSERT INTO TABLE t_d SETTINGS prefer_localhost_replica = 0 VALUES (2, 'bar') (12, 'bar')"
    )

    # t_d should create queue on disk1
    queue_path = node.query("SELECT data_path FROM system.distribution_queue")

    assert (
        "disk1" in queue_path
    ), "Distributed table should be using new disk (disk1), but the queue paths are {}".format(
        queue_path
    )

    node.query("DROP TABLE t")
    node.query("DROP TABLE t_d")
