import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node0 = cluster.add_instance(
    "node0", with_zookeeper=True, main_configs=["configs/config.xml"]
)
node1 = cluster.add_instance(
    "node1", with_zookeeper=True, main_configs=["configs/config.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


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


def test_hot_reload_policy(started_cluster):
    node0.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '0') PARTITION BY d ORDER BY tuple() SETTINGS storage_policy = 'default_policy'"
    )
    node0.query("INSERT INTO TABLE t VALUES (1, 'foo') (1, 'bar')")

    node1.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_mirror', '1') PARTITION BY d ORDER BY tuple() SETTINGS storage_policy = 'default_policy'"
    )
    node1.query(
        "CREATE TABLE t_d (d Int32, s String) ENGINE = Distributed('default', 'default', 't', d%20, 'default_policy')"
    )
    set_config(node1, new_disk_config)
    time.sleep(1)

    # After reloading new policy with new disk, merge tree tables should reinitialize the new disk (create relative path, 'detached' folder...)
    # Otherwise FETCH PARTITION will fails
    node1.query("ALTER TABLE t FETCH PARTITION 1 FROM '/clickhouse/tables/t'")
    node1.query("ALTER TABLE t ATTACH PARTITION 1")

    # Check that fetch partition success and we get full data from node0
    result = int(node1.query("SELECT count() FROM t"))
    assert (
        result == 2
    ), "Node should have 2 rows after reloading storage configuration and fetch new partition, but get {} rows".format(
        result
    )

    # Same test for distributed table, it should reinitialize the storage policy and data volume
    # We check it by trying an insert and the distribution queue must be on new disk
    node1.query("SYSTEM STOP DISTRIBUTED SENDS t_d")
    node1.query(
        "INSERT INTO TABLE t_d SETTINGS prefer_localhost_replica = 0 VALUES (2, 'bar') (12, 'bar')"
    )

    queue_path = node1.query("SELECT data_path FROM system.distribution_queue")

    assert (
        "disk1" in queue_path
    ), "Distributed table should be using new disk (disk1), but it's still creating queue in {}".format(
        queue_path
    )

    node1.query("SYSTEM START DISTRIBUTED SENDS t_d")

    node1.query("SYSTEM FLUSH DISTRIBUTED t_d")

    result = int(node1.query("SELECT count() FROM t"))

    assert (
        result == 4
    ), "Node should have 4 rows after inserting to distributed table, but get {} rows".format(
        result
    )
