import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node0 = cluster.add_instance(
    "node0", with_zookeeper=True, main_configs=["configs/storage_configuration.xml"]
)
node1 = cluster.add_instance(
    "node1", with_zookeeper=True, main_configs=["configs/storage_configuration.xml"]
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
            <disk2>
                <path>/var/lib/clickhouse/disk2/</path>
            </disk2>
        </disks>
        <policies>
            <default_policy>
                <volumes>
                    <default_volume>
                        <disk>disk2</disk>
                        <disk>disk1</disk>
                        <disk>disk0</disk>
                    </default_volume>
                </volumes>
            </default_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""


def set_config(node, config):
    node.replace_config(
        "/etc/clickhouse-server/config.d/storage_configuration.xml", config
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_hot_reload_policy(started_cluster):
    node0.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '0') PARTITION BY d ORDER BY tuple() SETTINGS storage_policy = 'default_policy'"
    )
    node0.query("INSERT INTO TABLE t VALUES (1, 'foo') (1, 'bar')")

    node1.query(
        "CREATE TABLE t (d Int32, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_mirror', '1') PARTITION BY d ORDER BY tuple() SETTINGS storage_policy = 'default_policy'"
    )
    set_config(node1, new_disk_config)
    time.sleep(1)
    node1.query("ALTER TABLE t FETCH PARTITION 1 FROM '/clickhouse/tables/t'")
    result = int(node1.query("SELECT count() FROM t"))
    assert (
        result == 4,
        "Node should have 2 x full data (4 rows) after reloading storage configuration and fetch new partition, but get {} rows".format(
            result
        ),
    )
