import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/disks.xml"], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_storage_policy_configuration_change(started_cluster):
    node.query(
        "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
    )

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disk2_only.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disks.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()
