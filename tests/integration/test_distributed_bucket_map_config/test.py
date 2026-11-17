import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/clusters.xml"])

CONFIG_PATH = "/etc/clickhouse-server/config.d/bucket_map_settings.xml"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_bucket_map_settings_from_config_do_not_change_existing_tables(started_cluster):
    # The server has no ZooKeeper, so a table that started to use a bucket map could not load it.
    node.query(
        """
        CREATE TABLE target (key UInt64) ENGINE = MergeTree ORDER BY key;
        CREATE TABLE dist (key UInt64) ENGINE = Distributed(localhost_cluster, default, target, key);
        CREATE DATABASE remote_db ENGINE = Remote('localhost', 'default');
        CREATE DATABASE cluster_db ENGINE = Cluster('localhost_cluster', 'default');
        """
    )

    insert_settings = "SETTINGS distributed_foreground_insert = 1"
    for table in ("dist", "remote_db.target", "cluster_db.target"):
        node.query(f"INSERT INTO {table} {insert_settings} VALUES (1)")
    assert node.query("SELECT count() FROM target") == "3\n"

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/bucket_map_settings.xml"), CONFIG_PATH
    )
    node.restart_clickhouse()

    for table in ("dist", "remote_db.target", "cluster_db.target"):
        node.query(f"INSERT INTO {table} {insert_settings} VALUES (2)")
    assert node.query("SELECT count() FROM target WHERE key = 2") == "3\n"
    for table in ("dist", "remote_db.target", "cluster_db.target"):
        assert node.query(f"SELECT count() FROM {table} WHERE key = 2") == "3\n"
    assert node.query("SELECT count() FROM system.distributed_bucket_maps") == "0\n"

    error = node.query_and_get_error(
        """
        CREATE TABLE dist_new (key UInt64)
        ENGINE = Distributed(localhost_cluster, default, target, key)
        """,
        settings={"allow_experimental_distributed_bucket_map": 1},
    )
    assert "must be set in the `SETTINGS` clause of the table" in error
