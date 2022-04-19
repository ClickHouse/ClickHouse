import logging
from string import Template
import time

import pytest
from helpers.cluster import ClickHouseCluster

from pyhdfs import HdfsClient

SHARDS = 2
FILES_OVERHEAD_PER_TABLE = 1  # format_version.txt
FILES_OVERHEAD_PER_PART_COMPACT = 7


def wait_for_hdfs_objects(cluster, fp, expected, num_tries=30):
    fs = HdfsClient(hosts=cluster.hdfs_ip)
    while num_tries > 0:
        num_hdfs_objects = len(fs.listdir(fp))
        if num_hdfs_objects == expected:
            break
        num_tries -= 1
        time.sleep(1)
    assert len(fs.listdir(fp)) == expected


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "node1"},
            with_zookeeper=True,
            with_hdfs=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "node2"},
            with_zookeeper=True,
            with_hdfs=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        if cluster.instances["node1"].is_debug_build():
            # https://github.com/ClickHouse/ClickHouse/issues/27814
            pytest.skip(
                "libhdfs3 calls rand function which does not pass harmful check in debug build"
            )
        logging.info("Cluster started")

        fs = HdfsClient(hosts=cluster.hdfs_ip)
        fs.mkdirs("/clickhouse1")
        fs.mkdirs("/clickhouse2")
        logging.info("Created HDFS directory")

        yield cluster
    finally:
        cluster.shutdown()


def test_hdfs_zero_copy_replication_insert(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    try:
        node1.query(
            """
            CREATE TABLE hdfs_test ON CLUSTER test_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/hdfs_test', '{replica}')
            ORDER BY (dt, id)
            SETTINGS storage_policy='hdfs_only'
            """
        )
        wait_for_hdfs_objects(
            cluster, "/clickhouse1", SHARDS * FILES_OVERHEAD_PER_TABLE
        )

        node1.query("INSERT INTO hdfs_test VALUES (now() - INTERVAL 3 DAY, 10)")
        node2.query("SYSTEM SYNC REPLICA hdfs_test")
        assert node1.query("SELECT count() FROM hdfs_test FORMAT Values") == "(1)"
        assert node2.query("SELECT count() FROM hdfs_test FORMAT Values") == "(1)"
        assert (
            node1.query("SELECT id FROM hdfs_test ORDER BY dt FORMAT Values") == "(10)"
        )
        assert (
            node2.query("SELECT id FROM hdfs_test ORDER BY dt FORMAT Values") == "(10)"
        )
        assert (
            node1.query(
                "SELECT partition_id,disk_name FROM system.parts WHERE table='hdfs_test' FORMAT Values"
            )
            == "('all','hdfs1')"
        )
        assert (
            node2.query(
                "SELECT partition_id,disk_name FROM system.parts WHERE table='hdfs_test' FORMAT Values"
            )
            == "('all','hdfs1')"
        )
        wait_for_hdfs_objects(
            cluster,
            "/clickhouse1",
            SHARDS * FILES_OVERHEAD_PER_TABLE + FILES_OVERHEAD_PER_PART_COMPACT,
        )
    finally:
        node1.query("DROP TABLE IF EXISTS hdfs_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS hdfs_test NO DELAY")


@pytest.mark.parametrize(
    ("storage_policy", "init_objects"),
    [("hybrid", 0), ("tiered", 0), ("tiered_copy", FILES_OVERHEAD_PER_TABLE)],
)
def test_hdfs_zero_copy_replication_single_move(cluster, storage_policy, init_objects):
    node1 = cluster.instances["node1"]
    try:
        node1.query(
            Template(
                """
            CREATE TABLE single_node_move_test (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/single_node_move_test', '{replica}')
            ORDER BY (dt, id)
            SETTINGS storage_policy='$policy'
            """
            ).substitute(policy=storage_policy)
        )
        wait_for_hdfs_objects(cluster, "/clickhouse1", init_objects)

        node1.query(
            "INSERT INTO single_node_move_test VALUES (now() - INTERVAL 3 DAY, 10), (now() - INTERVAL 1 DAY, 11)"
        )
        assert (
            node1.query(
                "SELECT id FROM single_node_move_test ORDER BY dt FORMAT Values"
            )
            == "(10),(11)"
        )

        node1.query(
            "ALTER TABLE single_node_move_test MOVE PARTITION ID 'all' TO VOLUME 'external'"
        )
        assert (
            node1.query(
                "SELECT partition_id,disk_name FROM system.parts WHERE table='single_node_move_test' FORMAT Values"
            )
            == "('all','hdfs1')"
        )
        assert (
            node1.query(
                "SELECT id FROM single_node_move_test ORDER BY dt FORMAT Values"
            )
            == "(10),(11)"
        )
        wait_for_hdfs_objects(
            cluster, "/clickhouse1", init_objects + FILES_OVERHEAD_PER_PART_COMPACT
        )

        node1.query(
            "ALTER TABLE single_node_move_test MOVE PARTITION ID 'all' TO VOLUME 'main'"
        )
        assert (
            node1.query(
                "SELECT id FROM single_node_move_test ORDER BY dt FORMAT Values"
            )
            == "(10),(11)"
        )
    finally:
        node1.query("DROP TABLE IF EXISTS single_node_move_test NO DELAY")


@pytest.mark.parametrize(
    ("storage_policy", "init_objects"),
    [("hybrid", 0), ("tiered", 0), ("tiered_copy", SHARDS * FILES_OVERHEAD_PER_TABLE)],
)
def test_hdfs_zero_copy_replication_move(cluster, storage_policy, init_objects):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    try:
        node1.query(
            Template(
                """
            CREATE TABLE move_test ON CLUSTER test_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/move_test', '{replica}')
            ORDER BY (dt, id)
            SETTINGS storage_policy='$policy'
            """
            ).substitute(policy=storage_policy)
        )
        wait_for_hdfs_objects(cluster, "/clickhouse1", init_objects)

        node1.query(
            "INSERT INTO move_test VALUES (now() - INTERVAL 3 DAY, 10), (now() - INTERVAL 1 DAY, 11)"
        )
        node2.query("SYSTEM SYNC REPLICA move_test")

        assert (
            node1.query("SELECT id FROM move_test ORDER BY dt FORMAT Values")
            == "(10),(11)"
        )
        assert (
            node2.query("SELECT id FROM move_test ORDER BY dt FORMAT Values")
            == "(10),(11)"
        )

        node1.query(
            "ALTER TABLE move_test MOVE PARTITION ID 'all' TO VOLUME 'external'"
        )
        wait_for_hdfs_objects(
            cluster, "/clickhouse1", init_objects + FILES_OVERHEAD_PER_PART_COMPACT
        )

        node2.query(
            "ALTER TABLE move_test MOVE PARTITION ID 'all' TO VOLUME 'external'"
        )
        assert (
            node1.query(
                "SELECT partition_id,disk_name FROM system.parts WHERE table='move_test' FORMAT Values"
            )
            == "('all','hdfs1')"
        )
        assert (
            node2.query(
                "SELECT partition_id,disk_name FROM system.parts WHERE table='move_test' FORMAT Values"
            )
            == "('all','hdfs1')"
        )
        assert (
            node1.query("SELECT id FROM move_test ORDER BY dt FORMAT Values")
            == "(10),(11)"
        )
        assert (
            node2.query("SELECT id FROM move_test ORDER BY dt FORMAT Values")
            == "(10),(11)"
        )
        wait_for_hdfs_objects(
            cluster, "/clickhouse1", init_objects + FILES_OVERHEAD_PER_PART_COMPACT
        )
    finally:
        node1.query("DROP TABLE IF EXISTS move_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS move_test NO DELAY")


@pytest.mark.parametrize(("storage_policy"), ["hybrid", "tiered", "tiered_copy"])
def test_hdfs_zero_copy_with_ttl_move(cluster, storage_policy):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    try:
        node1.query(
            Template(
                """
            CREATE TABLE ttl_move_test ON CLUSTER test_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/ttl_move_test', '{replica}')
            ORDER BY (dt, id)
            TTL dt + INTERVAL 2 DAY TO VOLUME 'external'
            SETTINGS storage_policy='$policy'
            """
            ).substitute(policy=storage_policy)
        )

        node1.query("INSERT INTO ttl_move_test VALUES (now() - INTERVAL 3 DAY, 10)")
        node1.query("INSERT INTO ttl_move_test VALUES (now() - INTERVAL 1 DAY, 11)")

        node1.query("OPTIMIZE TABLE ttl_move_test FINAL")
        node2.query("SYSTEM SYNC REPLICA ttl_move_test")

        assert node1.query("SELECT count() FROM ttl_move_test FORMAT Values") == "(2)"
        assert node2.query("SELECT count() FROM ttl_move_test FORMAT Values") == "(2)"
        assert (
            node1.query("SELECT id FROM ttl_move_test ORDER BY id FORMAT Values")
            == "(10),(11)"
        )
        assert (
            node2.query("SELECT id FROM ttl_move_test ORDER BY id FORMAT Values")
            == "(10),(11)"
        )
    finally:
        node1.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")


def test_hdfs_zero_copy_with_ttl_delete(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    try:
        node1.query(
            """
            CREATE TABLE ttl_delete_test ON CLUSTER test_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/ttl_delete_test', '{replica}')
            ORDER BY (dt, id)
            TTL dt + INTERVAL 2 DAY
            SETTINGS storage_policy='tiered'
            """
        )

        node1.query("INSERT INTO ttl_delete_test VALUES (now() - INTERVAL 3 DAY, 10)")
        node1.query("INSERT INTO ttl_delete_test VALUES (now() - INTERVAL 1 DAY, 11)")

        node1.query("OPTIMIZE TABLE ttl_delete_test FINAL")
        node2.query("SYSTEM SYNC REPLICA ttl_delete_test")

        assert node1.query("SELECT count() FROM ttl_delete_test FORMAT Values") == "(1)"
        assert node2.query("SELECT count() FROM ttl_delete_test FORMAT Values") == "(1)"
        assert (
            node1.query("SELECT id FROM ttl_delete_test ORDER BY id FORMAT Values")
            == "(11)"
        )
        assert (
            node2.query("SELECT id FROM ttl_delete_test ORDER BY id FORMAT Values")
            == "(11)"
        )
    finally:
        node1.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")
