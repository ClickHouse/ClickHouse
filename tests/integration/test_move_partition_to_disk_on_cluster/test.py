import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=10M", "/external:size=10M"],
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=10M", "/external:size=10M"],
    macros={"shard": 0, "replica": 2},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_move_partition_to_disk_on_cluster(start_cluster):
    for node in [node1, node2]:
        node.query(
            sql="CREATE TABLE test_local_table"
            "(x UInt64) "
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_local_table', '{replica}') "
            "ORDER BY tuple()"
            "SETTINGS storage_policy = 'jbod_with_external', temporary_directories_lifetime=1;",
        )

    node1.query("INSERT INTO test_local_table VALUES (0)")
    node1.query("SYSTEM SYNC REPLICA test_local_table", timeout=30)

    try:
        node1.query(
            sql="ALTER TABLE test_local_table ON CLUSTER 'test_cluster' MOVE PARTITION tuple() TO DISK 'jbod1';",
        )
    except QueryRuntimeException:
        pass

    for node in [node1, node2]:
        assert (
            node.query(
                "SELECT partition_id, disk_name FROM system.parts WHERE table = 'test_local_table' FORMAT Values"
            )
            == "('all','jbod1')"
        )

    node1.query(
        sql="ALTER TABLE test_local_table ON CLUSTER 'test_cluster' MOVE PARTITION tuple() TO DISK 'external';",
    )

    for node in [node1, node2]:
        assert (
            node.query(
                "SELECT partition_id, disk_name FROM system.parts WHERE table = 'test_local_table' FORMAT Values"
            )
            == "('all','external')"
        )

    node1.query(
        sql="ALTER TABLE test_local_table ON CLUSTER 'test_cluster' MOVE PARTITION tuple() TO VOLUME 'main';",
    )

    for node in [node1, node2]:
        assert (
            node.query(
                "SELECT partition_id, disk_name FROM system.parts WHERE table = 'test_local_table' FORMAT Values"
            )
            == "('all','jbod1')"
        )
