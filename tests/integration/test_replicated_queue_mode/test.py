import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        "node1",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 1},
    ),
    cluster.add_instance(
        "node2",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 2},
    ),
    cluster.add_instance(
        "node3",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 3},
    ),
]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_inserts(started_cluster):
    nodes[0].query(
        "CREATE TABLE queue_mode_test ON CLUSTER 'cluster' (a UInt64, b UInt64) ENGINE=ReplicatedMergeTree() ORDER BY a SETTINGS queue_mode=1"
    )

    # disable faults because in queue_mode internal retry-loop is disabled
    for i in range(3):
        nodes[i].query(
            f"INSERT INTO queue_mode_test (*) select {i + 1}, number from numbers({i + 1}) SETTINGS insert_keeper_fault_injection_probability=0"
        )

    for node in nodes:
        node.query("SYSTEM SYNC REPLICA queue_mode_test")

    for i in range(3):
        assert nodes[i].query(
            f"SELECT _queue_block_number FROM queue_mode_test WHERE a = {i + 1}"
        ).split() == [f"{i}"] * (i + 1)

    nodes[1].query(
        "INSERT INTO queue_mode_test (*) select 4, number from numbers(4) SETTINGS insert_keeper_fault_injection_probability=0"
    )
    nodes[0].query("OPTIMIZE TABLE queue_mode_test")

    for node in nodes:
        node.query("SYSTEM SYNC REPLICA queue_mode_test")

    for node in nodes:
        for i in range(4):
            assert node.query(
                f"SELECT _queue_block_number FROM queue_mode_test WHERE a = {i + 1}"
            ).split() == [f"{i}"] * (i + 1)
