import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config_custom_replica_params.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_custom_replica_params.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_custom_replica_params(started_cluster):
    """Verify that the Replicated database created by default uses all three
    customized default parameters (path, shard, replica) with _suffix from
    the config, and that replication works correctly between nodes."""

    node1.query("CREATE DATABASE test_custom_params ON CLUSTER 'default'")

    # Verify engine is Replicated
    engine = node1.query(
        "SELECT engine FROM system.databases WHERE name = 'test_custom_params'"
    ).strip()
    assert engine == "Replicated", f"Expected Replicated engine, got {engine}"

    # Verify custom shard_name and replica_name with _suffix on both nodes
    for node, host, expected_replica in [(node1, "node1", "1_suffix"), (node2, "node2", "2_suffix")]:
        shard_name = node.query(
            f"SELECT database_shard_name FROM system.clusters WHERE cluster = 'test_custom_params' AND host_name = '{host}'"
        ).strip()
        assert shard_name == "1_suffix", f"Expected shard '1_suffix', got '{shard_name}'"

        replica_name = node.query(
            f"SELECT database_replica_name FROM system.clusters WHERE cluster = 'test_custom_params' AND host_name = '{host}'"
        ).strip()
        assert (
            replica_name == expected_replica
        ), f"Expected replica '{expected_replica}', got '{replica_name}'"

    # Verify replication works with custom params
    node1.query(
        "CREATE TABLE test_custom_params.t1 (id UInt64, value String) "
        "ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    node1.query("INSERT INTO test_custom_params.t1 VALUES (1, 'custom_suffix')")

    assert_eq_with_retry(
        node2,
        "SELECT id, value FROM test_custom_params.t1 ORDER BY id",
        "1\tcustom_suffix\n",
    )

    node1.query("DROP DATABASE test_custom_params ON CLUSTER 'default' SYNC")
