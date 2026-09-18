import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config_replicated_default.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_replicated_default.xml"],
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


def test_default_database_engine_replicated(started_cluster):
    """Verify default_database_engine=Replicated behaviour:
    1. CREATE DATABASE without ENGINE creates a Replicated database with correct
       default params and working replication.
    2. An explicit ENGINE clause is still honoured."""

    # -- Part 1: default Replicated database creation & replication ----------

    node1.query("CREATE DATABASE test_default_replicated ON CLUSTER 'default'")

    engine = node1.query(
        "SELECT engine FROM system.databases WHERE name = 'test_default_replicated'"
    ).strip()
    assert engine == "Replicated", f"Expected Replicated engine, got {engine}"

    # Verify default params from DatabaseReplicatedSettings
    shard_name = node1.query(
        "SELECT database_shard_name FROM system.clusters WHERE cluster = 'test_default_replicated' AND host_name = 'node1'"
    ).strip()
    replica_name = node1.query(
        "SELECT database_replica_name FROM system.clusters WHERE cluster = 'test_default_replicated' AND host_name = 'node1'"
    ).strip()
    assert shard_name == "1", f"Expected shard '1', got '{shard_name}'"
    assert replica_name == "1", f"Expected replica '1', got '{replica_name}'"

    # Verify replication works
    node1.query(
        "CREATE TABLE test_default_replicated.test_table (id UInt64, value String) "
        "ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    node1.query(
        "INSERT INTO test_default_replicated.test_table VALUES (1, 'hello')"
    )

    assert_eq_with_retry(
        node2,
        "SELECT id, value FROM test_default_replicated.test_table ORDER BY id",
        "1\thello\n",
    )

    node1.query("DROP DATABASE test_default_replicated ON CLUSTER 'default' SYNC")

    # -- Part 2: explicit ENGINE clause is still honoured --------------------

    node1.query("CREATE DATABASE test_explicit_atomic ENGINE = Atomic")

    engine = node1.query(
        "SELECT engine FROM system.databases WHERE name = 'test_explicit_atomic'"
    ).strip()
    assert engine == "Atomic", f"Expected Atomic engine, got {engine}"

    node1.query("DROP DATABASE test_explicit_atomic SYNC")

    # -- Part 3: predefined and default databases are not affected -----------

    # The 'default' database should remain Atomic even with the setting enabled
    default_engine = node1.query(
        "SELECT engine FROM system.databases WHERE name = 'default'"
    ).strip()
    assert default_engine == "Atomic", (
        f"Expected 'default' database to stay Atomic, got {default_engine}"
    )

    # Predefined databases (system, information_schema, INFORMATION_SCHEMA) should not be Replicated
    for db_name in ["system", "information_schema", "INFORMATION_SCHEMA"]:
        eng = node1.query(
            f"SELECT engine FROM system.databases WHERE name = '{db_name}'"
        ).strip()
        assert eng != "Replicated", (
            f"Predefined database '{db_name}' should not be Replicated, got {eng}"
        )
