import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replication():
    node1.query("CREATE TYPE ReplId AS UInt64")
    node1.query("CREATE TYPE ReplPair(K, V) AS Tuple(K, V)")

    # Types created on one node appear on the other one through ZooKeeper.
    assert_eq_with_retry(node2, "SHOW TYPES", "ReplId\nReplPair\n")
    assert (
        node2.query("SELECT toTypeName(CAST((1, 'a'), 'ReplPair(ReplId, String)'))")
        == "Tuple(UInt64, String)\n"
    )
    assert (
        node2.query("SELECT create_query FROM system.user_defined_types WHERE name = 'ReplPair'")
        == "CREATE TYPE ReplPair(K, V) AS Tuple(K, V)\n"
    )

    # Functions and types are stored under the same ZooKeeper path but are separate namespaces.
    node1.query("CREATE FUNCTION ReplId AS (x) -> x + 1")
    assert_eq_with_retry(node2, "SELECT ReplId(1)", "2\n")
    assert node2.query("SELECT toTypeName(CAST(1, 'ReplId'))") == "UInt64\n"
    node1.query("DROP FUNCTION ReplId")
    assert_eq_with_retry(node2, "SHOW TYPES", "ReplId\nReplPair\n")

    # A replacement made on one node is seen on the other one.
    node2.query("CREATE TYPE OR REPLACE ReplId AS String")
    assert_eq_with_retry(
        node1,
        "SELECT base_type FROM system.user_defined_types WHERE name = 'ReplId'",
        "String\n",
    )

    # The definitions are loaded from ZooKeeper on start.
    node1.restart_clickhouse()
    assert node1.query("SHOW TYPES") == "ReplId\nReplPair\n"
    assert node1.query("SELECT toTypeName(CAST('x', 'ReplId'))") == "String\n"

    node2.query("DROP TYPE ReplPair")
    node2.query("DROP TYPE ReplId")
    assert_eq_with_retry(node1, "SHOW TYPES", "")
