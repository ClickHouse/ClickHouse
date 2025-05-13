import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def create_database(node, db_name: str, engine: str, comment: str):
    if engine == "Atomic":
        node.query(
            f"CREATE DATABASE {db_name} ON CLUSTER test_cluster ENGINE = Atomic COMMENT '{comment}'"
        )
        return

    if engine == "Lazy":
        node.query(
            f"CREATE DATABASE {db_name} ON CLUSTER test_cluster ENGINE = Lazy(1) COMMENT '{comment}'"
        )
        return

    if engine == "Memory":
        node.query(
            f"CREATE DATABASE {db_name} ON CLUSTER test_cluster ENGINE = Memory COMMENT '{comment}'"
        )
        return

    if engine == "Replicated":
        node.query(
            f"CREATE DATABASE {db_name} ON CLUSTER test_cluster "
            + r"ENGINE = Replicated('/db/test', '{shard}', '{replica}')"
            + f" COMMENT '{comment}'"
        )
        return

    raise QueryRuntimeException(f"Not supported engine {engine}")


@pytest.mark.parametrize("engine", ["Atomic", "Lazy", "Memory", "Replicated"])
def test_alter_database_comment(started_cluster, engine):
    node1.query("DROP DATABASE IF EXISTS test")
    node2.query("DROP DATABASE IF EXISTS test")

    create_database(node1, "test", engine, "initial comment")

    modified_comment = "modified comment"
    node1.query(
        f"ALTER DATABASE test ON CLUSTER test_cluster (MODIFY COMMENT '{modified_comment}')"
    )

    assert (
        node1.query("SELECT comment FROM system.databases WHERE name='test'").strip()
        == modified_comment
    )
    assert (
        node2.query("SELECT comment FROM system.databases WHERE name='test'").strip()
        == modified_comment
    )

    node1.query(f"DETACH DATABASE test ON CLUSTER test_cluster")

    assert (
        node1.query("SELECT count() FROM system.databases WHERE name='test'").strip()
        == "0"
    )
    assert (
        node2.query("SELECT count() FROM system.databases WHERE name='test'").strip()
        == "0"
    )

    node1.query(f"ATTACH DATABASE test ON CLUSTER test_cluster")

    assert (
        node1.query("SELECT comment FROM system.databases WHERE name='test'").strip()
        == modified_comment
    )
    assert (
        node2.query("SELECT comment FROM system.databases WHERE name='test'").strip()
        == modified_comment
    )

    node1.query("DROP DATABASE IF EXISTS test")
    node2.query("DROP DATABASE IF EXISTS test")
