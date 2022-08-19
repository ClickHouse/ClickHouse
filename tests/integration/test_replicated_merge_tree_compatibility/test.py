import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="20.12.4.5",
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="20.12.4.5",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_replicated_merge_tree_defaults_compatibility(started_cluster):
    # This test checks, that result of parsing list of columns with defaults
    # from 'CREATE/ATTACH' is compatible with parsing from zookeeper metadata on different versions.
    # We create table and write 'columns' node in zookeeper with old version, than restart with new version
    # drop and try recreate one replica. During startup of table structure is checked between 'CREATE' query and zookeeper.

    create_query = """
        CREATE TABLE test.table
        (
            a UInt32,
            b String DEFAULT If(a = 0, 'true', 'false'),
            c String DEFAULT Cast(a, 'String')
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/table', '{replica}')
        ORDER BY a
    """

    for node in (node1, node2):
        node.query("CREATE DATABASE test ENGINE = Ordinary")
        node.query(create_query.format(replica=node.name))

    node1.query("DETACH TABLE test.table")
    node2.query("SYSTEM DROP REPLICA 'node1' FROM TABLE test.table")
    node1.exec_in_container(
        ["bash", "-c", "rm /var/lib/clickhouse/metadata/test/table.sql"]
    )
    node1.exec_in_container(["bash", "-c", "rm -r /var/lib/clickhouse/data/test/table"])

    zk = cluster.get_kazoo_client("zoo1")
    exists_replica_1 = zk.exists("/clickhouse/tables/test/table/replicas/node1")
    assert exists_replica_1 == None

    node1.restart_with_latest_version()
    node2.restart_with_latest_version()

    node1.query(create_query.format(replica=1))
    node1.query("EXISTS TABLE test.table") == "1\n"
