import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.keeper_utils import KeeperClient


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
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


@pytest.fixture(scope="function")
def client(started_cluster):
    with KeeperClient.from_cluster(cluster, "zoo1") as keeper_client:
        yield keeper_client


def test_big_family(client: KeeperClient):
    client.touch("/test_big_family")
    client.touch("/test_big_family/1")
    client.touch("/test_big_family/1/1")
    client.touch("/test_big_family/1/2")
    client.touch("/test_big_family/1/3")
    client.touch("/test_big_family/1/4")
    client.touch("/test_big_family/1/5")
    client.touch("/test_big_family/2")
    client.touch("/test_big_family/2/1")
    client.touch("/test_big_family/2/2")
    client.touch("/test_big_family/2/3")

    response = client.find_big_family("/test_big_family")

    assert response == TSV(
        [
            ["/test_big_family/1", "5"],
            ["/test_big_family/2", "3"],
            ["/test_big_family/2/3", "0"],
            ["/test_big_family/2/2", "0"],
            ["/test_big_family/2/1", "0"],
            ["/test_big_family/1/5", "0"],
            ["/test_big_family/1/4", "0"],
            ["/test_big_family/1/3", "0"],
            ["/test_big_family/1/2", "0"],
            ["/test_big_family/1/1", "0"],
        ]
    )

    response = client.find_big_family("/test_big_family", 1)

    assert response == TSV(
        [
            ["/test_big_family/1", "5"],
        ]
    )


def test_find_super_nodes(client: KeeperClient):
    client.touch("/test_find_super_nodes")
    client.touch("/test_find_super_nodes/1")
    client.touch("/test_find_super_nodes/1/1")
    client.touch("/test_find_super_nodes/1/2")
    client.touch("/test_find_super_nodes/1/3")
    client.touch("/test_find_super_nodes/1/4")
    client.touch("/test_find_super_nodes/1/5")
    client.touch("/test_find_super_nodes/2")
    client.touch("/test_find_super_nodes/2/1")
    client.touch("/test_find_super_nodes/2/2")
    client.touch("/test_find_super_nodes/2/3")
    client.touch("/test_find_super_nodes/2/4")

    client.cd("/test_find_super_nodes")

    response = client.find_super_nodes(4)
    assert response == TSV(
        [
            ["/test_find_super_nodes/1", "5"],
            ["/test_find_super_nodes/2", "4"],
        ]
    )


def test_delete_stale_backups(client: KeeperClient):
    client.touch("/clickhouse")
    client.touch("/clickhouse/backups")
    client.touch("/clickhouse/backups/1")
    client.touch("/clickhouse/backups/1/stage")
    client.touch("/clickhouse/backups/1/stage/alive123")
    client.touch("/clickhouse/backups/2")
    client.touch("/clickhouse/backups/2/stage")
    client.touch("/clickhouse/backups/2/stage/dead123")

    response = client.delete_stale_backups()

    assert response == (
        'Found backup "/clickhouse/backups/1", checking if it\'s active\n'
        'Backup "/clickhouse/backups/1" is active, not going to delete\n'
        'Found backup "/clickhouse/backups/2", checking if it\'s active\n'
        'Backup "/clickhouse/backups/2" is not active, deleting it'
    )

    assert client.ls("/clickhouse/backups") == ["1"]


def test_base_commands(client: KeeperClient):
    client.create("/test_create_zk_node1", "testvalue1")
    client.create("/test_create_zk_node_2", "testvalue2")
    assert client.get("/test_create_zk_node1") == "testvalue1"

    client.create("/123", "1=2")
    client.create("/123/321", "'foo;bar'")
    assert client.get("/123") == "1=2"
    assert client.get("/123/321") == "foo;bar"


def test_four_letter_word_commands(client: KeeperClient):
    assert client.execute_query("ruok") == "imok"
