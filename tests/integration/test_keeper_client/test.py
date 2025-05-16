import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import KeeperClient, KeeperException
from helpers.test_tools import TSV

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
            ["/test_big_family", "11"],
            ["/test_big_family/1", "6"],
            ["/test_big_family/2", "4"],
            ["/test_big_family/2/3", "1"],
            ["/test_big_family/2/2", "1"],
            ["/test_big_family/2/1", "1"],
            ["/test_big_family/1/5", "1"],
            ["/test_big_family/1/4", "1"],
            ["/test_big_family/1/3", "1"],
            ["/test_big_family/1/2", "1"],
        ]
    )

    response = client.find_big_family("/test_big_family", 2)
    assert response == TSV(
        [
            ["/test_big_family", "11"],
            ["/test_big_family/1", "6"],
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

    # The order of the response is not guaranteed, so we need to sort it
    normalized_response = response.strip().split("\n")
    normalized_response.sort()

    assert TSV(normalized_response) == TSV(
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
    client.create("/123/321", "foo;bar")
    assert client.get("/123") == "1=2"
    assert client.get("/123/321") == "foo;bar"


def test_four_letter_word_commands(client: KeeperClient):
    assert client.execute_query("ruok") == "imok"


def test_rm_with_version(client: KeeperClient):
    node_path = "/test_rm_with_version_node"
    client.create(node_path, "value")
    assert client.get(node_path) == "value"

    with pytest.raises(KeeperException) as ex:
        client.rm(node_path, 1)

    ex_as_str = str(ex)
    assert "Coordination error: Bad version" in ex_as_str
    assert node_path in ex_as_str
    assert client.get(node_path) == "value"

    client.rm(node_path, 0)

    with pytest.raises(KeeperException) as ex:
        client.get(node_path)

    ex_as_str = str(ex)
    assert "node doesn't exist" in ex_as_str
    assert node_path in ex_as_str


def test_rm_without_version(client: KeeperClient):
    node_path = "/test_rm_with_version_node"
    client.create(node_path, "value")
    assert client.get(node_path) == "value"

    client.rm(node_path)

    with pytest.raises(KeeperException) as ex:
        client.get(node_path)

    ex_as_str = str(ex)
    assert "node doesn't exist" in ex_as_str
    assert node_path in ex_as_str


def test_set_with_version(client: KeeperClient):
    node_path = "/test_set_with_version_node"
    client.create(node_path, "value")
    assert client.get(node_path) == "value"

    client.set(node_path, "value1", 0)
    assert client.get(node_path) == "value1"

    with pytest.raises(KeeperException) as ex:
        client.set(node_path, "value2", 2)

    ex_as_str = str(ex)
    assert "Coordination error: Bad version" in ex_as_str
    assert node_path in ex_as_str
    assert client.get(node_path) == "value1"

    client.set(node_path, "value2", 1)
    assert client.get(node_path) == "value2"


def test_set_without_version(client: KeeperClient):
    node_path = "/test_set_without_version_node"
    client.create(node_path, "value")
    assert client.get(node_path) == "value"

    client.set(node_path, "value1")
    assert client.get(node_path) == "value1"

    client.set(node_path, "value2")
    assert client.get(node_path) == "value2"


def test_quoted_argument_parsing(client: KeeperClient):
    node_path = "/test_quoted_argument_parsing_node"
    client.create(node_path, "value")

    client.execute_query(f"set '{node_path}' 'value1 with some whitespace'")
    assert client.get(node_path) == "value1 with some whitespace"

    client.execute_query(f"set '{node_path}' 'value2 with some whitespace' 1")
    assert client.get(node_path) == "value2 with some whitespace"

    client.execute_query(f"set '{node_path}' \"value3 with some whitespace\"")
    assert client.get(node_path) == "value3 with some whitespace"

    client.execute_query(f"set '{node_path}' \"value4 with some whitespace\" 3")
    assert client.get(node_path) == "value4 with some whitespace"


def get_direct_children_number(client: KeeperClient):
    client.touch("/get_direct_children_number")
    client.touch("/get_direct_children_number/1")
    client.touch("/get_direct_children_number/1/1")
    client.touch("/get_direct_children_number/1/2")
    client.touch("/get_direct_children_number/2")
    client.touch("/get_direct_children_number/2/1")
    client.touch("/get_direct_children_number/2/2")

    assert client.get_direct_children_number("/get_direct_children_number") == "2"


def test_get_all_children_number(client: KeeperClient):
    client.touch("/test_get_all_children_number")
    client.touch("/test_get_all_children_number/1")
    client.touch("/test_get_all_children_number/1/1")
    client.touch("/test_get_all_children_number/1/2")
    client.touch("/test_get_all_children_number/1/3")
    client.touch("/test_get_all_children_number/1/4")
    client.touch("/test_get_all_children_number/1/5")
    client.touch("/test_get_all_children_number/2")
    client.touch("/test_get_all_children_number/2/1")
    client.touch("/test_get_all_children_number/2/2")
    client.touch("/test_get_all_children_number/2/3")
    client.touch("/test_get_all_children_number/2/4")

    assert client.get_all_children_number("/test_get_all_children_number") == "11"
