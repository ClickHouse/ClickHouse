import pytest
from helpers.client import CommandRequest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_base_commands(started_cluster):
    _ = started_cluster

    command = CommandRequest(
        [
            started_cluster.server_bin_path,
            "keeper-client",
            "--host",
            str(cluster.get_instance_ip("zoo1")),
            "--port",
            str(cluster.zookeeper_port),
            "-q",
            "create test_create_zk_node1 testvalue1;create test_create_zk_node_2 testvalue2;get test_create_zk_node1;",
        ],
        stdin="",
    )

    assert command.get_answer() == "testvalue1\n"


def test_four_letter_word_commands(started_cluster):
    _ = started_cluster

    command = CommandRequest(
        [
            started_cluster.server_bin_path,
            "keeper-client",
            "--host",
            str(cluster.get_instance_ip("zoo1")),
            "--port",
            str(cluster.zookeeper_port),
            "-q",
            "ruok",
        ],
        stdin="",
    )

    assert command.get_answer() == "imok\n"
