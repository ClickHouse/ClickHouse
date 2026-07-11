import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config_zk_include_test.xml"],
    with_zookeeper=True,
    extra_args="--shutdown_wait_unfinished 3",
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_config_not_overriding_args(start_cluster):
    assert (
        node.query(
            "select value from system.server_settings where name = 'shutdown_wait_unfinished'"
        )
        == "3\n"
    )
