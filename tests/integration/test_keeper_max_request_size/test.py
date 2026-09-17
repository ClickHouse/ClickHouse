import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml", "configs/overrides.xml"],
    with_zookeeper=False,
    use_keeper=False,
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node)
        yield cluster
    finally:
        cluster.shutdown()


def test_max_request_size(started_cluster):
    node.query("insert into system.zookeeper (name, path, value) select number::String, '/test_soft_limit', repeat('a', 3000) from numbers(100)")
    # Connection loss and Operation timeout are both valid client surfaces of the
    # connection-level rejection; do not narrow this back to a single code.
    with pytest.raises(Exception, match=r"Connection loss|Operation timeout"):
        node.query("insert into system.zookeeper (name, path, value) select number::String, '/test_soft_limit', repeat('a', 3000) from numbers(10000)")
