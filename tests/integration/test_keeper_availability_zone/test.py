import pytest

from helpers.cluster import ClickHouseCluster
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


def test_get_availability_zone():
    with KeeperClient.from_cluster(cluster, "zoo1") as client1:
        assert client1.get("/keeper/availability_zone") == "az-zoo1"

    # Keeper2 set enable_auto_detection_on_cloud to true, but is ignored and <value>az-zoo2</value> is used.
    with KeeperClient.from_cluster(cluster, "zoo2") as client2:
        assert client2.get("/keeper/availability_zone") == "az-zoo2"
        assert "availability_zone" in client2.ls("/keeper")

    # keeper3 is not configured with availability_zone value.
    with KeeperClient.from_cluster(cluster, "zoo3") as client3:
        with pytest.raises(Exception):
            client3.get("/keeper/availability_zone")
