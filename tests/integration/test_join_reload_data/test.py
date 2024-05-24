
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], user_configs=["configs/users.xml"],)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);;"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_start_join_reload_data(started_cluster):
    # Write data
    node1.query("INSERT INTO id_val_join VALUES (1,21)(2,22)(3,23);")
    assert node1.query("SELECT count() FROM id_val_join").rstrip() == "3"

    # Detach Table, Attach Table
    node1.query("DETACH TABLE id_val_join;")
    node1.query("ATTACH TABLE id_val_join;")
    assert node1.query("SELECT count() FROM id_val_join").rstrip() == "0"

    # Reload data
    node1.query("SYSTEM RELOAD JOIN id_val_join;")
    assert node1.query("SELECT count() FROM id_val_join").rstrip() == "3"
