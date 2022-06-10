import pytest
import helpers.client
import helpers.cluster


cluster = helpers.cluster.ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_merge_tree_truncate(started_cluster):
    node1.query(
        "CREATE TABLE mt_truncate (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )

    node1.query("INSERT INTO mt_truncate VALUES (1, 'a')")
    node1.query("INSERT INTO mt_truncate VALUES (2, 'b')")
    node1.query("INSERT INTO mt_truncate VALUES (3, 'c')")

    node1.query("OPTIMIZE TABLE mt_truncate FINAL")
    node1.query("TRUNCATE TABLE mt_truncate")

    node1.restart_clickhouse(kill=True)

    assert node1.query("SELECT count() FROM mt_truncate") == "0\n"
    assert (
        node1.query("SELECT count() FROM system.parts WHERE table = 'mt_truncate'")
        == "0\n"
    )
