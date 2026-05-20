import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query(
                "CREATE TABLE local_table(id UInt32, data Object('json')) ENGINE = MergeTree ORDER BY id",
                settings={"allow_experimental_object_type": 1},
            )
            node.query(
                "CREATE TABLE dist_table AS local_table ENGINE = Distributed(test_cluster, default, local_table)",
                settings={"allow_experimental_object_type": 1},
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_distributed_type_object(started_cluster):
    node1.query("TRUNCATE TABLE local_table")
    node2.query("TRUNCATE TABLE local_table")

    node1.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 1, "data": {"k1": 10}}'
    )
    node2.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 2, "data": {"k1": 20}}'
    )

    expected = TSV("10\n20\n")
    assert TSV(node1.query("SELECT data.k1 FROM dist_table ORDER BY id")) == expected

    node1.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 3, "data": {"k1": "str1"}}'
    )

    expected = TSV("10\n20\nstr1\n")
    assert TSV(node1.query("SELECT data.k1 FROM dist_table ORDER BY id")) == expected

    node1.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 4, "data": {"k2": 30}}'
    )

    expected = TSV("10\t0\n20\t0\nstr1\t0\n\t30")
    assert (
        TSV(node1.query("SELECT data.k1, data.k2 FROM dist_table ORDER BY id"))
        == expected
    )

    expected = TSV("120\n")
    assert (
        TSV(
            node1.query(
                "SELECT sum(data.k2 * id) FROM dist_table SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0"
            )
        )
        == expected
    )

    node1.query("TRUNCATE TABLE local_table")
    node2.query("TRUNCATE TABLE local_table")

    node1.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 1, "data": {"k1": "aa", "k2": {"k3": "bb", "k4": "c"}}} {"id": 2, "data": {"k1": "ee", "k5": "ff"}};'
    )
    node2.query(
        'INSERT INTO local_table FORMAT JSONEachRow {"id": 3, "data": {"k5":"foo"}};'
    )

    expected = TSV(
        """
1\taa\tbb\tc\t
2\tee\t\t\tff
3\t\t\t\tfoo"""
    )

    # The following query is not supported by analyzer now
    assert (
        TSV(
            node1.query(
                "SELECT id, data.k1, data.k2.k3, data.k2.k4, data.k5 FROM dist_table ORDER BY id SETTINGS enable_analyzer = 0"
            )
        )
        == expected
    )
