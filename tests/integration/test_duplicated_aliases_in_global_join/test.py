import pytest

import helpers.client as client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_duplicated_alias_in_join(started_cluster):
    
    for node in [node1, node2]:
        node.query(f"DROP TABLE IF EXISTS t1")
        node.query(f"DROP TABLE IF EXISTS t2")
        node.query(f"create table t1 ( a Int32 ) engine=MergeTree order by a as select number from numbers(4)")
        node.query(f"create table t2 ( a Int32 ) engine=MergeTree order by a as select number from numbers(4)")

    expected = "1\n1\n1\n1\n"
    assert TSV(node1.query(
        """
        SELECT 1
        FROM
        (
            SELECT a AS a1
            FROM
            (
                SELECT a AS a1
                FROM t1
            ) AS a
            LEFT JOIN
            (
                SELECT a
                FROM t2
            ) AS b ON a.a1 = b.a
        ) AS a
        LEFT JOIN
        (
            SELECT a AS a1
            FROM t1
        ) AS b ON a.a1 = b.a1 settings allow_experimental_analyzer=0,allow_experimental_parallel_reading_from_replicas=1,max_parallel_replicas=2
        """
    )) == TSV(expected)

