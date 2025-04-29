import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])

create_local_table_sql = """
CREATE TABLE default.local_table
(
    `ID` UInt32,
    `MayThrow` ALIAS throwIf(ID = 2, 'Simulated error: Table is damaged')
)
ENGINE = MergeTree
ORDER BY ID;
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(create_local_table_sql)
        node2.query(create_local_table_sql)
        node3.query(create_local_table_sql)

        node1.query(
            """
CREATE TABLE default.distributed_table_unavailable_or_exception
(
    `ID` UInt32,
    `MayThrow` UInt32
)
ENGINE = Distributed(test_cluster, default, local_table, rand())
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode='unavailable_or_exception';
"""
        )

        node1.query(
            """
CREATE TABLE default.distributed_table_unavailable
(
    `ID` UInt32,
    `MayThrow` UInt32
)
ENGINE = Distributed(test_cluster, default, local_table, rand())
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode='unavailable';
"""
        )

        node1.query(
            """
CREATE TABLE default.distributed_table_no_skip_unavailable_shards
(
    `ID` UInt32,
    `MayThrow` UInt32
)
ENGINE = Distributed(test_cluster, default, local_table, rand())
SETTINGS skip_unavailable_shards_mode='unavailable_or_exception';
"""
        )

        node1.query("INSERT INTO default.local_table VALUES (1)")
        node2.query("INSERT INTO default.local_table VALUES (2)")
        node3.query("INSERT INTO default.local_table VALUES (3)")

        yield cluster

    finally:
        cluster.shutdown()

def split_and_sort(result: str) -> list[str]:
    ids = result.rstrip().split('\n')
    ids.sort()
    return ids

def exception_raised(f):
    try:
        f()
    except:
        pass
    else:
        assert False, "exception expected"

def test_skip_unavailable_shards_mode(started_cluster):
    assert split_and_sort(node1.query("SELECT ID, MayThrow FROM default.distributed_table_unavailable_or_exception")) == ["1\t0", "3\t0"]

    exception_raised(lambda _: node1.query("SELECT ID, MayThrow FROM default.distributed_table_unavailable"))

    assert split_and_sort(node1.query("SELECT ID, MayThrow FROM default.distributed_table_unavailable SETTINGS skip_unavailable_shards_mode='unavailable_or_exception'")) == ["1\t0", "3\t0"]

    exception_raised(lambda _: node1.query("SELECT ID, MayThrow FROM default.distributed_table_no_skip_unavailable_shards"))
