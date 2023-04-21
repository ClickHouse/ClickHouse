import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/another_user.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE mydb")
        node.query("CREATE TABLE mydb.table1(x UInt32) ENGINE=Log")
        node.query("CREATE TABLE mydb.table2(x UInt32) ENGINE=Log")
        yield cluster

    finally:
        cluster.shutdown()


def test_full_read_from_system_tables():
    assert node.query("SELECT name FROM system.tables WHERE database = 'mydb'") == TSV([
        'table1',
        'table2',
    ])

    assert node.query("SELECT name FROM system.tables WHERE database = 'mydb'", user="another") == TSV([
        'table1',
        'table2',
    ])

    assert node.query("SELECT name FROM system.tables WHERE database = 'mydb'", user="restricted") == TSV([])
