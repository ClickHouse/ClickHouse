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


def test_allow_read_from_system_tables():
    assert node.query("SELECT name FROM system.tables WHERE database = 'mydb'") == TSV(
        [
            "table1",
            "table2",
        ]
    )

    assert node.query(
        "SELECT name FROM system.tables WHERE database = 'mydb'", user="another"
    ) == TSV(
        [
            "table1",
            "table2",
        ]
    )


def test_user_grants_from_config():
    assert node.query("SHOW GRANTS FOR another") == TSV(
        [
            "GRANT SHOW ON *.* TO another",
            "GRANT CREATE ON *.* TO another WITH GRANT OPTION",
            "GRANT SELECT ON system.* TO another",
            "REVOKE CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY ON system.* FROM another",
        ]
    )

    assert node.query("SHOW GRANTS FOR admin_user") == TSV(
        [
            "GRANT admin_role TO admin_user",
        ]
    )


def test_role_grants_from_config():
    assert node.query("SHOW GRANTS FOR test_role") == TSV(
        [
            "GRANT SHOW ON *.* TO test_role",
            "GRANT CREATE ON *.* TO test_role WITH GRANT OPTION",
            "REVOKE SHOW ON system.* FROM test_role",
        ]
    )
