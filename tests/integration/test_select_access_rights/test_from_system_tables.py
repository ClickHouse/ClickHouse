import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml"],
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
        node.query("CREATE TABLE table2(x UInt32) ENGINE=Log")
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_after_test():
    try:
        node.query("CREATE USER OR REPLACE sqluser")
        yield
    finally:
        pass


def test_system_db():
    assert node.query("SELECT count()>0 FROM system.settings") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users") == "1\n"
    assert node.query("SELECT count()>0 FROM system.clusters") == "1\n"
    assert node.query("SELECT count() FROM system.tables WHERE name='table1'") == "1\n"
    assert node.query("SELECT count() FROM system.tables WHERE name='table2'") == "1\n"

    assert node.query("SELECT count()>0 FROM system.settings", user="another") == "1\n"

    expected_error = (
        "necessary to have the grant SELECT for at least one column on system.users"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count()>0 FROM system.users", user="another"
    )

    expected_error = (
        "necessary to have the grant SELECT for at least one column on system.clusters"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count()>0 FROM system.clusters", user="another"
    )
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table1'", user="another"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table2'", user="another"
        )
        == "0\n"
    )

    assert node.query("SELECT count()>0 FROM system.settings", user="sqluser") == "1\n"

    expected_error = (
        "necessary to have the grant SELECT for at least one column on system.users"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count()>0 FROM system.users", user="sqluser"
    )

    expected_error = (
        "necessary to have the grant SELECT for at least one column on system.clusters"
    )
    assert node.query_and_get_error(
        "SELECT count()>0 FROM system.clusters", user="sqluser"
    )

    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table1'", user="sqluser"
        )
        == "0\n"
    )
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table2'", user="sqluser"
        )
        == "0\n"
    )

    node.query("GRANT SELECT ON system.users TO sqluser")
    node.query("GRANT SELECT ON system.clusters TO sqluser")
    node.query("GRANT SHOW ON mydb.table1 TO sqluser")
    node.query("GRANT SHOW ON table2 TO sqluser")
    assert node.query("SELECT count()>0 FROM system.settings", user="sqluser") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users", user="sqluser") == "1\n"
    assert node.query("SELECT count()>0 FROM system.clusters", user="sqluser") == "1\n"
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table1'", user="sqluser"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE name='table2'", user="sqluser"
        )
        == "1\n"
    )

    node.query("REVOKE ALL ON *.* FROM sqluser")
    node.query("GRANT SHOW USERS ON *.* TO sqluser")
    assert node.query("SELECT count()>0 FROM system.users", user="sqluser") == "1\n"


def test_information_schema():
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table1'"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table2'"
        )
        == "1\n"
    )

    expected_error = (
        "necessary to have the grant SELECT(table_name) ON information_schema.tables"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count() FROM information_schema.tables WHERE table_name='table1'",
        user="another",
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count() FROM information_schema.tables WHERE table_name='table2'",
        user="another",
    )

    assert expected_error in node.query_and_get_error(
        "SELECT count() FROM information_schema.tables WHERE table_name='table1'",
        user="sqluser",
    )
    assert expected_error in node.query_and_get_error(
        "SELECT count() FROM information_schema.tables WHERE table_name='table2'",
        user="sqluser",
    )

    node.query("GRANT SELECT ON information_schema.* TO sqluser")
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table1'",
            user="sqluser",
        )
        == "0\n"
    )
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table2'",
            user="sqluser",
        )
        == "0\n"
    )

    node.query("GRANT SHOW ON mydb.table1 TO sqluser")
    node.query("GRANT SHOW ON table2 TO sqluser")
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table1'",
            user="sqluser",
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table2'",
            user="sqluser",
        )
        == "1\n"
    )
