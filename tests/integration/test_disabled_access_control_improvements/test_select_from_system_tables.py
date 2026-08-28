
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/disable_access_control_improvements.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=[
        "configs/users.d/another_user.xml",
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
    expected_error = "necessary to have the grant SHOW USERS ON *.*"
    assert expected_error in node.query_and_get_error(
        "SELECT count()>0 FROM system.users", user="another"
    )
    assert node.query("SELECT count()>0 FROM system.clusters", user="another") == "1\n"
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
    expected_error = "necessary to have the grant SHOW USERS ON *.*"
    assert expected_error in node.query_and_get_error(
        "SELECT count()>0 FROM system.users", user="sqluser"
    )
    assert node.query("SELECT count()>0 FROM system.clusters", user="sqluser") == "1\n"
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

    node.query("GRANT SHOW USERS ON *.* TO sqluser")
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


def test_read_system_table_through_remote():
    # The user holds an implicit SELECT on the system database and no SHOW COLUMNS on it, so a read
    # through a cluster must be authorized by the privilege on the data. 127.0.0.1 makes the shard
    # local, which is the arm that resolves the structure on this server rather than on a remote one.
    node.query("GRANT READ ON REMOTE, CREATE TEMPORARY TABLE ON *.* TO sqluser")
    assert node.query("SELECT count() >= 0 FROM system.parts", user="sqluser") == "1\n"
    assert (
        node.query(
            "SELECT count() >= 0 FROM remote('127.0.0.1:9000', 'system', 'parts')",
            user="sqluser",
        )
        == "1\n"
    )
    # Introspection keeps requiring the privilege on the schema.
    expected_error = "necessary to have the grant SHOW COLUMNS ON system.parts"
    assert expected_error in node.query_and_get_error(
        "DESCRIBE TABLE remote('127.0.0.1:9000', 'system', 'parts')", user="sqluser"
    )


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

    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table1'",
            user="another",
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM information_schema.tables WHERE table_name='table2'",
            user="another",
        )
        == "0\n"
    )

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
