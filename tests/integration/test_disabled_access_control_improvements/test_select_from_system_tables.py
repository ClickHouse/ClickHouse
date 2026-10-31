
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


def test_system_users_with_unbuilt_subquery_set():
    # An `IN` subquery set is filled only once the pipeline runs, so `system.users` produces its rows
    # from a source instead of while the pipeline is built, and `LIMIT 0` closes that source's port
    # before any chunk is requested. The grant is therefore not checked on the row-producing path.
    expected_error = "necessary to have the grant SHOW USERS ON *.*"
    queries = [
        f"SELECT * FROM system.users WHERE name IN (SELECT toString(number) FROM numbers(3)){suffix}"
        for suffix in ["", " LIMIT 1", " LIMIT 0", " LIMIT 0 OFFSET 5"]
    ]

    for query in queries:
        assert expected_error in node.query_and_get_error(query, user="another"), query

    node.query("GRANT SHOW USERS ON *.* TO sqluser")
    for query in queries:
        node.query(query, user="sqluser")


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
