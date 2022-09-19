import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# Passwords in CREATE/ALTER queries must be hidden in logs.
def test_create_alter_user():
    node.query("CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS custom_a = 'a'")
    node.query("ALTER USER u1 IDENTIFIED BY '123qwe' SETTINGS custom_b = 'b'")
    assert (
        node.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS custom_b = \\'b\\'\n"
    )

    node.query("SYSTEM FLUSH LOGS")

    assert node.contains_in_log("CREATE USER u1")
    assert node.contains_in_log("ALTER USER u1")
    assert not node.contains_in_log("qwe123")
    assert not node.contains_in_log("123qwe")
    assert not node.contains_in_log("IDENTIFIED WITH sha256_password BY")
    assert not node.contains_in_log("IDENTIFIED WITH sha256_hash BY")

    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE 'CREATE USER u1%'"
            ).strip()
        )
        >= 1
    )
    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE 'ALTER USER u1%'"
            ).strip()
        )
        >= 1
    )
    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE '%qwe123%'"
            ).strip()
        )
        == 0
    )
    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE '%123qwe%'"
            ).strip()
        )
        == 0
    )
    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE '%IDENTIFIED WITH sha256_password BY%'"
            ).strip()
        )
        == 0
    )
    assert (
        int(
            node.query(
                "SELECT COUNT() FROM system.query_log WHERE query LIKE '%IDENTIFIED WITH sha256_hash BY%'"
            ).strip()
        )
        == 0
    )
