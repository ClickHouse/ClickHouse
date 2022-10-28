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


def check_logs(must_contain, must_not_contain):
    node.query("SYSTEM FLUSH LOGS")

    for str in must_contain:
        assert node.contains_in_log(str)
        assert (
            int(
                node.query(
                    f"SELECT COUNT() FROM system.query_log WHERE query LIKE '%{str}%'"
                ).strip()
            )
            >= 1
        )

    for str in must_not_contain:
        assert not node.contains_in_log(str)
        assert (
            int(
                node.query(
                    f"SELECT COUNT() FROM system.query_log WHERE query LIKE '%{str}%'"
                ).strip()
            )
            == 0
        )


# Passwords in CREATE/ALTER queries must be hidden in logs.
def test_create_alter_user():
    node.query("CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS custom_a = 'a'")
    node.query("ALTER USER u1 IDENTIFIED BY '123qwe' SETTINGS custom_b = 'b'")
    node.query(
        "CREATE USER u2 IDENTIFIED WITH plaintext_password BY 'plainpasswd' SETTINGS custom_c = 'c'"
    )

    assert (
        node.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS custom_b = \\'b\\'\n"
    )
    assert (
        node.query("SHOW CREATE USER u2")
        == "CREATE USER u2 IDENTIFIED WITH plaintext_password SETTINGS custom_c = \\'c\\'\n"
    )

    check_logs(
        must_contain=[
            "CREATE USER u1 IDENTIFIED WITH sha256_password",
            "ALTER USER u1 IDENTIFIED WITH sha256_password",
            "CREATE USER u2 IDENTIFIED WITH plaintext_password",
        ],
        must_not_contain=[
            "qwe123",
            "123qwe",
            "plainpasswd",
            "IDENTIFIED WITH sha256_password BY",
            "IDENTIFIED WITH sha256_hash BY",
            "IDENTIFIED WITH plaintext_password BY",
        ],
    )
