import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# Every `ALTER USER` drops the authentication methods whose `VALID UNTIL` deadline has already passed.
# An `ALTER USER ... ON CLUSTER` is executed by the DDL worker of every node, so each node prunes the
# user it holds; this test pins that the result is the same everywhere, both for a method that was
# written with an absolute past deadline and for one written with a negative `VALID FOR` interval,
# which the initiator canonicalizes to an absolute `VALID UNTIL` before shipping the query.
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/clusters.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/clusters.xml"],
    with_zookeeper=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def assert_valid_until_on_all_nodes(user, expected):
    # `valid_until` as Unix timestamps (`0` means "no expiration") identifies the surviving methods
    # independently of the server time zone.
    for node in all_nodes:
        assert_eq_with_retry(
            node,
            f"SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = '{user}'",
            expected,
        )


def test_expired_methods_are_pruned_on_every_node(started_cluster):
    node1.query("DROP USER IF EXISTS u_expired ON CLUSTER cluster")
    node1.query(
        "CREATE USER u_expired ON CLUSTER cluster IDENTIFIED WITH plaintext_password BY 'live'"
    )

    # A method the statement itself adds is kept even when it is already expired.
    node1.query(
        "ALTER USER u_expired ON CLUSTER cluster ADD IDENTIFIED WITH plaintext_password BY 'expired' "
        "VALID UNTIL '2020-01-01 00:00:00 UTC'"
    )
    assert_valid_until_on_all_nodes("u_expired", "[0,1577836800]")

    # The next write drops it on every node, even though this one does not mention authentication.
    node1.query("ALTER USER u_expired ON CLUSTER cluster DEFAULT DATABASE NONE")
    assert_valid_until_on_all_nodes("u_expired", "[0]")
    for node in all_nodes:
        assert node.query("SELECT 1", user="u_expired", password="live") == "1\n"
        assert "VALID UNTIL" not in node.query("SHOW CREATE USER u_expired")

    # A negative `VALID FOR` interval is resolved once on the initiator and shipped as an absolute
    # `VALID UNTIL`, so every node stores the same expired deadline ...
    node1.query(
        "ALTER USER u_expired ON CLUSTER cluster ADD IDENTIFIED WITH plaintext_password BY 'expired_interval' "
        "VALID FOR INTERVAL -1 DAY"
    )
    deadlines = [
        node.query(
            "SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'u_expired'"
        )
        for node in all_nodes
    ]
    assert deadlines[0] == deadlines[1]
    assert deadlines[0].startswith("[0,")
    assert deadlines[0] != "[0]\n"

    # ... and the next rotation drops it everywhere while keeping the methods that are still valid.
    node1.query(
        "ALTER USER u_expired ON CLUSTER cluster ADD IDENTIFIED WITH plaintext_password BY 'token' "
        "VALID UNTIL '2100-01-01 00:00:00 UTC'"
    )
    assert_valid_until_on_all_nodes("u_expired", "[0,4102444800]")
    for node in all_nodes:
        assert node.query("SELECT 1", user="u_expired", password="token") == "1\n"

    node1.query("DROP USER u_expired ON CLUSTER cluster")
