import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)

TENANT = {"user": "tenant", "password": "tpass"}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_scheduled_refresh_of_definer_view_carries_definer_to_remote_shard(
    started_cluster,
):
    # Protected data lives only on node2. The tenant exists on both nodes but has no grant on it.
    node2.query(
        """
        CREATE DATABASE lab;
        CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tpass';
        CREATE TABLE lab.secrets (id UInt64, payload String) ENGINE = MergeTree ORDER BY id;
        INSERT INTO lab.secrets VALUES (1, 'a'), (2, 'b'), (3, 'c');
        """
    )
    node1.query(
        """
        CREATE DATABASE tenant_db;
        CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tpass';
        GRANT CREATE TABLE, SELECT, INSERT ON tenant_db.* TO tenant;
        GRANT TABLE ENGINE ON Distributed TO tenant;
        """
    )

    node1.query(
        """
        CREATE TABLE tenant_db.dist (id UInt64, payload String)
        ENGINE = Distributed('secure', 'lab', 'secrets', rand());
        CREATE TABLE tenant_db.target
        (id UInt64, payload String, current_user String, authenticated_user String)
        ENGINE = MergeTree ORDER BY id;
        """,
        **TENANT,
    )

    # A direct read through the Distributed table is denied on the remote shard.
    assert "ACCESS_DENIED" in node1.query_and_get_error(
        "SELECT * FROM tenant_db.dist", **TENANT
    )

    # The refresh runs under the definer's identity and must be denied on the remote shard too.
    # Before the fix, the refresh query was sent with an empty `initial_user`, which the remote
    # shard treated as interserver mode and executed without any access checks.
    node1.query(
        """
        CREATE MATERIALIZED VIEW tenant_db.rmv REFRESH EVERY 1 YEAR APPEND TO tenant_db.target
        DEFINER = CURRENT_USER SQL SECURITY DEFINER
        AS SELECT id, payload, currentUser() AS current_user, authenticatedUser() AS authenticated_user
        FROM tenant_db.dist;
        """,
        **TENANT,
    )

    with pytest.raises(QueryRuntimeException, match="ACCESS_DENIED"):
        node1.query("SYSTEM WAIT VIEW tenant_db.rmv")
    assert node1.query("SELECT count() FROM tenant_db.target") == "0\n"
    assert "ACCESS_DENIED" in node1.query(
        "SELECT exception FROM system.view_refreshes WHERE database = 'tenant_db' AND view = 'rmv'"
    )

    # Once the definer is granted access on the remote shard, the refresh succeeds and the remote
    # shard sees the definer as the current and authenticated user.
    node2.query("GRANT SELECT ON lab.secrets TO tenant")
    node1.query("SYSTEM REFRESH VIEW tenant_db.rmv")
    node1.query("SYSTEM WAIT VIEW tenant_db.rmv")
    assert node1.query("SELECT * FROM tenant_db.target ORDER BY id", **TENANT) == (
        "1\ta\ttenant\ttenant\n2\tb\ttenant\ttenant\n3\tc\ttenant\ttenant\n"
    )
