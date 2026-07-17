import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# A bare-table `GRANTS (...)` clause on an authentication method (e.g. `GRANTS (SELECT ON t1)`) must be
# bound to the initiator's current database before the `ON CLUSTER` DDL is shipped. `AddDefaultDatabaseVisitor`
# does not rewrite `ASTAuthenticationData::grants` (they are stored outside the AST children), and a DDL
# worker's current database is not the initiator's (it is `default`). Without the initiator rewrite in
# `InterpreterCreateUserQuery::execute`, each node would rebind the bare table against its own database, so
# the persisted clause would silently diverge between the node that issued the DDL from `db1` and the others,
# which would rebind it against `default`. This test issues the DDL from `db1` and asserts every node persists
# the same `GRANTS (SELECT ON db1.t1)`.
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE IF NOT EXISTS db1 ON CLUSTER cluster")
        yield cluster
    finally:
        cluster.shutdown()


def assert_grants_on_all_nodes(user, expected_auth_grants):
    for node in all_nodes:
        # `system.users.auth_grants` is the array of the precisely serialized `GRANTS` clause of each
        # authentication method (empty string for a method without a clause).
        assert_eq_with_retry(
            node,
            f"SELECT auth_grants FROM system.users WHERE name = '{user}'",
            expected_auth_grants,
        )
        # `SHOW CREATE USER` must render the same bound clause on every node.
        assert "GRANTS (SELECT ON db1.t1)" in node.query(f"SHOW CREATE USER {user}")


def test_create_user_on_cluster_binds_initiator_database(started_cluster):
    node1.query("DROP USER IF EXISTS u_create ON CLUSTER cluster")

    # Issued from `db1`, with a bare-table grant. Every node must persist `db1.t1`, not its own `default.t1`.
    node1.query(
        "CREATE USER u_create ON CLUSTER cluster IDENTIFIED WITH sha256_password BY 'pw' GRANTS (SELECT ON t1)",
        database="db1",
    )

    assert_grants_on_all_nodes("u_create", "['SELECT ON db1.t1']")

    node1.query("DROP USER u_create ON CLUSTER cluster")


def test_alter_user_on_cluster_binds_initiator_database(started_cluster):
    node1.query("DROP USER IF EXISTS u_alter ON CLUSTER cluster")

    # A first method without a grants clause; then add a second method carrying a bare-table grant from `db1`.
    node1.query("CREATE USER u_alter ON CLUSTER cluster IDENTIFIED WITH sha256_password BY 'pw1'")
    node1.query(
        "ALTER USER u_alter ON CLUSTER cluster ADD IDENTIFIED WITH sha256_password BY 'pw2' GRANTS (SELECT ON t1)",
        database="db1",
    )

    # The first method has no clause (empty string); the second must be bound to `db1.t1` on every node.
    assert_grants_on_all_nodes("u_alter", "['','SELECT ON db1.t1']")

    node1.query("DROP USER u_alter ON CLUSTER cluster")
