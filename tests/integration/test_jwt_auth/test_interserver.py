"""
Regression: JWT / token-directory users must be able to run Distributed /
cluster() queries when remote_servers uses an interserver <secret>.

Without AlwaysAllowCredentials support in TokenAccessStorage, the internal
native hop authenticates with the JWT subject name alone, fails with
"There is no user '<sub>' in token", and the initiator surfaces Code 210.

The durable path mirrors LDAP: after the cluster secret is verified, the
receiving node trusts initial_user via AlwaysAllowCredentials and applies
roles pushed by push_external_roles_in_interserver_queries.
"""

import jwt
import pytest

from helpers.cluster import ClickHouseCluster

# Matches configs/validators.xml <single_key_processor>.
SECRET = "my_secret"
ROLE_GROUP = "role_read"

cluster = ClickHouseCluster(__file__)

instance1 = cluster.add_instance(
    "instance1",
    main_configs=["configs/validators.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users_interserver.xml"],
    macros={"shard": 1, "replica": "instance1"},
    stay_alive=True,
)

instance2 = cluster.add_instance(
    "instance2",
    main_configs=["configs/validators.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users_interserver.xml"],
    macros={"shard": 1, "replica": "instance2"},
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def make_jwt(sub, groups):
    return jwt.encode({"sub": sub, "groups": groups}, SECRET, algorithm="HS256")


def query_with_token(node, token, sql):
    resp = node.http_request(
        "",
        method="POST",
        data=sql,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.text


def test_push_token_role_to_other_nodes():
    """
    instance1 maps JWT group -> role_read and pushes it over the interserver
    hop. instance2 must honor the pushed role even though the JWT subject has
    never authenticated there directly.
    """
    for node in (instance1, instance2):
        node.query("DROP TABLE IF EXISTS distributed_table SYNC", user="common_user", password="qwerty")
        node.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
        node.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")
        node.query("CREATE ROLE role_read", user="common_user", password="qwerty")
        node.query("GRANT SELECT, REMOTE ON *.* TO role_read", user="common_user", password="qwerty")
        node.query(
            "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id",
            user="common_user",
            password="qwerty",
        )

    instance2.query(
        "INSERT INTO local_table VALUES (1), (2), (3)",
        user="common_user",
        password="qwerty",
    )

    instance1.query(
        "CREATE TABLE IF NOT EXISTS distributed_table AS local_table "
        "ENGINE = Distributed(test_token_cluster, default, local_table)",
        user="common_user",
        password="qwerty",
    )

    token = make_jwt("jwt_distributed_user", [ROLE_GROUP])

    # Sanity: token auth + role mapping work locally on the initiator.
    roles = query_with_token(
        instance1,
        token,
        "SELECT role_name FROM system.current_roles ORDER BY role_name FORMAT TabSeparated",
    )
    assert roles.strip() == "role_read"

    # Distributed query must succeed via the interserver secret hop with
    # AlwaysAllowCredentials + pushed roles (prefer_localhost_replica=0).
    result = query_with_token(
        instance1,
        token,
        "SELECT sum(id) FROM distributed_table FORMAT TabSeparated",
    )
    assert result.strip() == "6"

    for node in (instance1, instance2):
        node.query("DROP TABLE IF EXISTS distributed_table SYNC", user="common_user", password="qwerty")
        node.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
        node.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")


def test_cluster_table_function_with_token_user():
    """Same failure mode via cluster(), which Hybrid tables use for the native segment."""
    for node in (instance1, instance2):
        node.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
        node.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")
        node.query("CREATE ROLE role_read", user="common_user", password="qwerty")
        node.query("GRANT SELECT, REMOTE ON *.* TO role_read", user="common_user", password="qwerty")
        node.query(
            "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id",
            user="common_user",
            password="qwerty",
        )

    instance2.query(
        "INSERT INTO local_table VALUES (10), (20)",
        user="common_user",
        password="qwerty",
    )

    token = make_jwt("jwt_cluster_user", [ROLE_GROUP])
    result = query_with_token(
        instance1,
        token,
        "SELECT sum(id) FROM cluster('test_token_cluster', default.local_table) FORMAT TabSeparated",
    )
    assert result.strip() == "30"

    for node in (instance1, instance2):
        node.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
        node.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")
