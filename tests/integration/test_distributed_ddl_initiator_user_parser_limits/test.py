import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"replica": "node2"},
)

all_nodes = [node1, node2]

# Deeper than the `max_parser_depth` the `default` profile allows (10), shallower than the
# `deep_parser` profile allows (500). `identity` is not folded away when the accepted AST is
# formatted back into the DDL queue entry, unlike a nested literal.
NESTING = 40


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def prepare_test():
    try:
        yield
    finally:
        for node in all_nodes:
            node.query("DROP VIEW IF EXISTS default.deep_view SYNC")


def test_on_cluster_parses_the_entry_under_the_initiator_user_profile(started_cluster):
    """
    The DDL worker parses the queue entry before it builds the query context, so it used to clamp the
    entry's `max_parser_depth` / `max_parser_backtracks` against its own profile. With
    `distributed_ddl_use_initial_user_and_roles = 1` the entry is executed as the initiator's user,
    whose profile allows a deeper query than the worker's own one, so the pre-parse rejected a query
    that execution would have accepted.
    """
    deep_expression = "identity(" * NESTING + "1" + ")" * NESTING

    node1.query(
        f"CREATE VIEW default.deep_view ON CLUSTER cluster AS SELECT {deep_expression} AS x",
        user="deep_user",
        # The initiator's user and roles travel with the entry only from this entry format version on.
        settings={"distributed_ddl_entry_format_version": 8},
    )

    for node in all_nodes:
        assert (
            node.query(
                "SELECT count() FROM system.tables WHERE database = 'default' AND name = 'deep_view'"
            ).strip()
            == "1"
        )
        assert node.query("SELECT x FROM default.deep_view").strip() == "1"


@pytest.mark.parametrize("entry_version, use_role", [(7, False), (8, True)])
def test_self_authentication_on_cluster(started_cluster, entry_version, use_role):
    # Version 7 omits the actor, exercising the worker's own context. Version 8
    # preserves the actor and explicitly enabled roles under this fixture's config.
    user = f"self_auth_cluster_{entry_version}"
    other = f"{user}_other"
    role = f"{user}_role"
    password = "InitialClusterPassword"
    restricted_password = "RestrictedClusterPassword"
    denied_password = "DeniedClusterPassword"
    privilege = "ALTER USER SELF AUTHENTICATION"
    settings = {
        "distributed_ddl_entry_format_version": entry_version,
        "distributed_ddl_output_mode": "throw",
    }
    role_prefix = f"SET ROLE {role}; " if use_role else ""

    def definitions():
        return [
            node.query(f"SHOW CREATE USER {user}; SHOW CREATE USER {other}")
            for node in all_nodes
        ]

    def deny(query, message, login_password=password, extra_settings=None):
        before = definitions()
        error = node1.query_and_get_error(
            role_prefix + query,
            user=user,
            password=login_password,
            settings={**settings, **(extra_settings or {})},
        )
        assert "ACCESS_DENIED" in error or "READONLY" in error
        assert message in error
        assert definitions() == before
        # These denials all precede the successful distributed statements below.
        assert node1.query(
            f"SELECT count() FROM system.distributed_ddl_queue WHERE position(query, '{user}') > 0"
        ) == "0\n"

    def assert_login(secret):
        for node in all_nodes:
            assert node.query("SELECT 1", user=user, password=secret) == "1\n"

    try:
        for node in all_nodes:
            # Create independently, so UUIDs differ across servers.
            node.query(
                f"CREATE USER {user} IDENTIFIED WITH sha256_password BY '{password}', "
                f"sha256_password BY '{restricted_password}' GRANTS ({privilege}, CLUSTER ON *.*);"
                f"CREATE USER {other} IDENTIFIED WITH sha256_password BY '{password}';"
                f"CREATE ROLE {role};"
                f"GRANT {role} TO {user};"
                f"ALTER USER {user} DEFAULT ROLE NONE;"
                f"GRANT CLUSTER ON *.* TO {user};"
            )
        ids = [
            node.query(f"SELECT id FROM system.users WHERE name = '{user}'")
            for node in all_nodes
        ]
        assert len(set(ids)) == len(all_nodes)

        replacement = f"ALTER USER {user} ON CLUSTER cluster IDENTIFIED WITH sha256_password BY '{denied_password}'"
        deny(replacement, privilege)
        for node in all_nodes:
            recipient = role if use_role else user
            node.query(f"GRANT {privilege} TO {recipient}")
        node1.query(f"REVOKE CLUSTER ON *.* FROM {user}")
        deny(replacement, "CLUSTER")
        node1.query(f"GRANT CLUSTER ON *.* TO {user}")

        for targets, clause in (
            (other, ""),
            (f"{user}, {other}", ""),
            (user, " HOST ANY"),
            (user, " IN nonexistent_storage"),
            (user, " VALID UNTIL '2100-01-01'"),
        ):
            deny(
                f"ALTER USER {targets} ON CLUSTER cluster IDENTIFIED WITH sha256_password BY '{denied_password}'{clause}",
                "ALTER USER",
            )
        deny(replacement, "readonly", extra_settings={"readonly": 1})
        deny(replacement, "cannot change authentication methods", login_password=restricted_password)
        deny(
            f"ALTER USER {user} ON CLUSTER cluster RESET AUTHENTICATION METHODS TO NEW",
            "cannot change authentication methods",
            login_password=restricted_password,
        )

        password = "ReplacementClusterPassword"
        node1.query(
            role_prefix + f"ALTER USER {user} ON CLUSTER cluster IDENTIFIED WITH sha256_password BY '{password}'",
            user=user,
            password="InitialClusterPassword",
            settings=settings,
        )
        assert_login(password)
        added_password = "AddedClusterPassword"
        node1.query(
            role_prefix + f"ALTER USER {user} ON CLUSTER cluster ADD IDENTIFIED WITH sha256_password BY '{added_password}'",
            user=user,
            password=password,
            settings=settings,
        )
        assert_login(password)
        assert_login(added_password)
        node1.query(
            role_prefix + f"ALTER USER {user} ON CLUSTER cluster RESET AUTHENTICATION METHODS TO NEW",
            user=user,
            password=added_password,
            settings=settings,
        )
        assert_login(added_password)
        for node in all_nodes:
            assert node.query(
                f"SELECT length(auth_type) FROM system.users WHERE name = '{user}'"
            ) == "1\n"
            assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
                "SELECT 1", user=user, password=password
            )
    finally:
        for node in all_nodes:
            node.query(f"DROP USER IF EXISTS {user}, {other}; DROP ROLE IF EXISTS {role};")
