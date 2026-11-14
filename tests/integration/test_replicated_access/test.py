from os import path as p

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/grant_all.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/grant_all.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def prepare_test():
    node1.query("CREATE USER test")
    node1.query("CREATE TABLE IF NOT EXISTS table ON CLUSTER default (x UInt64) ENGINE=MergeTree ORDER BY x")
    node1.query("CREATE TABLE IF NOT EXISTS secret ON CLUSTER default (value String) ENGINE=MergeTree ORDER BY value")
    try:
        yield
    finally:
        node1.query("DROP USER IF EXISTS test")
        node1.query("DROP TABLE IF EXISTS table ON CLUSTER default")
        node1.query("DROP TABLE IF EXISTS secret ON CLUSTER default")


def test_initiator_user_in_ddl(started_cluster):
    node1.query("INSERT INTO secret VALUES ('super_secret')")

    node1.query("GRANT ALTER ON table TO test")
    node1.query("GRANT CLUSTER ON *.* TO test")

    query = """
    ALTER TABLE table ON CLUSTER default
        ADD PROJECTION test (
            SELECT
                x,
                (SELECT * FROM secret LIMIT 1) as bar
            ORDER BY x
        )
    SETTINGS distributed_ddl_entry_format_version = 8
    """

    error = node1.query_and_get_error(query, user="test")
    assert "Not enough privileges" in error


    for node in all_nodes:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/config.xml",
            "<distributed_ddl_use_initial_user_and_roles>1</distributed_ddl_use_initial_user_and_roles>",
            "<distributed_ddl_use_initial_user_and_roles>0</distributed_ddl_use_initial_user_and_roles>",
        )
        node.restart_clickhouse()

    node1.query(query, user="test")

    for node in all_nodes:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/config.xml",
            "<distributed_ddl_use_initial_user_and_roles>0</distributed_ddl_use_initial_user_and_roles>",
            "<distributed_ddl_use_initial_user_and_roles>1</distributed_ddl_use_initial_user_and_roles>",
        )
        node.restart_clickhouse()


def test_self_authentication_replication_with_management_disabled(started_cluster):
    setting = "experimental_self_service_password_change"
    privilege = "ALTER USER SELF AUTHENTICATION"
    direct, member, role = (
        "self_auth_repl_direct",
        "self_auth_repl_member",
        "self_auth_repl_role",
    )
    password = "ReplicatedSelfPassword"
    config_path = "/etc/clickhouse-server/config.d/self_auth_setting.xml"
    normalized = "self_auth_repl_normalized"
    actor = "self_auth_repl_actor"
    try:
        node2.replace_config(
            config_path, f"<clickhouse><{setting}>false</{setting}></clickhouse>"
        )
        node2.restart_clickhouse()
        # node1 opts in through the shared test config; node2 receives trusted metadata.
        node1.query(
            f"CREATE USER {direct}, {member} IDENTIFIED WITH sha256_password BY '{password}';"
            f"CREATE ROLE {role};"
            f"GRANT {privilege} TO {direct} WITH GRANT OPTION;"
            f"GRANT ACCESS MANAGEMENT ON *.* TO {role};"
            f"REVOKE ALTER USER ON * FROM {role};"
            f"REVOKE CREATE USER ON self_auth_repl_excluded FROM {role};"
            f"GRANT {role} TO {member};"
        )
        for name in (direct, member, role):
            assert_eq_with_retry(
                node2, f"SHOW GRANTS FOR {name}", node1.query(f"SHOW GRANTS FOR {name}")
            )
        expected = node2.query(f"SHOW GRANTS FOR {role}")
        node2.restart_clickhouse()
        assert node2.query(f"SHOW GRANTS FOR {role}") == expected
        for user in (direct, member):
            node2.query(
                f"ALTER USER {user} IDENTIFIED WITH sha256_password BY '{password}'",
                user=user,
                password=password,
            )
            assert node2.query("SELECT 1", user=user, password=password) == "1\n"
        node2.query(f"REVOKE ALL ON *.* FROM {direct}")
        assert (
            node2.query(f"SHOW GRANTS FOR {direct}")
            == f"GRANT {privilege} TO {direct} WITH GRANT OPTION\n"
        )
        node2.query(
            f"ALTER USER {direct} IDENTIFIED WITH sha256_password BY '{password}'",
            user=direct,
            password=password,
        )

        # Dispatch from a disabled initiator to both disabled and enabled workers.
        node1.query(f"CREATE ROLE {normalized}")
        settings = {
            "distributed_ddl_entry_format_version": 8,
            "distributed_ddl_output_mode": "throw",
            "ignore_on_cluster_for_replicated_access_entities_queries": 0,
        }
        for group in ("ALL", "ACCESS MANAGEMENT"):
            for initial in (0, 1):
                node1.query(f"REVOKE ALL ON *.* FROM {normalized}")
                if initial:
                    node1.query(f"GRANT {privilege} TO {normalized} WITH GRANT OPTION")
                assert_eq_with_retry(
                    node2,
                    f"SHOW GRANTS FOR {normalized}",
                    node1.query(f"SHOW GRANTS FOR {normalized}"),
                )
                for action in ("GRANT", "REVOKE"):
                    recipient_keyword = "TO" if action == "GRANT" else "FROM"
                    node2.query(
                        f"{action} ON CLUSTER default {group} ON *.* {recipient_keyword} {normalized}",
                        settings=settings,
                    )
                    assert (
                        node1.query(
                            f"SELECT countIf(access_type = '{privilege}'), "
                            f"maxIf(grant_option, access_type = '{privilege}'), "
                            "countIf(access_type = 'ALTER USER' AND NOT is_partial_revoke) "
                            f"FROM system.grants WHERE role_name = '{normalized}'"
                        )
                        == f"{initial}\t{initial}\t{int(action == 'GRANT')}\n"
                    )

        assert (
            node2.query(
                "SELECT uniqExact(entry), countIf(position(query, 'ALTER USER SELF AUTHENTICATION') > 0) "
                f"FROM system.distributed_ddl_queue WHERE position(query, '{normalized}') > 0"
            )
            == "8\t0\n"
        )
        before = node2.query(f"SHOW GRANTS FOR {normalized}")
        error = node2.query_and_get_error(
            f"GRANT ON CLUSTER default ALL, {privilege} ON *.* TO {normalized}",
            settings=settings,
        )
        assert "SUPPORT_IS_DISABLED" in error and setting in error
        assert node2.query(f"SHOW GRANTS FOR {normalized}") == before

        replacement = f"GRANT ON CLUSTER default SELECT ON system.one TO {normalized} WITH REPLACE OPTION"
        for initial in (0, 1, 2):
            node1.query(
                f"REVOKE ALL ON *.* FROM {normalized}; GRANT INSERT ON system.numbers TO {normalized};"
            )
            if initial:
                option = " WITH GRANT OPTION" if initial == 2 else ""
                node1.query(f"GRANT {privilege} TO {normalized}{option}")
            assert_eq_with_retry(
                node2,
                f"SHOW GRANTS FOR {normalized}",
                node1.query(f"SHOW GRANTS FOR {normalized}"),
            )
            # Both disabled and enabled workers clear the same full direct privilege set.
            node2.query(replacement, settings=settings)
            for node in all_nodes:
                assert (
                    node.query(f"SHOW GRANTS FOR {normalized}")
                    == f"GRANT SELECT ON system.one TO {normalized}\n"
                )

        node1.query(
            f"CREATE USER {actor} IDENTIFIED WITH sha256_password BY '{password}';"
            f"GRANT ALL ON *.* TO {actor} WITH GRANT OPTION; REVOKE {privilege} FROM {actor};"
        )
        assert_eq_with_retry(
            node2, f"SHOW GRANTS FOR {actor}", node1.query(f"SHOW GRANTS FOR {actor}")
        )
        # Full ALTER USER cannot supply the missing grant option for the full implicit revoke,
        # even though the target currently has no self-service bit.
        before = node2.query(f"SHOW GRANTS FOR {normalized}")
        error = node2.query_and_get_error(
            replacement, user=actor, password=password, settings=settings
        )
        assert "ACCESS_DENIED" in error
        assert f"(Missing permissions: {privilege})" in error
        assert node2.query(f"SHOW GRANTS FOR {normalized}") == before
        node1.query(f"GRANT {privilege} TO {actor} WITH GRANT OPTION")
        assert_eq_with_retry(
            node2, f"SHOW GRANTS FOR {actor}", node1.query(f"SHOW GRANTS FOR {actor}")
        )
        node2.query(replacement, user=actor, password=password, settings=settings)
        assert node2.query(f"SHOW GRANTS FOR {normalized}") == before
    finally:
        node1.query(
            f"DROP USER IF EXISTS {direct}, {member}, {actor}; DROP ROLE IF EXISTS {role}, {normalized};"
        )
        node2.exec_in_container(["rm", "-f", config_path])
        node2.restart_clickhouse()
