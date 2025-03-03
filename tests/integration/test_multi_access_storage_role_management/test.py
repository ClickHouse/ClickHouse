import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", stay_alive=True, main_configs=["configs/memory.xml"]
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE mydb")
        node.query("CREATE TABLE mydb.table1(x UInt32) ENGINE=Log")

        node.query("CREATE USER test_user")
        node.query("CREATE USER test_user2")
        yield cluster

    finally:
        cluster.shutdown()


def execute_test_for_access_type(access_type: str, system_table_name: str):
    node.query(f"CREATE {access_type} test1 IN local_directory")
    node.query(f"CREATE {access_type} test2 IN local_directory")
    node.query(f"CREATE {access_type} test3 IN local_directory")

    node.query(f"CREATE {access_type} test4 IN memory")
    node.query(f"CREATE {access_type} test5 IN memory")
    node.query(f"CREATE {access_type} test6 IN memory")

    # Already exists
    with pytest.raises(QueryRuntimeException):
        node.query(f"CREATE {access_type} test1 IN memory")

    node.query(f"MOVE {access_type} test1 TO memory")
    assert node.query(
        f"SELECT storage FROM system.{system_table_name} WHERE name = 'test1'"
    ) == TSV(["memory"])

    node.query(f"MOVE {access_type} test2 TO local_directory")
    assert node.query(
        f"SELECT storage FROM system.{system_table_name} WHERE name = 'test2'"
    ) == TSV(["local_directory"])

    node.query(f"MOVE {access_type} test2,test3 TO memory")
    assert node.query(
        f"SELECT storage FROM system.{system_table_name} WHERE name = 'test2'"
    ) == TSV(["memory"])
    assert node.query(
        f"SELECT storage FROM system.{system_table_name} WHERE name = 'test3'"
    ) == TSV(["memory"])

    node.query(f"MOVE {access_type} test4,test5 TO local_directory")

    # Different storages
    with pytest.raises(QueryRuntimeException):
        node.query(f"MOVE {access_type} test4,test1 TO memory")

    # Doesn't exist
    with pytest.raises(QueryRuntimeException):
        node.query(f"MOVE {access_type} test7 TO local_directory")

    # Storage doesn't exist
    with pytest.raises(QueryRuntimeException):
        node.query(f"MOVE {access_type} test6 TO non_existing_storage")

    # Unwriteable storage
    with pytest.raises(QueryRuntimeException):
        node.query(f"MOVE {access_type} test6 TO users_xml")

    node.query(f"DROP {access_type} test1")
    node.query(f"DROP {access_type} test2")
    node.query(f"DROP {access_type} test3")
    node.query(f"DROP {access_type} test4")
    node.query(f"DROP {access_type} test5")
    node.query(f"DROP {access_type} test6")


def test_roles():
    execute_test_for_access_type("ROLE", "roles")


def test_users():
    execute_test_for_access_type("USER", "users")


def test_settings_profiles():
    execute_test_for_access_type("SETTINGS PROFILE", "settings_profiles")


def test_quotas():
    execute_test_for_access_type("QUOTA", "quotas")


def test_role_from_different_storages():
    node.query("CREATE ROLE default_role")
    node.query("GRANT SELECT ON system.* TO default_role")

    assert node.query("SHOW GRANTS FOR default_role") == TSV(
        ["GRANT SELECT ON system.* TO default_role"]
    )
    assert node.query("SHOW ROLES") == TSV(["default_role"])

    node.query("GRANT default_role TO test_user")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/roles.xml"),
        "/etc/clickhouse-server/users.d/roles.xml",
    )

    node.restart_clickhouse()

    assert node.query("SELECT name, storage FROM system.roles") == TSV(
        [["default_role", "users_xml"], ["default_role", "local_directory"]]
    )

    # Role from users.xml will have priority
    assert node.query("SHOW GRANTS FOR default_role") == TSV(
        ["GRANT ALL ON *.* TO default_role WITH GRANT OPTION"]
    )

    node.query("GRANT default_role TO test_user")
    node.query("GRANT default_role TO test_user2")
    assert node.query(
        "SELECT granted_role_id FROM system.role_grants WHERE user_name = 'test_user2'"
    ) == TSV(
        [
            "62bedbf3-7fb1-94cb-3a35-e479693223b3"
        ]  # roles from users.xml have deterministic ids
    )

    node.query("DROP ROLE default_role FROM local_directory")
    assert node.query(
        "SELECT granted_role_id FROM system.role_grants WHERE user_name = 'test_user'"
    ) == TSV(["62bedbf3-7fb1-94cb-3a35-e479693223b3"])

    # Already exists
    with pytest.raises(QueryRuntimeException):
        node.query("CREATE ROLE default_role IN memory")

    node.query("CREATE ROLE other_role IN memory")

    assert node.query(
        "SELECT storage FROM system.roles WHERE name = 'other_role'"
    ) == TSV(["memory"])
