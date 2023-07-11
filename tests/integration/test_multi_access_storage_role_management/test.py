import pytest
import os
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
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


def test_role_from_different_storages():
    node.query("CREATE ROLE default_role")
    node.query("GRANT SELECT ON system.* TO default_role")

    assert node.query("SHOW GRANTS FOR default_role") == TSV(["GRANT SELECT ON system.* TO default_role"])
    assert node.query("SHOW ROLES") == TSV(["default_role"])

    node.query("GRANT default_role TO test_user")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/roles.xml"),
        "/etc/clickhouse-server/users.d/roles.xml",
    )

    node.restart_clickhouse()

    assert node.query("SELECT name, storage FROM system.roles") == TSV(
        [
            ["default_role", "users_xml"],
            ["default_role", "local_directory"]
        ]
    )

    # Role from users.xml will have priority
    assert node.query("SHOW GRANTS FOR default_role") == TSV(["GRANT ALL ON *.* TO default_role WITH GRANT OPTION"])

    node.query("GRANT default_role TO test_user")
    node.query("GRANT default_role TO test_user2")
    assert node.query("SELECT granted_role_id FROM system.role_grants WHERE user_name = 'test_user2'") == TSV(
        ["62bedbf3-7fb1-94cb-3a35-e479693223b3"]  # roles from users.xml have deterministic ids
    )

    node.query("DROP ROLE default_role FROM local_directory")
    assert node.query("SELECT granted_role_id FROM system.role_grants WHERE user_name = 'test_user'") == TSV(["62bedbf3-7fb1-94cb-3a35-e479693223b3"])

    # Already exists
    with pytest.raises(QueryRuntimeException):
        node.query("CREATE ROLE default_role AT memory")

    node.query("CREATE ROLE other_role AT memory")

    assert node.query("SELECT storage FROM system.roles WHERE name = 'other_role'") == TSV(["memory"])
