import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import os.path


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def init_before_test():
    node.query("DROP USER IF EXISTS u0")
    yield


def test_migrate_user_from_version_1():
    # Replace folder /var/lib/clickhouse/access with the contents of "version_1/"
    node.stop_clickhouse()
    node.clear_dir_in_container("/var/lib/clickhouse/access")
    node.copy_dir_to_container(
        os.path.join(SCRIPT_DIR, "version_1"), "/var/lib/clickhouse/access"
    )
    node.start_clickhouse()

    u0_id = node.query("SELECT id FROM system.users WHERE name='u0'").rstrip()

    assert node.query("SHOW CREATE USER u0") == "CREATE USER u0\n"
    assert node.query("SHOW CREATE USER u0 WITH VERSION") == TSV(
        ["SET rbac_version = 2", "CREATE USER u0"]
    )
    assert node.query("SHOW GRANTS FOR u0") == TSV(
        ["GRANT INSERT ON mydb.mytable TO u0", "GRANT SELECT ON system.* TO u0"]
    )

    assert (
        node.read_file(os.path.join("/var/lib/clickhouse/access", u0_id + ".sql"))
        == "ATTACH USER u0;\n"
        "ATTACH GRANT INSERT ON mydb.mytable TO u0;\n"
    )

    # We can SELECT from all the system tables
    node.query("SELECT cluster FROM system.clusters", user="u0")
    node.query("SELECT name FROM system.databases", user="u0")
    node.query("SELECT * FROM information_schema.schemata", user="u0")
    node.query("SELECT * FROM system.one", user="u0")

    # Revoke some grants for the system database.
    node.query("REVOKE ALL ON system.clusters FROM u0")

    assert node.query("SHOW CREATE USER u0") == "CREATE USER u0\n"
    assert node.query("SHOW CREATE USER u0 WITH VERSION") == TSV(
        ["SET rbac_version = 2", "CREATE USER u0"]
    )

    assert node.query("SHOW GRANTS FOR u0") == TSV(
        [
            "GRANT INSERT ON mydb.mytable TO u0",
            "GRANT SELECT ON system.* TO u0",
            "REVOKE SELECT ON system.clusters FROM u0",
        ]
    )

    assert (
        node.read_file(os.path.join("/var/lib/clickhouse/access", u0_id + ".sql"))
        == "SET rbac_version = 2;\n"
        "ATTACH USER u0;\n"
        "ATTACH GRANT INSERT ON mydb.mytable TO u0;\n"
        "ATTACH GRANT SELECT ON system.* TO u0;\n"
        "ATTACH REVOKE SELECT ON system.clusters FROM u0;\n"
    )

    # We can SELECT from all the system tables except system.clusters
    expected_error = "necessary to have grant SELECT(cluster) ON system.clusters"
    assert expected_error in node.query_and_get_error(
        "SELECT cluster FROM system.clusters", user="u0"
    )
    node.query("SELECT name FROM system.databases", user="u0")
    node.query("SELECT * FROM information_schema.schemata", user="u0")
    node.query("SELECT * FROM system.one", user="u0")

    # Revoke all grants for the system database.
    node.query("REVOKE ALL ON system.* FROM u0")

    assert node.query("SHOW CREATE USER u0") == "CREATE USER u0\n"
    assert node.query("SHOW CREATE USER u0 WITH VERSION") == TSV(
        ["SET rbac_version = 2", "CREATE USER u0"]
    )

    assert node.query("SHOW GRANTS FOR u0") == TSV(
        [
            "GRANT INSERT ON mydb.mytable TO u0",
        ]
    )

    assert (
        node.read_file(os.path.join("/var/lib/clickhouse/access", u0_id + ".sql"))
        == "SET rbac_version = 2;\n"
        "ATTACH USER u0;\n"
        "ATTACH GRANT INSERT ON mydb.mytable TO u0;\n"
    )

    # We still can SELECT from some system tables (because of implicit grants)
    expected_error = "necessary to have grant SELECT(cluster) ON system.clusters"
    assert expected_error in node.query_and_get_error(
        "SELECT cluster FROM system.clusters", user="u0"
    )
    expected_error = "necessary to have grant SELECT(name) ON system.databases"
    assert expected_error in node.query_and_get_error(
        "SELECT name FROM system.databases", user="u0"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT * FROM information_schema.schemata", user="u0"
    )
    node.query("SELECT * FROM system.one", user="u0")

    expected_error = "necessary to have grant SELECT(name, engine, data_path, metadata_path, uuid, comment) ON system.databases"
    assert expected_error in node.query_and_get_error(
        "SELECT * FROM system.databases", user="u0"
    )
    expected_error = "necessary to have grant SELECT(name) ON system.databases"
    assert expected_error in node.query_and_get_error(
        "SELECT * FROM information_schema.schemata", user="u0"
    )

    # Restart server and check again.
    node.restart_clickhouse()

    assert node.query("SHOW CREATE USER u0") == "CREATE USER u0\n"

    assert node.query("SHOW GRANTS FOR u0") == TSV(
        [
            "GRANT INSERT ON mydb.mytable TO u0",
        ]
    )

    assert (
        node.read_file(os.path.join("/var/lib/clickhouse/access", u0_id + ".sql"))
        == "SET rbac_version = 2;\n"
        "ATTACH USER u0;\n"
        "ATTACH GRANT INSERT ON mydb.mytable TO u0;\n"
    )

    # We still can SELECT from some system tables (because of implicit grants)
    expected_error = "necessary to have grant SELECT(cluster) ON system.clusters"
    assert expected_error in node.query_and_get_error(
        "SELECT cluster FROM system.clusters", user="u0"
    )
    expected_error = "necessary to have grant SELECT(name) ON system.databases"
    assert expected_error in node.query_and_get_error(
        "SELECT name FROM system.databases", user="u0"
    )
    assert expected_error in node.query_and_get_error(
        "SELECT * FROM information_schema.schemata", user="u0"
    )
    node.query("SELECT * FROM system.one", user="u0")


def test_create_user_with_version_1():
    node.query("CREATE USER u0", settings={"rbac_version": 1})
    u0_id = node.query("SELECT id FROM system.users WHERE name='u0'").rstrip()

    assert node.query("SHOW CREATE USER u0") == "CREATE USER u0\n"
    assert node.query("SHOW CREATE USER u0 WITH VERSION") == TSV(
        ["SET rbac_version = 2", "CREATE USER u0"]
    )
    assert node.query("SHOW GRANTS FOR u0") == TSV(["GRANT SELECT ON system.* TO u0"])

    assert (
        node.read_file(os.path.join("/var/lib/clickhouse/access", u0_id + ".sql"))
        == "SET rbac_version = 2;\n"
        "ATTACH USER u0;\n"
        "ATTACH GRANT SELECT ON system.* TO u0;\n"
    )


def test_ignore_obsolete_grant_create_function_on_database():
    # Replace folder /var/lib/clickhouse/access with the contents of "grant_create_function_on_database/"
    node.stop_clickhouse()
    node.clear_dir_in_container("/var/lib/clickhouse/access")
    node.copy_dir_to_container(
        os.path.join(SCRIPT_DIR, "grant_create_function_on_database"),
        "/var/lib/clickhouse/access",
    )
    node.start_clickhouse()

    assert node.query("SHOW GRANTS FOR u0") == TSV(
        ["GRANT SELECT ON mydb.* TO u0", "GRANT SELECT ON system.* TO u0"]
    )


def test_wrong_lists_need_rebuild():
    # Replace folder /var/lib/clickhouse/access with the contents of "grant_create_function_on_database/"
    node.stop_clickhouse()
    node.clear_dir_in_container("/var/lib/clickhouse/access")
    node.copy_dir_to_container(
        os.path.join(SCRIPT_DIR, "wrong_lists_need_rebuild"),
        "/var/lib/clickhouse/access",
    )
    node.start_clickhouse()

    node.query("SHOW CREATE USER user1")
    node.query("SHOW CREATE ROLE role1")
    node.query("SHOW CREATE ROW POLICY policy1 ON mydb.mytable")
    node.query("SHOW CREATE SETTINGS PROFILE profile1")
    node.query("SHOW CREATE QUOTA quota1")
