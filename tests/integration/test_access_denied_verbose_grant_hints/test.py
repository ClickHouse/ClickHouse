import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node_show = cluster.add_instance(
    "node_show",
    main_configs=["configs/always.xml"],
)
node_hide = cluster.add_instance(
    "node_hide",
    main_configs=["configs/never.xml"],
)
node_require = cluster.add_instance(
    "node_require",
    main_configs=["configs/require_show.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_show_mode_exposes_details():
    node_show.query("DROP TABLE IF EXISTS t_show")
    node_show.query("CREATE TABLE t_show(x UInt32) ENGINE = Memory")
    node_show.query("CREATE USER OR REPLACE u_show")

    err = node_show.query_and_get_error("SELECT * FROM t_show", user="u_show")
    assert "necessary to have the grant SELECT" in err


def test_hide_mode_masks_details():
    node_hide.query("DROP TABLE IF EXISTS t_hide")
    node_hide.query("CREATE TABLE t_hide(x UInt32) ENGINE = Memory")
    node_hide.query("CREATE USER OR REPLACE u_hide")

    err = node_hide.query_and_get_error("SELECT * FROM t_hide", user="u_hide")
    assert "Not enough privileges" in err
    assert "necessary to have the grant" not in err


def test_require_show_requires_show_privilege_for_details():
    node_require.query("DROP TABLE IF EXISTS t_require")
    node_require.query("CREATE TABLE t_require(x UInt32) ENGINE = Memory")
    node_require.query("CREATE USER OR REPLACE u_require")

    err = node_require.query_and_get_error("SELECT * FROM t_require", user="u_require")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW DATABASES ON *.* TO u_require")
    node_require.query("GRANT SHOW TABLES ON default.t_require TO u_require")
    node_require.query("GRANT SHOW COLUMNS ON default.t_require TO u_require")
    err = node_require.query_and_get_error("SELECT * FROM t_require", user="u_require")
    assert "necessary to have the grant SELECT" in err


def test_require_show_requires_show_privilege_for_dictionary_hints():
    node_require.query("DROP DICTIONARY IF EXISTS dict_require")
    node_require.query(
        """
        CREATE DICTIONARY dict_require (id UInt64, value UInt64)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'system.one'))
        LAYOUT(FLAT())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    node_require.query("CREATE USER OR REPLACE u_dict")

    err = node_require.query_and_get_error("SELECT * FROM dict_require", user="u_dict")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW DATABASES ON *.* TO u_dict")
    node_require.query("GRANT SHOW DICTIONARIES ON default.dict_require TO u_dict")
    err = node_require.query_and_get_error("SELECT * FROM dict_require", user="u_dict")
    assert "necessary to have the grant SELECT" in err


def test_require_show_requires_show_privilege_for_access_management_hints():
    node_require.query("CREATE USER OR REPLACE u_manage")

    err = node_require.query_and_get_error("CREATE USER u_new", user="u_manage")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW USERS ON *.* TO u_manage")
    err = node_require.query_and_get_error("CREATE USER u_new", user="u_manage")
    assert "necessary to have the grant CREATE USER" in err


def test_require_show_requires_all_levels_for_table_hint():
    node_require.query("DROP TABLE IF EXISTS t_levels")
    node_require.query("CREATE TABLE t_levels(x UInt32) ENGINE = Memory")
    node_require.query("CREATE USER OR REPLACE u_levels")

    # Missing all SHOW: generic error.
    err = node_require.query_and_get_error("SELECT * FROM t_levels", user="u_levels")
    assert "necessary to have the grant" not in err

    # Table SHOW without database SHOW: still generic.
    node_require.query("GRANT SHOW TABLES ON default.t_levels TO u_levels")
    err = node_require.query_and_get_error("SELECT * FROM t_levels", user="u_levels")
    assert "necessary to have the grant" not in err

    # Database SHOW without table SHOW: still generic.
    node_require.query("REVOKE SHOW TABLES ON default.t_levels FROM u_levels")
    node_require.query("GRANT SHOW DATABASES ON *.* TO u_levels")
    err = node_require.query_and_get_error("SELECT * FROM t_levels", user="u_levels")
    assert "necessary to have the grant" not in err

    # Both database and table SHOW but no SHOW COLUMNS: still generic (column names are revealed in hint).
    node_require.query("GRANT SHOW TABLES ON *.* TO u_levels")
    node_require.query("GRANT SHOW TABLES ON default.t_levels TO u_levels")
    err = node_require.query_and_get_error("SELECT * FROM t_levels", user="u_levels")
    assert "necessary to have the grant" not in err

    # Add SHOW COLUMNS: detailed hint.
    node_require.query("GRANT SHOW COLUMNS ON default.t_levels TO u_levels")
    err = node_require.query_and_get_error("SELECT * FROM t_levels", user="u_levels")
    assert "necessary to have the grant SELECT" in err


def test_require_show_requires_show_columns_for_column_hints():
    node_require.query("DROP TABLE IF EXISTS t_cols")
    node_require.query("CREATE TABLE t_cols(x UInt32, y UInt32) ENGINE = Memory")
    node_require.query("CREATE USER OR REPLACE u_cols")

    # Database + table SHOW but no SHOW COLUMNS: generic.
    node_require.query("GRANT SHOW DATABASES ON *.* TO u_cols")
    node_require.query("GRANT SHOW TABLES ON default.t_cols TO u_cols")
    err = node_require.query_and_get_error("SELECT x FROM t_cols", user="u_cols")
    assert "necessary to have the grant" not in err

    # Add SHOW COLUMNS: detailed.
    node_require.query("GRANT SHOW COLUMNS ON default.t_cols TO u_cols")
    err = node_require.query_and_get_error("SELECT x FROM t_cols", user="u_cols")
    assert "necessary to have the grant SELECT" in err


def test_require_show_for_roles_and_users():
    node_require.query("CREATE USER OR REPLACE u_role_tests")

    # No SHOW: generic.
    err = node_require.query_and_get_error("CREATE ROLE r1", user="u_role_tests")
    assert "necessary to have the grant" not in err

    # SHOW USERS alone: still generic for roles.
    node_require.query("GRANT SHOW USERS ON *.* TO u_role_tests")
    err = node_require.query_and_get_error("CREATE ROLE r1", user="u_role_tests")
    assert "necessary to have the grant" not in err

    # SHOW ROLES: detailed for role grant.
    node_require.query("GRANT SHOW ROLES ON *.* TO u_role_tests")
    err = node_require.query_and_get_error("CREATE ROLE r1", user="u_role_tests")
    assert "necessary to have the grant CREATE ROLE" in err

    # For users, SHOW USERS should be enough.
    node_require.query("GRANT SHOW USERS ON *.* TO u_role_tests")
    err = node_require.query_and_get_error("CREATE USER u_new2", user="u_role_tests")
    assert "necessary to have the grant CREATE USER" in err


def test_require_show_for_quotas_and_profiles():
    node_require.query("DROP QUOTA IF EXISTS q1")
    node_require.query("DROP SETTINGS PROFILE IF EXISTS p1")
    node_require.query("CREATE USER OR REPLACE u_quota")

    err = node_require.query_and_get_error("CREATE QUOTA q1", user="u_quota")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW QUOTAS ON *.* TO u_quota")
    err = node_require.query_and_get_error("CREATE QUOTA q1", user="u_quota")
    assert "necessary to have the grant CREATE QUOTA" in err

    node_require.query("CREATE USER OR REPLACE u_profile")

    err = node_require.query_and_get_error("CREATE SETTINGS PROFILE p1", user="u_profile")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW PROFILES ON *.* TO u_profile")
    err = node_require.query_and_get_error("CREATE SETTINGS PROFILE p1", user="u_profile")
    assert ("necessary to have the grant CREATE PROFILE" in err) or ("necessary to have the grant CREATE SETTINGS PROFILE" in err)


def test_require_show_for_database_scope():
    node_require.query("DROP DATABASE IF EXISTS db_scope")
    node_require.query("CREATE USER OR REPLACE u_db_scope")

    err = node_require.query_and_get_error("CREATE DATABASE db_scope", user="u_db_scope")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW DATABASES ON db_scope.* TO u_db_scope")
    err = node_require.query_and_get_error("CREATE DATABASE db_scope", user="u_db_scope")
    assert "necessary to have the grant CREATE DATABASE" in err


def test_require_show_for_named_collections():
    node_require.query("CREATE USER OR REPLACE u_named")

    err = node_require.query_and_get_error("CREATE NAMED COLLECTION nc1 AS url='s3://bucket'", user="u_named")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW NAMED COLLECTIONS ON *.* TO u_named")
    err = node_require.query_and_get_error("CREATE NAMED COLLECTION nc1 AS url='s3://bucket'", user="u_named")
    assert "necessary to have the grant CREATE NAMED COLLECTION" in err


def test_require_show_for_definer_actions():
    node_require.query("CREATE USER OR REPLACE u_definer")

    err = node_require.query_and_get_error("CREATE USER u_definer_target HOST ANY IDENTIFIED WITH plaintext_password BY 'a' DEFAULT ROLE NONE", user="u_definer")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW USERS ON *.* TO u_definer")
    err = node_require.query_and_get_error("CREATE USER u_definer_target HOST ANY IDENTIFIED WITH plaintext_password BY 'a' DEFAULT ROLE NONE", user="u_definer")
    assert "necessary to have the grant CREATE USER" in err


def test_require_show_with_any_parameter_global():
    node_require.query("CREATE USER OR REPLACE u_any_param")

    err = node_require.query_and_get_error("CREATE SETTINGS PROFILE prof_any TO u_any_param", user="u_any_param")
    assert "necessary to have the grant" not in err

    node_require.query("GRANT SHOW PROFILES ON *.* TO u_any_param")
    err = node_require.query_and_get_error("CREATE SETTINGS PROFILE prof_any TO u_any_param", user="u_any_param")
    assert ("necessary to have the grant CREATE PROFILE" in err) or ("necessary to have the grant CREATE SETTINGS PROFILE" in err)


def test_require_show_for_table_like_dictionary_columns():
    node_require.query("DROP DICTIONARY IF EXISTS dict_cols")
    node_require.query(
        """
        CREATE DICTIONARY dict_cols (id UInt64, value UInt64)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'system.one'))
        LAYOUT(FLAT())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    node_require.query("CREATE USER OR REPLACE u_dict_cols")

    node_require.query("GRANT SHOW DATABASES ON *.* TO u_dict_cols")
    node_require.query("GRANT SHOW DICTIONARIES ON default.dict_cols TO u_dict_cols")

    err = node_require.query_and_get_error("SELECT id FROM dict_cols", user="u_dict_cols")
    assert "necessary to have the grant SELECT" in err
