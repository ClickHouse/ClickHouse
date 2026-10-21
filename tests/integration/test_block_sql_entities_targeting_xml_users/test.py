"""
Tests that SQL-created quotas, row policies, and settings profiles
cannot target users defined in users.xml.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/enable_block.xml"],
    user_configs=[
        "configs/users.d/xml_user.xml",
        "configs/users.d/runtime_xml_user.xml",
    ],
    stay_alive=True,
)


RUNTIME_USER_XML = """<clickhouse>
    <users>
        <xml-runtime>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>0</access_management>
        </xml-runtime>
    </users>
</clickhouse>
"""


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def drop_everything():
    """Drop all state the tests touch. Safe to call before and after each test."""
    node.query(
        """
        DROP ROW POLICY IF EXISTS rxml ON db.t;
        DROP ROW POLICY IF EXISTS rsql ON db.t;
        DROP ROW POLICY IF EXISTS rall ON db.t;
        DROP QUOTA IF EXISTS qxml;
        DROP QUOTA IF EXISTS qsql;
        DROP QUOTA IF EXISTS qall;
        DROP QUOTA IF EXISTS qexcept;
        DROP QUOTA IF EXISTS qrole;
        DROP SETTINGS PROFILE IF EXISTS pxml;
        DROP SETTINGS PROFILE IF EXISTS psql;
        DROP SETTINGS PROFILE IF EXISTS pall;
        DROP USER IF EXISTS sql_user, sql_user2;
        DROP ROLE IF EXISTS sql_role;
        DROP TABLE IF EXISTS db.t SYNC;
        DROP DATABASE IF EXISTS db SYNC;
        """
    )


@pytest.fixture(autouse=True)
def isolate():
    drop_everything()
    yield
    drop_everything()


def setup_db_and_table():
    node.query(
        """
        CREATE DATABASE db;
        CREATE TABLE db.t (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
        INSERT INTO db.t VALUES (1, 'secret');
        """
    )


def test_explicit_targeting_blocked_at_creation():
    """Creation-time check rejects `TO xml-internal` for all entity types."""
    setup_db_and_table()

    for q in [
        "CREATE QUOTA qxml FOR INTERVAL 1 SECOND MAX queries = 0 TO `xml-internal`",
        "CREATE ROW POLICY rxml ON db.t USING 0 TO `xml-internal`",
        "CREATE SETTINGS PROFILE pxml SETTINGS max_memory_usage = 1 TO `xml-internal`",
    ]:
        err = node.query_and_get_error(q)
        assert "ACCESS_DENIED" in err or "Cannot target" in err, f"Expected ACCESS_DENIED for: {q}\nGot: {err}"


def test_targeting_sql_user_works():
    """Targeting a SQL-created user is allowed."""
    node.query(
        """
        CREATE DATABASE db;
        CREATE TABLE db.t (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
        CREATE USER sql_user;
        CREATE QUOTA qsql FOR INTERVAL 1 SECOND MAX queries = 100 TO sql_user;
        CREATE ROW POLICY rsql ON db.t USING x > 0 TO sql_user;
        CREATE SETTINGS PROFILE psql SETTINGS max_memory_usage = 1000000000 TO sql_user;
        """
    )


def test_to_all_does_not_affect_xml_user():
    """`TO ALL` is accepted at creation, but the runtime layer filters out XML users."""
    node.query(
        """
        CREATE USER sql_user;
        CREATE SETTINGS PROFILE pall SETTINGS max_block_size = 12345 TO ALL;
        """
    )

    value = node.query("SELECT getSetting('max_block_size')", user="sql_user").strip()
    assert value == "12345", f"SQL user should get the profile value, got: {value!r}"

    value = node.query("SELECT getSetting('max_block_size')", user="xml-internal").strip()
    assert value != "12345", f"XML user should keep its own value, got: {value!r}"


def test_to_all_except_current_user():
    """`TO ALL EXCEPT X` must not affect XML users."""
    node.query(
        """
        CREATE USER sql_user, sql_user2;
        CREATE SETTINGS PROFILE pall SETTINGS max_block_size = 12345 TO ALL EXCEPT sql_user;
        """
    )

    # The excepted SQL user keeps its own value.
    assert node.query("SELECT getSetting('max_block_size')", user="sql_user").strip() != "12345"
    # Another SQL user gets the profile value (profile applies via ALL).
    assert node.query("SELECT getSetting('max_block_size')", user="sql_user2").strip() == "12345"
    # XML user is not affected.
    assert node.query("SELECT getSetting('max_block_size')", user="xml-internal").strip() != "12345"


def test_runtime_added_xml_user_protected():
    """An XML user added via SYSTEM RELOAD CONFIG is also protected."""
    node.exec_in_container(
        ["bash", "-c", f"cat > /etc/clickhouse-server/users.d/runtime_xml_user.xml << 'EOF'\n{RUNTIME_USER_XML}\nEOF"]
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert "xml-runtime" in node.query(
        "SELECT name FROM system.users WHERE storage = 'users_xml' ORDER BY name"
    )

    err = node.query_and_get_error(
        "CREATE QUOTA qxml FOR INTERVAL 1 SECOND MAX queries = 0 TO `xml-runtime`"
    )
    assert "ACCESS_DENIED" in err or "Cannot target" in err


def test_backwards_compat_setting_disabled():
    """With the setting off, SQL entities can target XML users again (pre-fix behavior).

    The disable file is named `zz_*` so it loads after `enable_block.xml`.
    """
    config = """<clickhouse>
    <access_control_improvements>
        <forbid_sql_access_entities_targeting_users_config>false</forbid_sql_access_entities_targeting_users_config>
    </access_control_improvements>
</clickhouse>
"""
    node.exec_in_container(
        ["bash", "-c", f"cat > /etc/clickhouse-server/config.d/zz_disable_block.xml << 'EOF'\n{config}\nEOF"]
    )
    try:
        node.restart_clickhouse()
        node.query("CREATE QUOTA qxml FOR INTERVAL 1 SECOND MAX queries = 100 TO `xml-internal`")
    finally:
        node.exec_in_container(["rm", "-f", "/etc/clickhouse-server/config.d/zz_disable_block.xml"])
        node.restart_clickhouse()


def test_targeting_xml_role_is_allowed():
    """SQL-created policies can target XML-defined roles. The policy applies to SQL users granted that role."""
    setup_db_and_table()
    node.query(
        """
        CREATE USER sql_user;
        GRANT SELECT ON db.t TO sql_user;
        GRANT xml_role TO sql_user;
        CREATE ROW POLICY rxml ON db.t USING 0 TO xml_role;
        """
    )
    # sql_user has xml_role, policy applies, `USING 0` hides the row.
    assert node.query("SELECT count() FROM db.t", user="sql_user").strip() == "0"


def test_settings_profile_via_role_does_not_apply_to_xml_user():
    """A SQL settings profile attached to a SQL role does not leak into an XML user.

    A profile reaches a user as a parent only via the role's or user's own settings.
    Both require the SQL role to be enabled for the XML user, but `GRANT ... TO` a
    read-only users.xml user is rejected, so the profile is never merged for that user.
    """
    setup_db_and_table()
    node.query(
        """
        CREATE SETTINGS PROFILE psql SETTINGS max_block_size = 12345;
        CREATE ROLE sql_role SETTINGS PROFILE psql;
        CREATE USER sql_user DEFAULT ROLE sql_role;
        """
    )

    # The SQL user that has the role inherits the profile's setting.
    assert node.query("SELECT getSetting('max_block_size')", user="sql_user").strip() == "12345"

    # Granting the SQL role to the XML user is rejected: there is no path to enable it.
    assert "ACCESS_STORAGE_READONLY" in node.query_and_get_error("GRANT sql_role TO `xml-internal`")

    # The XML user keeps its own value, not the profile's.
    assert node.query("SELECT getSetting('max_block_size')", user="xml-internal").strip() != "12345"


def test_row_policy_to_all_does_not_affect_xml_user():
    """SQL row policy `TO ALL` is fully ignored for XML users, including under
    users_without_row_policies_can_read_rows=false."""
    setup_db_and_table()
    node.query(
        """
        CREATE USER sql_user;
        GRANT SELECT ON db.t TO sql_user;
        CREATE ROW POLICY rall ON db.t USING 0 TO ALL;
        """
    )

    # SQL user: policy applies, `USING 0` excludes every row.
    assert node.query("SELECT count() FROM db.t", user="sql_user").strip() == "0"
    # XML user: policy does not apply, row is visible.
    assert node.query("SELECT count() FROM db.t", user="xml-internal").strip() == "1"


def test_role_based_bypass_blocked():
    """A SQL policy targeting a SQL role does not apply to the XML user."""
    setup_db_and_table()
    node.query(
        """
        CREATE ROLE sql_role;
        CREATE USER sql_user DEFAULT ROLE sql_role;
        GRANT SELECT ON db.t TO sql_role;
        CREATE ROW POLICY rsql ON db.t USING 0 TO sql_role;
        """
    )

    # sql_user has sql_role, policy applies, `USING 0` hides the row.
    assert node.query("SELECT count() FROM db.t", user="sql_user").strip() == "0"
    # XML user: policy does not apply, row is visible.
    assert node.query("SELECT count() FROM db.t", user="xml-internal").strip() == "1"
