import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


def system_settings_profile(profile_name):
    return TSV(instance.query(
        "SELECT name, storage, num_elements, apply_to_all, apply_to_list, apply_to_except FROM system.settings_profiles WHERE name='" + profile_name + "'"))


def system_settings_profile_elements(profile_name=None, user_name=None, role_name=None):
    where = ""
    if profile_name:
        where = " WHERE profile_name='" + profile_name + "'"
    elif user_name:
        where = " WHERE user_name='" + user_name + "'"
    elif role_name:
        where = " WHERE role_name='" + role_name + "'"
    return TSV(instance.query("SELECT * FROM system.settings_profile_elements" + where))


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()

        instance.query("CREATE USER robin")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_after_test():
    try:
        yield
    finally:
        instance.query("CREATE USER OR REPLACE robin")
        instance.query("DROP ROLE IF EXISTS worker")
        instance.query("DROP SETTINGS PROFILE IF EXISTS xyz, alpha")


def test_smoke():
    # Set settings and constraints via CREATE SETTINGS PROFILE ... TO user
    instance.query(
        "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error(
        "SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error(
        "SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile("xyz") == [["xyz", "local directory", 1, 0, "['robin']", "[]"]]
    assert system_settings_profile_elements(profile_name="xyz") == [
        ["xyz", "\\N", "\\N", 0, "max_memory_usage", 100000001, 90000000, 110000000, "\\N", "\\N"]]

    instance.query("ALTER SETTINGS PROFILE xyz TO NONE")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile("xyz") == [["xyz", "local directory", 1, 0, "[]", "[]"]]
    assert system_settings_profile_elements(user_name="robin") == []

    # Set settings and constraints via CREATE USER ... SETTINGS PROFILE
    instance.query("ALTER USER robin SETTINGS PROFILE xyz")
    assert instance.query("SHOW CREATE USER robin") == "CREATE USER robin SETTINGS PROFILE xyz\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error(
        "SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error(
        "SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile_elements(user_name="robin") == [
        ["\\N", "robin", "\\N", 0, "\\N", "\\N", "\\N", "\\N", "\\N", "xyz"]]

    instance.query("ALTER USER robin SETTINGS NONE")
    assert instance.query("SHOW CREATE USER robin") == "CREATE USER robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile_elements(user_name="robin") == []


def test_settings_from_granted_role():
    # Set settings and constraints via granted role
    instance.query(
        "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MAX 110000000, max_ast_depth = 2000")
    instance.query("CREATE ROLE worker SETTINGS PROFILE xyz")
    instance.query("GRANT worker TO robin")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MAX 110000000, max_ast_depth = 2000\n"
    assert instance.query("SHOW CREATE ROLE worker") == "CREATE ROLE worker SETTINGS PROFILE xyz\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000001\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_ast_depth'", user="robin") == "2000\n"
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error(
        "SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile("xyz") == [["xyz", "local directory", 2, 0, "[]", "[]"]]
    assert system_settings_profile_elements(profile_name="xyz") == [
        ["xyz", "\\N", "\\N", 0, "max_memory_usage", 100000001, "\\N", 110000000, "\\N", "\\N"],
        ["xyz", "\\N", "\\N", 1, "max_ast_depth", 2000, "\\N", "\\N", "\\N", "\\N"]]
    assert system_settings_profile_elements(role_name="worker") == [
        ["\\N", "\\N", "worker", 0, "\\N", "\\N", "\\N", "\\N", "\\N", "xyz"]]

    instance.query("REVOKE worker FROM robin")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER ROLE worker SETTINGS NONE")
    instance.query("GRANT worker TO robin")
    assert instance.query("SHOW CREATE ROLE worker") == "CREATE ROLE worker\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile_elements(role_name="worker") == []

    # Set settings and constraints via CREATE SETTINGS PROFILE ... TO granted role
    instance.query("ALTER SETTINGS PROFILE xyz TO worker")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MAX 110000000, max_ast_depth = 2000 TO worker\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error(
        "SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile("xyz") == [["xyz", "local directory", 2, 0, "['worker']", "[]"]]

    instance.query("ALTER SETTINGS PROFILE xyz TO NONE")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MAX 110000000, max_ast_depth = 2000\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 120000000", user="robin")
    assert system_settings_profile("xyz") == [["xyz", "local directory", 2, 0, "[]", "[]"]]


def test_inheritance():
    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000002 READONLY")
    instance.query("CREATE SETTINGS PROFILE alpha SETTINGS PROFILE xyz TO robin")
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000002 READONLY\n"
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE alpha") == "CREATE SETTINGS PROFILE alpha SETTINGS INHERIT xyz TO robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000002\n"
    assert "Setting max_memory_usage should not be changed" in instance.query_and_get_error(
        "SET max_memory_usage = 80000000", user="robin")

    assert system_settings_profile("xyz") == [["xyz", "local directory", 1, 0, "[]", "[]"]]
    assert system_settings_profile_elements(profile_name="xyz") == [
        ["xyz", "\\N", "\\N", 0, "max_memory_usage", 100000002, "\\N", "\\N", 1, "\\N"]]
    assert system_settings_profile("alpha") == [["alpha", "local directory", 1, 0, "['robin']", "[]"]]
    assert system_settings_profile_elements(profile_name="alpha") == [
        ["alpha", "\\N", "\\N", 0, "\\N", "\\N", "\\N", "\\N", "\\N", "xyz"]]
    assert system_settings_profile_elements(user_name="robin") == []


def test_alter_and_drop():
    instance.query(
        "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000003 MIN 90000000 MAX 110000000 TO robin")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "100000003\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error(
        "SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error(
        "SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER SETTINGS PROFILE xyz SETTINGS readonly=1")
    assert "Cannot modify 'max_memory_usage' setting in readonly mode" in instance.query_and_get_error(
        "SET max_memory_usage = 80000000", user="robin")

    instance.query("DROP SETTINGS PROFILE xyz")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'",
                          user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")


def test_show_profiles():
    instance.query("CREATE SETTINGS PROFILE xyz")
    assert instance.query("SHOW SETTINGS PROFILES") == "default\nreadonly\nxyz\n"
    assert instance.query("SHOW PROFILES") == "default\nreadonly\nxyz\n"

    assert instance.query("SHOW CREATE PROFILE xyz") == "CREATE SETTINGS PROFILE xyz\n"
    assert instance.query(
        "SHOW CREATE SETTINGS PROFILE default") == "CREATE SETTINGS PROFILE default SETTINGS max_memory_usage = 10000000000, load_balancing = \\'random\\'\n"
    assert instance.query(
        "SHOW CREATE PROFILES") == "CREATE SETTINGS PROFILE default SETTINGS max_memory_usage = 10000000000, load_balancing = \\'random\\'\n" \
                                   "CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1\n" \
                                   "CREATE SETTINGS PROFILE xyz\n"

    expected_access = "CREATE SETTINGS PROFILE default SETTINGS max_memory_usage = 10000000000, load_balancing = \\'random\\'\n" \
                      "CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1\n" \
                      "CREATE SETTINGS PROFILE xyz\n"
    assert expected_access in instance.query("SHOW ACCESS")


def test_allow_ddl():
    assert "it's necessary to have grant" in instance.query_and_get_error("CREATE TABLE tbl(a Int32) ENGINE=Log", user="robin")
    assert "it's necessary to have grant" in instance.query_and_get_error("GRANT CREATE ON tbl TO robin", user="robin")
    assert "DDL queries are prohibited" in instance.query_and_get_error("CREATE TABLE tbl(a Int32) ENGINE=Log", settings={"allow_ddl": 0})

    instance.query("GRANT CREATE ON tbl TO robin")
    instance.query("CREATE TABLE tbl(a Int32) ENGINE=Log", user="robin")
    instance.query("DROP TABLE tbl")


def test_allow_introspection():
    assert instance.query("SELECT demangle('a')", settings={"allow_introspection_functions": 1}) == "signed char\n"

    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')")
    assert "it's necessary to have grant" in instance.query_and_get_error("SELECT demangle('a')", user="robin")
    assert "it's necessary to have grant" in instance.query_and_get_error("SELECT demangle('a')", user="robin", settings={"allow_introspection_functions": 1})

    instance.query("GRANT demangle ON *.* TO robin")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")
    assert instance.query("SELECT demangle('a')", user="robin", settings={"allow_introspection_functions": 1}) == "signed char\n"

    instance.query("ALTER USER robin SETTINGS allow_introspection_functions=1")
    assert instance.query("SELECT demangle('a')", user="robin") == "signed char\n"

    instance.query("ALTER USER robin SETTINGS NONE")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")

    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS allow_introspection_functions=1 TO robin")
    assert instance.query("SELECT demangle('a')", user="robin") == "signed char\n"

    instance.query("DROP SETTINGS PROFILE xyz")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")

    instance.query("REVOKE demangle ON *.* FROM robin", settings={"allow_introspection_functions": 1})
    assert "it's necessary to have grant" in instance.query_and_get_error("SELECT demangle('a')", user="robin")
