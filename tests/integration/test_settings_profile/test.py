import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


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


def test_settings_profile():
    # Set settings and constraints via CREATE SETTINGS PROFILE ... TO user 
    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER SETTINGS PROFILE xyz TO NONE")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")

    # Set settings and constraints via CREATE USER ... SETTINGS PROFILE
    instance.query("ALTER USER robin SETTINGS PROFILE xyz")
    assert instance.query("SHOW CREATE USER robin") == "CREATE USER robin SETTINGS PROFILE xyz\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER USER robin SETTINGS NONE")
    assert instance.query("SHOW CREATE USER robin") == "CREATE USER robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")


def test_settings_profile_from_granted_role():
    # Set settings and constraints via granted role
    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000")
    instance.query("CREATE ROLE worker SETTINGS PROFILE xyz")
    instance.query("GRANT worker TO robin")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000\n"
    assert instance.query("SHOW CREATE ROLE worker") == "CREATE ROLE worker SETTINGS PROFILE xyz\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error("SET max_memory_usage = 120000000", user="robin")

    instance.query("REVOKE worker FROM robin")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER ROLE worker SETTINGS NONE")
    instance.query("GRANT worker TO robin")
    assert instance.query("SHOW CREATE ROLE worker") == "CREATE ROLE worker\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")

    # Set settings and constraints via CREATE SETTINGS PROFILE ... TO granted role
    instance.query("ALTER SETTINGS PROFILE xyz TO worker")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO worker\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000001\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER SETTINGS PROFILE xyz TO NONE")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")


def test_inheritance_of_settings_profile():
    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000002 READONLY")
    instance.query("CREATE SETTINGS PROFILE alpha SETTINGS PROFILE xyz TO robin")
    assert instance.query("SHOW CREATE SETTINGS PROFILE xyz") == "CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000002 READONLY\n"
    assert instance.query("SHOW CREATE SETTINGS PROFILE alpha") == "CREATE SETTINGS PROFILE alpha SETTINGS INHERIT xyz TO robin\n"
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000002\n"
    assert "Setting max_memory_usage should not be changed" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")


def test_alter_and_drop():
    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS max_memory_usage = 100000003 MIN 90000000 MAX 110000000 TO robin")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "100000003\n"
    assert "Setting max_memory_usage shouldn't be less than 90000000" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")
    assert "Setting max_memory_usage shouldn't be greater than 110000000" in instance.query_and_get_error("SET max_memory_usage = 120000000", user="robin")

    instance.query("ALTER SETTINGS PROFILE xyz SETTINGS readonly=1")
    assert "Cannot modify 'max_memory_usage' setting in readonly mode" in instance.query_and_get_error("SET max_memory_usage = 80000000", user="robin")

    instance.query("DROP SETTINGS PROFILE xyz")
    assert instance.query("SELECT value FROM system.settings WHERE name = 'max_memory_usage'", user="robin") == "10000000000\n"
    instance.query("SET max_memory_usage = 80000000", user="robin")
    instance.query("SET max_memory_usage = 120000000", user="robin")


def test_allow_introspection():
    assert "Not enough privileges" in instance.query_and_get_error("SELECT demangle('a')", user="robin")
    
    instance.query("GRANT ALL ON *.* TO robin")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")

    instance.query("ALTER USER robin SETTINGS allow_introspection_functions=1")
    assert instance.query("SELECT demangle('a')", user="robin") == "signed char\n"

    instance.query("ALTER USER robin SETTINGS NONE")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")

    instance.query("CREATE SETTINGS PROFILE xyz SETTINGS allow_introspection_functions=1 TO robin")
    assert instance.query("SELECT demangle('a')", user="robin") == "signed char\n"

    instance.query("DROP SETTINGS PROFILE xyz")
    assert "Introspection functions are disabled" in instance.query_and_get_error("SELECT demangle('a')", user="robin")

    instance.query("REVOKE ALL ON *.* FROM robin")
    assert "Not enough privileges" in instance.query_and_get_error("SELECT demangle('a')", user="robin")
