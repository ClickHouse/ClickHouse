import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/no_default_constraints.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_system_settings(started_cluster):
    assert (
        instance.query(
            "SELECT name, value, min, max, readonly from system.settings WHERE name = 'force_index_by_date'"
        )
        == "force_index_by_date\t0\t\\N\t\\N\t1\n"
    )

    assert (
        instance.query(
            "SELECT name, value, min, max, readonly from system.settings WHERE name = 'max_memory_usage'"
        )
        == "max_memory_usage\t10000000000\t5000000000\t20000000000\t0\n"
    )

    assert (
        instance.query(
            "SELECT name, value, min, max, readonly from system.settings WHERE name = 'readonly'"
        )
        == "readonly\t0\t\\N\t\\N\t0\n"
    )

    assert (
        instance.query(
            "SELECT name, value, min, max, readonly from system.settings WHERE name = 'alter_sync'"
        )
        == "alter_sync\t2\t\\N\t\\N\t1\n"
    )


def test_system_constraints(started_cluster):
    assert_query_settings(
        instance,
        "SELECT 1",
        settings={"readonly": 0},
        exception="Cannot modify 'readonly'",
        user="readonly_user",
    )

    assert_query_settings(
        instance,
        "SELECT 1",
        settings={"allow_ddl": 1},
        exception="Cannot modify 'allow_ddl'",
        user="no_dll_user",
    )


def test_read_only_constraint(started_cluster):
    # Default value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='force_index_by_date'",
        settings={},
        result="0",
    )

    # Invalid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='force_index_by_date'",
        settings={"force_index_by_date": 1},
        result=None,
        exception="Setting force_index_by_date should not be changed",
    )

    # Invalid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name= 'replication_alter_partitions_sync'",
        settings={"replication_alter_partitions_sync": 1},
        result=None,
        exception="Setting alter_sync should not be changed",
    )

    # Invalid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name= 'alter_sync'",
        settings={"alter_sync": 1},
        result=None,
        exception="Setting alter_sync should not be changed",
    )


def test_min_constraint(started_cluster):
    # Default value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        {},
        result="10000000000",
    )

    # Valid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 5000000000},
        result="5000000000",
    )

    # Invalid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 4999999999},
        result=None,
        exception="Setting max_memory_usage shouldn't be less than 5000000000",
    )


def test_max_constraint(started_cluster):
    # Default value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        {},
        result="10000000000",
    )

    # Valid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 20000000000},
        result="20000000000",
    )

    # Invalid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 20000000001},
        result=None,
        exception="Setting max_memory_usage shouldn't be greater than 20000000000",
    )


def test_disallowed_constraint(started_cluster):
    # Default value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        {},
        result="10000000000",
    )

    # Valid value
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 6000000002},
        result="6000000002",
    )

    # Invalid values
    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 6000000000},
        result=None,
        exception=" Setting max_memory_usage shouldn't be 6000000000",
    )

    assert_query_settings(
        instance,
        "SELECT value FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 6000000001},
        result=None,
        exception=" Setting max_memory_usage shouldn't be 6000000001",
    )

    # Check that the disallowed values returned are as expected
    assert_query_settings(
        instance,
        "SELECT disallowed_values FROM system.settings WHERE name='max_memory_usage'",
        settings={"max_memory_usage": 10000000000},
        result="['6000000000','6000000001']"
    )


def test_disallowed_constraint_merge_tree(started_cluster):
    # Default value
    assert_query_settings(
        instance,
        "SELECT value FROM system.merge_tree_settings WHERE name='max_parts_in_total'",
        {},
        result="100000",
    )

    assert_query_settings(
        instance,
        "SELECT disallowed_values FROM system.merge_tree_settings WHERE name='max_parts_in_total'",
        {},
        result="['5000']",
    )

    # Test by creating a merge tree table with valid setting, followed by alters with both valid and invalid (disallowed) settings
    # Valid values
    instance.query("CREATE TABLE test (x Int) ENGINE=MergeTree ORDER BY x SETTINGS max_parts_in_total=2000")
    instance.query("ALTER TABLE test MODIFY SETTING max_parts_in_total=3000")
    # Invalid value
    assert " Setting max_parts_in_total shouldn't be 5000." in instance.query_and_get_error(
        "ALTER TABLE test MODIFY SETTING max_parts_in_total=5000")
    # Clean up
    instance.query("DROP TABLE IF EXISTS test")


def test_merge_tree_setting_constraint_via_alias(started_cluster):
    """The MergeTreeSettings overload of checkImpl must resolve aliases before
    looking up the constraint, in both directions: the constraint may be declared
    on the canonical name or on the alias, and the query may name either side.
    All four combinations must hit the same constraint.

    enable_block_number_column has the alias allow_experimental_block_number_column.
    The default profile declares the constraint on the canonical name; the
    alias_constraint_profile declares it on the alias name.
    Without the upfront resolveName, querying via an alias against a constraint
    declared on the canonical name silently bypasses the constraint."""

    instance.query("DROP TABLE IF EXISTS test_alias_constraint")
    instance.query(
        "CREATE TABLE test_alias_constraint (x Int) ENGINE=MergeTree ORDER BY x"
    )

    # default user: constraint declared on canonical name.
    for setting in ("enable_block_number_column", "allow_experimental_block_number_column"):
        assert "should not be changed" in instance.query_and_get_error(
            f"ALTER TABLE test_alias_constraint MODIFY SETTING {setting}=1"
        )
        assert "should not be changed" in instance.query_and_get_error(
            "CREATE TABLE test_alias_constraint_create (x Int) ENGINE=MergeTree ORDER BY x "
            f"SETTINGS {setting}=1"
        )

    # alias_constraint_user: constraint declared on alias name.
    for setting in ("enable_block_number_column", "allow_experimental_block_number_column"):
        assert "should not be changed" in instance.query_and_get_error(
            f"ALTER TABLE test_alias_constraint MODIFY SETTING {setting}=1",
            user="alias_constraint_user",
        )

    instance.query("DROP TABLE IF EXISTS test_alias_constraint")


def test_merge_tree_setting_constraint_via_granted_role(started_cluster):
    """A constraint declared on a granted role's settings profile (not the
    user's own base profile) must still be enforced, including through
    MergeTreeSettings alias resolution. The system-wide default_profile is
    overridden to an empty profile (see configs/no_default_constraints.xml),
    so an SQL-created user without an explicit profile inherits no merge_tree
    constraints; the constraint reaches the user only via the granted role."""

    instance.query("DROP TABLE IF EXISTS test_role_constraint")
    instance.query("DROP USER IF EXISTS role_constraint_user")
    instance.query("DROP ROLE IF EXISTS role_with_block_number_constraint")
    instance.query("DROP SETTINGS PROFILE IF EXISTS role_block_number_profile")

    instance.query("CREATE USER role_constraint_user")
    instance.query(
        "CREATE SETTINGS PROFILE role_block_number_profile "
        "SETTINGS merge_tree_enable_block_number_column CONST"
    )
    instance.query(
        "CREATE ROLE role_with_block_number_constraint "
        "SETTINGS PROFILE role_block_number_profile"
    )
    instance.query(
        "GRANT role_with_block_number_constraint TO role_constraint_user"
    )

    instance.query(
        "CREATE TABLE test_role_constraint (x Int) ENGINE=MergeTree ORDER BY x"
    )
    instance.query(
        "GRANT ALTER ON test_role_constraint TO role_with_block_number_constraint"
    )

    try:
        for setting in (
            "enable_block_number_column",
            "allow_experimental_block_number_column",
        ):
            assert "should not be changed" in instance.query_and_get_error(
                f"ALTER TABLE test_role_constraint MODIFY SETTING {setting}=1",
                user="role_constraint_user",
            )
    finally:
        instance.query("DROP TABLE IF EXISTS test_role_constraint")
        instance.query("DROP USER IF EXISTS role_constraint_user")
        instance.query("DROP ROLE IF EXISTS role_with_block_number_constraint")
        instance.query("DROP SETTINGS PROFILE IF EXISTS role_block_number_profile")


def test_disallowed_value_is_clamped_not_thrown(started_cluster):
    """The disallowed_values check must respect the `reaction` parameter:
    initial queries throw, but secondary queries (clamp path) must silently
    drop the change instead of throwing. Otherwise a value that happens to
    match a disallowed entry can break secondary queries even though the
    user set a value the initiator already accepted."""

    # Initial query: throws (unchanged behavior).
    assert " Setting max_memory_usage shouldn't be 6000000000" in instance.query_and_get_error(
        "SELECT 1", settings={"max_memory_usage": 6000000000}
    )

    # Secondary query: must not throw. The clamp implementation should leave
    # the value alone (no clamp target for disallowed entries) and proceed.
    result = instance.exec_in_container(
        [
            "clickhouse",
            "client",
            "--query_kind=secondary_query",
            "--max_memory_usage=6000000000",
            "--query=SELECT 1",
        ]
    )
    assert result.strip() == "1"


def test_disallowed_value_overlapping_clamp_target(started_cluster):
    """When a clamp target (min/max) coincides with a disallowed entry, the
    disallowed check must see the post-clamp value rather than the pre-clamp
    one. Otherwise an out-of-range value gets clamped to a disallowed value
    and is silently accepted.

    The overlap_constraint_user has min=5000000000 and disallowed=5000000000
    on max_memory_usage. A secondary query that sends max_memory_usage below
    the min would clamp up to 5000000000 — which is disallowed — and must
    therefore be dropped, not applied."""

    result = instance.exec_in_container(
        [
            "clickhouse",
            "client",
            "--user=overlap_constraint_user",
            "--query_kind=secondary_query",
            "--max_memory_usage=4000000000",
            "--query=SELECT getSetting('max_memory_usage')",
        ]
    )
    # Default max_memory_usage in 0_stateless tests is non-zero; we only need to
    # verify the clamp-to-disallowed value is NOT what we ended up with.
    assert result.strip() != "5000000000", (
        f"Clamped value 5000000000 is on the disallowed list and must not be applied, got: {result!r}"
    )


def test_create_table_query_setting_constraints(started_cluster):
    """Test that query-level settings passed in CREATE TABLE's engine SETTINGS clause
    are validated against setting constraints (not bypassing them)."""

    # The settings in CREATE TABLE ... SETTINGS should be rejected:
    assert "should not be changed" in instance.query_and_get_error(
        "CREATE TABLE test_constraint (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS force_index_by_date = 1"
    )

    # Also test with max_memory_usage min constraint via CREATE TABLE SETTINGS
    assert "shouldn't be less than" in instance.query_and_get_error(
        "CREATE TABLE test_constraint (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS max_memory_usage = 1"
    )


def assert_query_settings(
    instance, query, settings, result=None, exception=None, user=None
):
    """
    Try and send the query with custom settings via all available methods:
    1. TCP Protocol with settings packet
    2. HTTP Protocol with settings params
    3. TCP Protocol with session level settings
    4. TCP Protocol with query level settings
    """

    if not settings:
        settings = {}

    # tcp level settings
    if exception:
        assert exception in instance.query_and_get_error(
            query, settings=settings, user=user
        )
    else:
        assert instance.query(query, settings=settings, user=user).strip() == result

    # http level settings
    if exception:
        assert exception in instance.http_query_and_get_error(
            query, params=settings, user=user
        )
    else:
        assert instance.http_query(query, params=settings, user=user).strip() == result

    # session level settings
    queries = ""

    for k, v in list(settings.items()):
        queries += "SET {}={};\n".format(k, v)

    queries += query

    if exception:
        assert exception in instance.query_and_get_error(queries, user=user)
    else:
        assert instance.query(queries, user=user).strip() == result

    if settings:
        query += " SETTINGS "
        for ix, (k, v) in enumerate(settings.items()):
            query += "{} = {}".format(k, v)
            if ix != len(settings) - 1:
                query += ", "

    if exception:
        assert exception in instance.query_and_get_error(queries, user=user)
    else:
        assert instance.query(queries, user=user).strip() == result
