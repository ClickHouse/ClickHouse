import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
initiator = cluster.add_instance(
    "initiator", main_configs=["configs/remote_servers.xml"]
)
receiver = cluster.add_instance("receiver", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for node in (initiator, receiver):
            node.query(
                "CREATE TABLE leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x"
            )
            node.query("INSERT INTO leaf SELECT number, number FROM numbers(10)")
            node.query(
                "CREATE TABLE m (x UInt32, y UInt32) ENGINE = Merge(default, '^leaf$')"
            )
            node.query("CREATE USER u")
            node.query("GRANT SELECT ON default.leaf TO u")
        initiator.query(
            "CREATE TABLE dist (x UInt32, y UInt32) ENGINE = Distributed(receiver_only, default, m)"
        )
        initiator.query("GRANT SELECT ON default.dist TO u")
        initiator.query("GRANT SELECT(x) ON default.m TO u")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def receiver_grant():
    def grant(columns):
        receiver.query(f"GRANT SELECT({columns}) ON default.m TO u")

    yield grant
    receiver.query("REVOKE SELECT ON default.m FROM u")
    for node in (initiator, receiver):
        node.query("DROP ROW POLICY IF EXISTS p ON default.m")


def select(query, serialize_query_plan, **settings):
    settings["serialize_query_plan"] = serialize_query_plan
    return initiator.query(query, user="u", settings=settings)


def select_error(query, serialize_query_plan, **settings):
    settings["serialize_query_plan"] = serialize_query_plan
    return initiator.query_and_get_error(query, user="u", settings=settings)


# The receiver checks its own grants on the `Merge` table, whether it gets the query text or a plan.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_checks_its_own_column_grants(receiver_grant, serialize_query_plan):
    receiver_grant("y")
    error = select_error("SELECT sum(x) FROM dist", serialize_query_plan)
    assert "ACCESS_DENIED" in error
    assert "default.m" in error
    # A grant on any column is enough to count rows.
    assert select("SELECT count() FROM dist", serialize_query_plan) == "10\n"

    receiver_grant("x")
    assert select("SELECT sum(x) FROM dist", serialize_query_plan) == "45\n"


# A shipped plan reads the policy column for its filter step, which needs no grant.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_policy_column_needs_no_grant_on_receiver(receiver_grant, serialize_query_plan):
    for node in (initiator, receiver):
        node.query("CREATE ROW POLICY p ON default.m USING y < 5 TO u")
    receiver_grant("x")
    assert select("SELECT sum(x) FROM dist", serialize_query_plan) == "10\n"


@pytest.fixture
def initiator_grant_y():
    initiator.query("GRANT SELECT(y) ON default.m TO u")
    yield
    initiator.query("REVOKE SELECT(y) ON default.m FROM u")


# The filter of the query reads `y` on the receiver, as with the query text.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_checks_its_own_grants_on_additional_table_filters(
    receiver_grant, initiator_grant_y, serialize_query_plan
):
    filters = {"additional_table_filters": "{'default.m': 'y < 5'}"}
    receiver_grant("x")
    error = select_error("SELECT sum(x) FROM dist", serialize_query_plan, **filters)
    assert "ACCESS_DENIED" in error
    assert "default.m" in error

    receiver_grant("y")
    assert select("SELECT sum(x) FROM dist", serialize_query_plan, **filters) == "10\n"


@pytest.fixture
def receiver_profile(receiver_grant, initiator_grant_y):
    receiver_grant("x, y")

    def create(setting):
        receiver.query(
            f"CREATE SETTINGS PROFILE receiver_profile SETTINGS {setting} READONLY TO u"
        )

    yield create
    receiver.query("DROP SETTINGS PROFILE IF EXISTS receiver_profile")


# The receiver applies the settings profile of its own user, which the initiator does not have.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_applies_its_own_additional_table_filters(
    receiver_profile, serialize_query_plan
):
    receiver_profile("additional_table_filters = '{\\'default.m\\': \\'y < 5\\'}'")
    assert select("SELECT sum(x) FROM dist", serialize_query_plan) == "10\n"


# The entry of the query reaches the receiver too, where `getSetting` reads the receiver's own value.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_resolves_additional_table_filters_with_its_own_settings(
    receiver_profile, serialize_query_plan
):
    receiver_profile("max_threads = 1")
    filters = {
        "additional_table_filters": "{'default.m': 'x < getSetting(\\'max_threads\\')'}",
        "max_threads": 2,
    }
    assert select("SELECT count() FROM dist", serialize_query_plan, **filters) == "1\n"

    # Also next to a constant that changes with each analysis of the entry.
    filters["additional_table_filters"] = (
        "{'default.m': 'x < getSetting(\\'max_threads\\') AND y != randConstant() + 10'}"
    )
    assert select("SELECT count() FROM dist", serialize_query_plan, **filters) == "1\n"

    # Also inside a scalar subquery, which each node evaluates on its own.
    filters["additional_table_filters"] = (
        "{'default.m': 'x < (SELECT getSetting(\\'max_threads\\'))'}"
    )
    assert select("SELECT count() FROM dist", serialize_query_plan, **filters) == "1\n"

    # Also over a policy of the `Merge` table, which the plan applies as a step under the entry.
    for node in (initiator, receiver):
        node.query("CREATE ROW POLICY p ON default.m USING y < 100 TO u")
    filters["additional_table_filters"] = (
        "{'default.m': 'x < getSetting(\\'max_threads\\')'}"
    )
    assert select("SELECT count() FROM dist", serialize_query_plan, **filters) == "1\n"


@pytest.fixture
def lookup_table():
    for node, value in ((initiator, 7), (receiver, 3)):
        node.query("CREATE TABLE lim (v UInt32) ENGINE = MergeTree ORDER BY v")
        node.query(f"INSERT INTO lim VALUES ({value})")
        node.query("GRANT SELECT ON default.lim TO u")
        node.query("CREATE TABLE kv (k UInt64, v UInt32) ENGINE = MergeTree ORDER BY k")
        node.query(f"INSERT INTO kv VALUES (1, {value})")
        node.query(
            "CREATE DICTIONARY d (k UInt64, v UInt32) PRIMARY KEY k "
            "SOURCE(CLICKHOUSE(TABLE 'kv')) LIFETIME(0) LAYOUT(FLAT())"
        )
        node.query("GRANT dictGet ON default.d TO u")
        node.exec_in_container(
            ["bash", "-c", f"printf {value} > /var/lib/clickhouse/user_files/limit.txt"]
        )
        node.query("GRANT READ ON FILE TO u")
    yield
    for node in (initiator, receiver):
        node.query("REVOKE READ ON FILE FROM u")
        node.query("DROP TABLE IF EXISTS lim")
        node.query("DROP DICTIONARY IF EXISTS d")
        node.query("DROP TABLE IF EXISTS kv")


# A scalar subquery that reads a table gets the rows of the receiver there, so an entry of the query with the same text
# does not replace the entry of the receiver.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_evaluates_its_own_scalar_subquery_over_a_table(
    receiver_profile, lookup_table, serialize_query_plan
):
    receiver_profile(
        "additional_table_filters = '{\\'default.m\\': \\'x < (SELECT max(v) FROM default.lim)\\'}'"
    )
    filters = {
        "additional_table_filters": "{'default.m': 'x < (SELECT max(v) FROM default.lim)'}"
    }
    assert select("SELECT count() FROM dist", serialize_query_plan, **filters) == "3\n"


# Also through a dictionary or a file, and next to a constant that changes with each analysis of the entry.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
@pytest.mark.parametrize(
    "entry",
    [
        "x < (SELECT dictGet('default.d', 'v', materialize(toUInt64(1))))",
        "x < (SELECT toUInt32(file(materialize('limit.txt'))))",
        "x < (SELECT max(v) FROM default.lim WHERE v != randConstant()) AND y != randConstant() + 10",
    ],
)
def test_receiver_evaluates_its_own_scalar_subquery_over_server_state(
    receiver_profile, lookup_table, serialize_query_plan, entry
):
    filters = "{'default.m': '" + entry.replace("'", "\\'") + "'}"
    escaped = filters.replace("\\", "\\\\").replace("'", "\\'")
    receiver_profile(f"additional_table_filters = '{escaped}'")
    assert (
        select(
            "SELECT count() FROM dist",
            serialize_query_plan,
            additional_table_filters=filters,
        )
        == "3\n"
    )


@pytest.fixture
def initiator_only_database():
    initiator.query("CREATE DATABASE db1")
    for node, database in ((initiator, "db1"), (receiver, "default")):
        node.query(
            f"CREATE TABLE {database}.lim (v UInt32) ENGINE = MergeTree ORDER BY v"
        )
        node.query(f"INSERT INTO {database}.lim VALUES (1), (2)")
        node.query(f"GRANT SELECT ON {database}.lim TO u")
    yield
    initiator.query("DROP DATABASE db1")
    receiver.query("DROP TABLE lim")


# The subquery of the entry reads `lim` of the current database, which is `db1` only on the initiator. The receiver
# plans the entry with its own current database, and the set of the initiator's plan is neither resolved nor built.
@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_resolves_the_subquery_of_additional_table_filters_itself(
    receiver_grant, initiator_grant_y, initiator_only_database, serialize_query_plan
):
    receiver_grant("x, y")
    settings = {
        "serialize_query_plan": serialize_query_plan,
        "additional_table_filters": "{'default.m': 'y IN (SELECT v FROM lim)'}",
    }
    assert (
        initiator.query(
            "SELECT count() FROM default.dist",
            user="u",
            database="db1",
            settings=settings,
        )
        == "2\n"
    )


@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_checks_its_own_max_columns_to_read(
    receiver_profile, serialize_query_plan
):
    receiver_profile("max_columns_to_read = 1")
    error = select_error("SELECT sum(x + y) FROM dist", serialize_query_plan)
    assert "TOO_MANY_COLUMNS" in error
    assert select("SELECT sum(x) FROM dist", serialize_query_plan) == "45\n"
