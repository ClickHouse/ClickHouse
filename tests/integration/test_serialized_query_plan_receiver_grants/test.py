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


@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_receiver_checks_its_own_max_columns_to_read(
    receiver_profile, serialize_query_plan
):
    receiver_profile("max_columns_to_read = 1")
    error = select_error("SELECT sum(x + y) FROM dist", serialize_query_plan)
    assert "TOO_MANY_COLUMNS" in error
    assert select("SELECT sum(x) FROM dist", serialize_query_plan) == "45\n"
