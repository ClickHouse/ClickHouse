import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/definer_profile.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_definer_profile_cannot_disable_the_analyzer(start_cluster):
    # The analyzer is the only query analysis there is, and `allow_experimental_analyzer` is an
    # obsolete setting frozen at `1`. A settings profile written in the server configuration is
    # applied without consulting the constraint that refuses a `0`, so `definer_user` carries one.
    # The value is normalized where a query starts, so what a query reports is the analysis that
    # actually ran - and a `SQL SECURITY DEFINER` body, which runs in a context built from the
    # global one and the definer's profile rather than from the query, has to report it too.

    assert (
        node.query(
            "SELECT toUInt8(getSetting('allow_experimental_analyzer'))", user="definer_user"
        )
        == "1\n"
    )

    node.query("CREATE TABLE src (x UInt8) ENGINE = Memory")
    node.query("CREATE TABLE dst (a UInt8) ENGINE = Memory")
    node.query("GRANT SELECT, INSERT ON default.* TO definer_user")

    node.query(
        "CREATE VIEW v DEFINER = definer_user SQL SECURITY DEFINER "
        "AS SELECT toUInt8(getSetting('allow_experimental_analyzer')) AS a"
    )
    assert node.query("SELECT * FROM v") == "1\n"

    node.query(
        "CREATE MATERIALIZED VIEW mv TO dst DEFINER = definer_user SQL SECURITY DEFINER "
        "AS SELECT toUInt8(getSetting('allow_experimental_analyzer')) AS a FROM src"
    )
    node.query("INSERT INTO src VALUES (1)")
    assert node.query("SELECT * FROM dst") == "1\n"
