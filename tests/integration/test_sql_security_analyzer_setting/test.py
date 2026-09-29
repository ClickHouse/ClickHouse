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
    # No `GRANT` for `definer_user`: it is defined in `users.xml`, and a configuration-defined user
    # with no `<grants>` section is granted everything already. Granting to it would not be a no-op
    # but an error - that storage is read-only, so `IAccessStorage::updateImpl` refuses with
    # `ACCESS_STORAGE_READONLY`.

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

    # Those two assertions are only worth something if the body really does read the definer's
    # profile: a body that read the invoker's context instead would report `1` as well, and nothing
    # in a passing run would say which of the two happened. So prove it with a value from the same
    # profile that is not normalized. `min_free_disk_space_for_temporary_data` is set there to
    # `1234`, and the invoker reports the default `0`.
    #
    # `toUInt64` is load-bearing. `getSetting` types its result from the value, so it is `UInt8` for
    # the `0` the invoker sees and `UInt16` for the `1234` the definer sees, and a view stores the
    # columns it was created with - here the invoker's `UInt8`. Without the cast the body's `1234`
    # is converted to that stored `UInt8` on the way out and the probe reads `210`, which looks
    # exactly like a body that saw neither value.
    node.query(
        "CREATE VIEW v_probe DEFINER = definer_user SQL SECURITY DEFINER "
        "AS SELECT toUInt64(getSetting('min_free_disk_space_for_temporary_data')) AS a"
    )
    assert node.query("SELECT * FROM v_probe") == "1234\n"
    assert (
        node.query(
            "SELECT toUInt64(getSetting('min_free_disk_space_for_temporary_data'))"
        )
        == "0\n"
    )
