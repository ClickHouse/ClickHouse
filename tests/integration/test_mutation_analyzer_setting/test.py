import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/default_profile.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_mutation_cannot_be_told_the_analyzer_is_off(start_cluster):
    # The analyzer is the only query analysis there is, and `allow_experimental_analyzer` is an
    # obsolete setting frozen at `1`. The default profile of this server carries a `0` all the same,
    # because a settings profile written in the configuration is applied without consulting the
    # constraint that refuses one.
    #
    # A query is normalized in `executeQuery`, so it reports the analysis that actually ran. A
    # mutation does not run there: it is executed in the background from a context built out of the
    # background context, which inherits the same profile. The analyzer analyzes it either way.

    assert (
        node.query("SELECT toUInt64(getSetting('allow_experimental_analyzer'))")
        == "1\n"
    )

    node.query(
        "CREATE TABLE t (k UInt8, v UInt64, probe UInt64) ENGINE = MergeTree ORDER BY k"
    )
    node.query("INSERT INTO t VALUES (1, 0, 0)")

    node.query(
        "ALTER TABLE t UPDATE "
        "v = toUInt64(getSetting('allow_experimental_analyzer')), "
        "probe = toUInt64(getSetting('min_free_disk_space_for_temporary_data')) "
        "WHERE 1",
        settings={"mutations_sync": 2},
    )

    # `probe` is what keeps this honest: it is `1234` in the same profile and `0` by default, so a
    # mutation that reports it proves the profile really did reach the context the mutation was
    # analyzed in - which is the context `v` would have read a `0` from.
    assert node.query("SELECT v, probe FROM t") == "1\t1234\n"
