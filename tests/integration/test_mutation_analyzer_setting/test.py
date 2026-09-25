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


def test_detach_of_a_part_an_unapplied_mutation_owes_is_refused(start_cluster):
    # The refusal that keeps a part an unfinished mutation still has to rewrite inside the table
    # resolves the mutation's `IN PARTITION` scope, so it has to resolve it to the same partition
    # the mutation executor does. Both read the setting above out of the default profile, so a
    # scope that depends on it is where the two would disagree: the executor is normalized and owes
    # the part, while a scope resolved under the raw `0` names a partition that does not exist and
    # would make the command look skippable. Allowing the detach marks the mutation done with the
    # rows never rewritten, and attaching the part back returns them unchanged - #120902.
    node.query("DROP TABLE IF EXISTS t_scope SYNC")
    node.query(
        "CREATE TABLE t_scope (k UInt8, v UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k"
    )
    node.query("SYSTEM STOP MERGES t_scope")
    node.query("INSERT INTO t_scope VALUES (1, 100)")

    # Armed first: the refusal below cannot pass vacuously against a mutation that already ended.
    node.query(
        "ALTER TABLE t_scope UPDATE v = 1100 "
        "IN PARTITION tuple(toUInt8(getSetting('allow_experimental_analyzer'))) WHERE 1",
        settings={"mutations_sync": 0},
    )
    assert (
        node.query(
            "SELECT is_done, parts_to_do FROM system.mutations "
            "WHERE database = currentDatabase() AND table = 't_scope'"
        )
        == "0\t1\n"
    )

    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error(
        "ALTER TABLE t_scope DETACH PART '1_1_1_0'"
    )

    # The complement, so the refusal is not asserted against a part nothing owed: the executor
    # resolves the same scope to the partition that holds the part, and rewrites it.
    node.query("SYSTEM START MERGES t_scope")
    node.query("ALTER TABLE t_scope UPDATE v = v WHERE 0", settings={"mutations_sync": 2})
    assert node.query("SELECT v FROM t_scope") == "1100\n"
