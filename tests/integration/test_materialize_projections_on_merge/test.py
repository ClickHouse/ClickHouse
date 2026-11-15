import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_rebuild_on_merge(started_cluster):
    """When materialize_projections_on_merge is enabled, ReplicatedMergeTree
    should rebuild projections that are missing from the source parts (e.g.
    because they were inserted with materialize_projections_on_insert = 0)
    when those parts are merged."""

    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (key UInt32, value UInt64,
            PROJECTION p1 (SELECT key, sum(value) GROUP BY key))
        ENGINE = ReplicatedMergeTree('/test_mat_proj_merge/t', '1')
        ORDER BY key
        SETTINGS materialize_projections_on_insert = 0, materialize_projections_on_merge = 1
        """
    )

    # Insert data — these parts have no projection (materialize_projections_on_insert = 0)
    node.query("INSERT INTO t SELECT number, number FROM numbers(3)")
    node.query("INSERT INTO t SELECT number + 10, number FROM numbers(3)")

    # No projection parts exist yet
    assert (
        node.query(
            """
            SELECT count()
            FROM system.projection_parts
            WHERE database = currentDatabase() AND table = 't' AND active
            """
        ).strip()
        == "0"
    )

    # All parts share the same (empty) projection set, so merge is allowed and rebuilds p1
    node.query("OPTIMIZE TABLE t FINAL")
    node.query("SYSTEM SYNC REPLICA t")

    # Verify a single merged part that now has projection p1
    result = node.query(
        """
        SELECT count() AS num_parts,
               arraySort(groupUniqArrayArray(projections)) AS all_projections
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't' AND active
        """
    ).strip()
    assert result == "1\t['p1']"

    node.query("DROP TABLE t SYNC")


def test_replicated_merge_without_setting_does_not_merge(started_cluster):
    """When materialize_projections_on_merge is disabled (default),
    ReplicatedMergeTree should not merge parts with different projection sets."""

    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (key UInt32, value UInt64,
            PROJECTION p1 (SELECT key, sum(value) GROUP BY key))
        ENGINE = ReplicatedMergeTree('/test_mat_proj_merge_disabled/t', '1')
        ORDER BY key
        SETTINGS materialize_projections_on_merge = 0
        """
    )

    node.query("INSERT INTO t SELECT number, number FROM numbers(3)")
    node.query("INSERT INTO t SELECT number + 10, number FROM numbers(3)")

    node.query(
        "ALTER TABLE t ADD PROJECTION p2 (SELECT key, max(value) GROUP BY key)"
    )

    node.query("INSERT INTO t SELECT number + 20, number FROM numbers(3)")
    node.query("INSERT INTO t SELECT number + 30, number FROM numbers(3)")

    # OPTIMIZE should not be able to merge all parts into one because projection sets differ.
    # With `optimize_throw_if_noop = 1` this surfaces as a `CANNOT_ASSIGN_OPTIMIZE` exception
    # mentioning the differing projection sets.
    with pytest.raises(QueryRuntimeException, match="different projection sets"):
        node.query(
            "OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1",
            settings={"server_logs_file": ""},
        )

    result = node.query(
        """
        SELECT count()
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't' AND active
        """
    ).strip()
    # Parts with different projection sets remain unmerged
    assert int(result) > 1

    node.query("DROP TABLE t SYNC")


def test_replicated_no_rebuild_when_setting_disabled(started_cluster):
    """When materialize_projections_on_merge is disabled and all parts have the
    same (empty) projection set, merge should not rebuild projections that exist
    in the table definition but are missing from parts."""

    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (key UInt32, value UInt64,
            PROJECTION p1 (SELECT key, sum(value) GROUP BY key))
        ENGINE = ReplicatedMergeTree('/test_mat_proj_merge_no_rebuild/t', '1')
        ORDER BY key
        SETTINGS materialize_projections_on_merge = 0,
                 materialize_projections_on_insert = 0
        """
    )

    # Insert data without materializing projections
    node.query("INSERT INTO t SELECT number, number FROM numbers(3)")
    node.query("INSERT INTO t SELECT number + 10, number FROM numbers(3)")

    # All parts have the same empty projection set, so merge is allowed
    node.query("OPTIMIZE TABLE t FINAL")
    node.query("SYSTEM SYNC REPLICA t")

    # The merged part should NOT have projection p1 rebuilt
    result = node.query(
        """
        SELECT projections
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't' AND active
        """
    ).strip()
    assert result == "[]"

    node.query("DROP TABLE t SYNC")
