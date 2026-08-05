import pytest

from helpers.cluster import ClickHouseCluster

_OLD_VERSION = "26.5"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=_OLD_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_statistics_minmax_upgrade(start_cluster):
    # --- Old version: create a table with explicit minmax statistics and fill it. ---
    node.query("DROP TABLE IF EXISTS t_minmax SYNC")
    node.query(
        """
        CREATE TABLE t_minmax
        (
            id UInt64,
            value Int64 STATISTICS(minmax)
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, allow_experimental_statistics = 1
        """,
    )

    node.query("SYSTEM STOP MERGES t_minmax")
    # Two non-overlapping parts so the minmax statistics can prune one of them.
    # `materialize_statistics_on_insert` does not exist on the old version, so the statistics
    # objects are persisted explicitly below via MATERIALIZE STATISTICS.
    node.query("INSERT INTO t_minmax SELECT number, number FROM numbers(1000)")
    node.query(
        "INSERT INTO t_minmax SELECT number + 1000, number + 1000000 FROM numbers(1000)"
    )

    assert node.query("SELECT count() FROM t_minmax").strip() == "2000"
    create_before = node.query("SHOW CREATE TABLE t_minmax")
    assert "STATISTICS(minmax)" in create_before

    # --- Upgrade to the current version. ---
    node.restart_with_latest_version()

    # The table with deprecated `minmax` statistics still attaches and keeps its data.
    assert node.query("SELECT count() FROM t_minmax").strip() == "2000"
    # The declared `minmax` statistics is preserved in the metadata.
    assert "STATISTICS(minmax)" in node.query("SHOW CREATE TABLE t_minmax")

    # The old parts that carry `minmax` statistics are still readable.
    assert (
        node.query(
            "SELECT count() FROM t_minmax WHERE value > 500000",
            settings={"use_statistics_for_part_pruning": 1},
        ).strip()
        == "1000"
    )

    # DETACH / ATTACH must tolerate the deprecated statistics type.
    node.query("DETACH TABLE t_minmax")
    node.query("ATTACH TABLE t_minmax")
    assert node.query("SELECT count() FROM t_minmax").strip() == "2000"

    # An unrelated ALTER on the upgraded table must not be rejected because of the explicit
    # `minmax` statistics it still carries in its metadata (loaded from disk with is_implicit=false).
    # `checkAlterIsPossible` validates the whole post-alter metadata, so only `minmax` newly introduced
    # by the statement should be rejected; pre-existing `minmax` is grandfathered.
    node.query("ALTER TABLE t_minmax COMMENT COLUMN id 'the primary key'")
    assert "the primary key" in node.query("SHOW CREATE TABLE t_minmax")
    # The declared `minmax` statistics is still preserved after the unrelated ALTER.
    assert "STATISTICS(minmax)" in node.query("SHOW CREATE TABLE t_minmax")
    # Re-stating the column with its pre-existing explicit `minmax` is grandfathered too.
    node.query("ALTER TABLE t_minmax MODIFY COLUMN value Int64 STATISTICS(minmax)")
    assert "STATISTICS(minmax)" in node.query("SHOW CREATE TABLE t_minmax")
    assert node.query("SELECT count() FROM t_minmax").strip() == "2000"

    # Introducing a NEW `minmax` statistics on the upgraded table is still rejected.
    assert "INCORRECT_QUERY" in node.query_and_get_error(
        "ALTER TABLE t_minmax ADD STATISTICS id TYPE minmax",
        settings={"allow_statistics": 1},
    )

    # New inserts and a merge across old and new parts work.
    node.query("SYSTEM START MERGES t_minmax")
    node.query(
        "INSERT INTO t_minmax SELECT number + 5000, number + 5000 FROM numbers(1000)",
        settings={"materialize_statistics_on_insert": 1},
    )
    node.query("OPTIMIZE TABLE t_minmax FINAL", settings={"optimize_throw_if_noop": 1})
    assert node.query("SELECT count() FROM t_minmax").strip() == "3000"
    assert (
        node.query(
            "SELECT count() FROM t_minmax WHERE value > 500000",
            settings={"use_statistics_for_part_pruning": 1},
        ).strip()
        == "1000"
    )

    # --- New `minmax` statistics can no longer be created on the upgraded version. ---
    assert "INCORRECT_QUERY" in node.query_and_get_error(
        "CREATE TABLE t_minmax_new (id UInt64, value Int64 STATISTICS(minmax)) "
        "ENGINE = MergeTree ORDER BY id",
        settings={"allow_statistics": 1},
    )
    assert "INCORRECT_QUERY" in node.query_and_get_error(
        "ALTER TABLE t_minmax ADD STATISTICS id TYPE minmax",
        settings={"allow_statistics": 1},
    )
    assert "INCORRECT_QUERY" in node.query_and_get_error(
        "CREATE TABLE t_minmax_auto (id UInt64) ENGINE = MergeTree ORDER BY id "
        "SETTINGS auto_statistics_types = 'minmax, uniq'"
    )

    # A new table created with the current defaults uses `basic` (the replacement), not `minmax`.
    node.query("DROP TABLE IF EXISTS t_default SYNC")
    node.query(
        "CREATE TABLE t_default (id UInt64, value Int64) ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO t_default SELECT number, number FROM numbers(100)",
        settings={"materialize_statistics_on_insert": 1},
    )
    stats = node.query(
        "SELECT arrayStringConcat(arraySort(groupArrayDistinct(arrayJoin(statistics))), ',') "
        "FROM system.parts_columns "
        "WHERE database = currentDatabase() AND table = 't_default' AND active "
        "AND column = 'value'"
    ).strip()
    assert "Basic" in stats, stats
    assert "MinMax" not in stats, stats

    node.query("DROP TABLE t_minmax SYNC")
    node.query("DROP TABLE t_default SYNC")
