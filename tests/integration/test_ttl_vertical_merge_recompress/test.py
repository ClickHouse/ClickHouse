import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/part_log.xml"])

TABLE = "t_ttl_vertical_recompress"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_vertical_ttl_merge_keeps_recompress_input(started_cluster):
    """A vertical TTL merge must still rebuild the RECOMPRESS TTL.

    The TTL step of a vertical merge runs on the horizontal stream, which used to
    carry only the rows-TTL inputs, so rebuilding the RECOMPRESS TTL failed with
    NOT_FOUND_COLUMN_IN_BLOCK on its `d2` input. The stateless version of this case
    cannot pin the merge to the vertical path across CI build flavors, and the
    horizontal path always carried `d2`, so it passes without exercising anything.
    Here the server config is fixed and the algorithm is asserted.
    """
    node.query(f"DROP TABLE IF EXISTS {TABLE} SYNC")
    node.query(
        f"""
        CREATE TABLE {TABLE}
        (
            id UInt64,
            event_time DateTime,
            d2 DateTime,
            pad String
        )
        ENGINE = MergeTree()
        ORDER BY id
        TTL event_time + INTERVAL 1 SECOND, d2 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3))
        SETTINGS
            enable_vertical_merge_algorithm = 1,
            vertical_merge_optimize_ttl_delete = 1,
            vertical_merge_algorithm_min_rows_to_activate = 1,
            vertical_merge_algorithm_min_columns_to_activate = 1,
            vertical_merge_algorithm_min_bytes_to_activate = 0,
            allow_vertical_merges_from_compact_to_wide_parts = 1,
            min_bytes_for_wide_part = 0,
            min_rows_for_wide_part = 0
        """
    )

    # Two parts, each half expired by the rows TTL and none by the RECOMPRESS one.
    node.query(f"SYSTEM STOP MERGES {TABLE}")
    node.query(
        f"""
        INSERT INTO {TABLE}
        SELECT number,
               if(number % 2 = 0, now() - INTERVAL 1 HOUR, now() + INTERVAL 10 HOUR),
               now(),
               repeat('x', 100)
        FROM numbers(1000)
        """
    )
    node.query(
        f"""
        INSERT INTO {TABLE}
        SELECT number + 1000,
               if(number % 2 = 0, now() - INTERVAL 1 HOUR, now() + INTERVAL 10 HOUR),
               now(),
               repeat('y', 100)
        FROM numbers(1000)
        """
    )
    node.query(f"SYSTEM START MERGES {TABLE}")

    # On an unfixed server this raises NOT_FOUND_COLUMN_IN_BLOCK ("Not found column
    # or subcolumn d2 in block") instead of completing the merge.
    node.query(
        f"OPTIMIZE TABLE {TABLE} FINAL", settings={"optimize_throw_if_noop": "1"}
    )

    # Guard against the case degrading into the horizontal path, which would satisfy
    # everything below without touching the code under test. `error = 0` matters: a
    # merge that aborts still logs its algorithm, so without it a failing build could
    # satisfy this too.
    node.query("SYSTEM FLUSH LOGS")
    vertical_merges = node.query(
        f"""
        SELECT countIf(merge_algorithm = 'Vertical' AND error = 0)
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = '{TABLE}'
          AND event_type = 'MergeParts'
          AND length(merged_from) > 1
        """
    ).strip()
    assert vertical_merges != "0", "no vertical merge of several parts completed"

    assert node.query(f"SELECT count() FROM {TABLE}").strip() == "1000"

    # The RECOMPRESS TTL is rebuilt from the merged data, so its deadline is still
    # in the future rather than dropped or left stale.
    recompress_pending = node.query(
        f"""
        SELECT recompression_ttl_info.max[1] > now()
        FROM system.parts
        WHERE database = currentDatabase() AND table = '{TABLE}' AND active
        """
    ).strip()
    assert recompress_pending == "1"

    node.query(f"DROP TABLE {TABLE} SYNC")
