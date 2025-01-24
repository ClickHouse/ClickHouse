import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# This test is bad and it should be a functional test but S3 metrics
# are accounted  incorrectly for merges in part_log and query_log.
# Also we have text_log with level 'trace' in functional tests
# but this test requeires text_log with level 'test'.


@pytest.mark.parametrize("min_bytes_for_wide_part", [0, 1000000000])
def test_merge_load_marks(started_cluster, min_bytes_for_wide_part):
    node.query(
        f"""
        DROP TABLE IF EXISTS t_load_marks;

        CREATE TABLE t_load_marks (a UInt64, b UInt64)
        ENGINE = MergeTree ORDER BY a
        SETTINGS min_bytes_for_wide_part = {min_bytes_for_wide_part};

        INSERT INTO t_load_marks SELECT number, number FROM numbers(1000);
        INSERT INTO t_load_marks SELECT number, number FROM numbers(1000);

        OPTIMIZE TABLE t_load_marks FINAL;
        SYSTEM FLUSH LOGS;
    """
    )

    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE table = 't_load_marks'"
    ).strip()

    result = node.query(
        f"""
        SELECT count()
        FROM system.text_log
        WHERE (query_id LIKE '%{uuid}::all_1_2_1%') AND (message LIKE '%Loading marks%')
    """
    ).strip()

    result = int(result)

    is_wide = min_bytes_for_wide_part == 0
    not_loaded = result == 0

    assert is_wide == not_loaded
