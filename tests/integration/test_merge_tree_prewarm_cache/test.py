import pytest
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/primary_key_cache.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_merge_tree_prewarm_index_cache(started_cluster):
    node.query(
        """
        SYSTEM DROP PRIMARY INDEX CACHE;

        DROP TABLE IF EXISTS t_prewarm_index_cache;

        CREATE TABLE t_prewarm_index_cache (d Date, u UInt64)
        ENGINE = MergeTree ORDER BY u PARTITION BY d SETTINGS index_granularity = 1, use_primary_key_cache = 1;

        SYSTEM STOP MERGES t_prewarm_index_cache;

        -- 20220101_1_1_0
        INSERT INTO t_prewarm_index_cache SELECT toDate('2022-01-01'), 1 FROM numbers(50);
        -- 20220201_2_2_0
        INSERT INTO t_prewarm_index_cache SELECT toDate('2022-02-01'), 2 FROM numbers(50);
        -- 20220201_3_3_0
        INSERT INTO t_prewarm_index_cache SELECT toDate('2022-02-01'), 3 FROM numbers(50);
        -- 20220101_4_4_0
        INSERT INTO t_prewarm_index_cache SELECT toDate('2022-01-01'), 4 FROM numbers(50);
    """
    )

    node.query(
        "SYSTEM PREWARM PRIMARY INDEX CACHE t_prewarm_index_cache",
        settings={"max_threads": 1},
    )

    assert (
        node.query(
            "SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE table = 't_prewarm_index_cache'"
        )
        == "0\n"
    )

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    assert (
        node.query(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'PrimaryIndexCacheFiles'"
        )
        == "2\n"
    )

    log_comment = str(uuid.uuid4())

    node.query(
        """
        SELECT count() FROM t_prewarm_index_cache WHERE _part = '20220201_2_2_0' AND u = 2;
        SELECT count() FROM t_prewarm_index_cache WHERE _part = '20220201_3_3_0' AND u = 3;

        SELECT count() FROM t_prewarm_index_cache WHERE _part = '20220101_1_1_0' AND u = 1;
        SELECT count() FROM t_prewarm_index_cache WHERE _part = '20220101_4_4_0' AND u = 4;
    """,
        settings={"log_comment": log_comment},
    )

    node.query("SYSTEM FLUSH LOGS")

    # The cache is configured to fit only two entries for partition 20220201.
    # Here we check that we correctly stop prewarming after the cache is considered full.

    expected = """
        20220201_2_2_0\t0\n
        20220201_3_3_0\t0\n
        20220101_1_1_0\t1\n
        20220101_4_4_0\t1\n
    """

    result = node.query(
        f"""
        SELECT extract(query, '\d{{8}}_\d+_\d+_\d+') AS part, ProfileEvents['LoadedPrimaryIndexFiles'] FROM system.query_log
        WHERE type = 'QueryFinish' AND Settings['log_comment'] = '{log_comment}'
        ORDER BY event_time_microseconds
    """
    )

    assert TSV(result) == TSV(expected)
