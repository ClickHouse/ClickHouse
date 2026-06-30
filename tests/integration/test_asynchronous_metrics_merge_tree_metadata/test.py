import time

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


METRICS_TO_INCREASE = [
    "TotalMergeTreeMetadataBytesInMemoryAllocated",
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_metric(metric):
    return int(
        node.query(
            f"SELECT value FROM system.asynchronous_metrics WHERE metric = '{metric}'"
        ).strip()
    )


def get_metrics(metrics):
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    return {metric: get_metric(metric) for metric in metrics}


def wait_until_metrics_increase(baseline, metrics, retries=30, delay=0.5):
    for _ in range(retries):
        current = get_metrics(metrics)
        if all(current[metric] > baseline[metric] for metric in metrics):
            return current
        time.sleep(delay)
    return get_metrics(metrics)


def wait_until_metrics_drop(post_insert, metrics, retries=30, delay=0.5):
    for _ in range(retries):
        current = get_metrics(metrics)
        if all(current[metric] < post_insert[metric] for metric in metrics):
            return current
        time.sleep(delay)
    return get_metrics(metrics)


def test_merge_tree_metadata_async_metrics(started_cluster):
    node.query("DROP TABLE IF EXISTS test_metadata_memory SYNC")

    column_defs = []
    insert_exprs = []
    projection_columns = ["id"]
    for i in range(48):
        column_defs.append(
            f"c{i} Tuple(a String, b Array(Nullable(UInt32)), c Map(String, UInt64))"
        )
        insert_exprs.append(
            "tuple(concat('value_', toString(number)), "
            "[toNullable(toUInt32(number))], map('k', toUInt64(number)))"
        )
        if i < 8:
            projection_columns.append(f"c{i}")

    node.query(
        f"""
        CREATE TABLE test_metadata_memory
        (
            id UInt64,
            {", ".join(column_defs)},
            PROJECTION p (SELECT {", ".join(projection_columns)} ORDER BY id)
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            ratio_of_defaults_for_sparse_serialization = 0.5,
            write_marks_for_substreams_in_compact_parts = 1
        """
    )

    baseline = get_metrics(METRICS_TO_INCREASE)

    node.query(
        f"""
        INSERT INTO test_metadata_memory
        SELECT
            number,
            {", ".join(insert_exprs)}
        FROM numbers(10000)
        """
    )

    assert (
        int(
            node.query(
                """
                SELECT count()
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table = 'test_metadata_memory'
                  AND active
                """
            ).strip()
        )
        > 0
    )
    assert (
        int(
            node.query(
                """
                SELECT count()
                FROM system.projection_parts
                WHERE database = currentDatabase()
                  AND table = 'test_metadata_memory'
                  AND active
                """
            ).strip()
        )
        > 0
    )

    post_insert = wait_until_metrics_increase(baseline, METRICS_TO_INCREASE)
    for metric in METRICS_TO_INCREASE:
        assert post_insert[metric] > baseline[metric], metric

    node.query("TRUNCATE TABLE test_metadata_memory")
    post_truncate = wait_until_metrics_drop(post_insert, METRICS_TO_INCREASE)
    for metric in METRICS_TO_INCREASE:
        assert post_truncate[metric] < post_insert[metric], metric

    node.query("DROP TABLE IF EXISTS test_metadata_memory SYNC")
