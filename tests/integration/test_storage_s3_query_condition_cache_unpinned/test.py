import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_query_condition_cache_does_not_write_for_unpinned_s3_read(started_cluster):
    # Query-condition-cache entries are keyed by the generation from LIST/HEAD. With
    # s3_validate_etag_on_read disabled, the following GET is not pinned to that generation,
    # so its marks must not be recorded under the listed generation's key.
    table_name = f"test_qcc_unpinned_{uuid.uuid4().hex}"
    bucket = started_cluster.minio_bucket
    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}.parquet"

    node.query(f"""
        CREATE TABLE {table_name} (id Int64, val String)
        ENGINE = S3('{url}', 'minio', '{minio_secret_key}', 'Parquet')
        SETTINGS output_format_parquet_row_group_size = 1
        """)
    node.query(f"""
        INSERT INTO {table_name}
        SELECT number AS id, toString(number) AS val
        FROM numbers(200)
        """)
    node.query("SYSTEM DROP QUERY CONDITION CACHE")

    result = node.query(
        f"SELECT count() FROM {table_name} WHERE id < 100",
        settings={
            "use_query_condition_cache": 1,
            "s3_validate_etag_on_read": 0,
        },
    )
    assert result.strip() == "100"
    assert int(node.query("SELECT count() FROM system.query_condition_cache")) == 0

    node.query(f"DROP TABLE {table_name}")


def test_query_condition_cache_is_not_reused_for_unpinned_s3_read(started_cluster):
    # The read-side half of the same contract: an entry written by a read pinned to the listed
    # generation (s3_validate_etag_on_read enabled) must not be applied to an unpinned read,
    # because that read's GET may return a different generation than the one the marks describe.
    table_name = f"test_qcc_unpinned_reuse_{uuid.uuid4().hex}"
    bucket = started_cluster.minio_bucket
    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}.parquet"

    node.query(f"""
        CREATE TABLE {table_name} (id Int64, val String)
        ENGINE = S3('{url}', 'minio', '{minio_secret_key}', 'Parquet')
        SETTINGS output_format_parquet_row_group_size = 1
        """)
    node.query(f"""
        INSERT INTO {table_name}
        SELECT number AS id, toString(number) AS val
        FROM numbers(200)
        """)
    node.query("SYSTEM DROP QUERY CONDITION CACHE")

    query = f"SELECT count() FROM {table_name} WHERE id < 100"

    # Seed the cache with a pinned read.
    result = node.query(
        query,
        settings={
            "use_query_condition_cache": 1,
            "s3_validate_etag_on_read": 1,
        },
    )
    assert result.strip() == "100"
    assert int(node.query("SELECT count() FROM system.query_condition_cache")) > 0

    # The pinned repeat hits the entry, which proves that it is usable at all.
    pinned_query_id = f"{table_name}_pinned"
    result = node.query(
        query,
        query_id=pinned_query_id,
        settings={
            "use_query_condition_cache": 1,
            "s3_validate_etag_on_read": 1,
        },
    )
    assert result.strip() == "100"

    # The unpinned read must not consult the entry.
    unpinned_query_id = f"{table_name}_unpinned"
    result = node.query(
        query,
        query_id=unpinned_query_id,
        settings={
            "use_query_condition_cache": 1,
            "s3_validate_etag_on_read": 0,
        },
    )
    assert result.strip() == "100"

    node.query("SYSTEM FLUSH LOGS query_log")

    def cache_hits(query_id):
        return int(
            node.query(
                f"SELECT ProfileEvents['QueryConditionCacheHits'] FROM system.query_log "
                f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
            )
        )

    assert cache_hits(pinned_query_id) > 0
    assert cache_hits(unpinned_query_id) == 0

    node.query(f"DROP TABLE {table_name}")
