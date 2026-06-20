import pytest

from helpers.iceberg_utils import (
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_query_condition_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_query_condition_cache_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, val STRING)
        USING iceberg
        OPTIONS('format-version'='2')
        """
    )

    for i in range(1, 10):
        execute_spark_query(
            f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'value_{i}')"
        )

    instance.query(
        get_creation_expression(
            storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=False
        )
    )
    creation_expression = TABLE_NAME

    instance.query("SYSTEM DROP QUERY CONDITION CACHE")

    filter_condition = "WHERE id % 7 = 0"
    select_query = f"SELECT * FROM {creation_expression} {filter_condition} ORDER BY id"
    settings = {
        "use_query_condition_cache": 1,
        "allow_experimental_analyzer": 1,
    }

    query_id_first = f"{TABLE_NAME}_first"
    result_first = instance.query(select_query, query_id=query_id_first, settings=settings)

    instance.query("SYSTEM FLUSH LOGS")

    misses_first = int(
        instance.query(
            f"SELECT ProfileEvents['QueryConditionCacheMisses'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_first}' AND type = 'QueryFinish'"
        )
    )
    hits_first = int(
        instance.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_first}' AND type = 'QueryFinish'"
        )
    )
    assert misses_first > 0, f"Expected cache misses on first run, got {misses_first}"
    assert hits_first == 0, f"Expected no cache hits on first run, got {hits_first}"

    query_id_second = f"{TABLE_NAME}_second"
    result_second = instance.query(select_query, query_id=query_id_second, settings=settings)
    assert result_second == result_first

    instance.query("SYSTEM FLUSH LOGS")

    hits_second = int(
        instance.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_second}' AND type = 'QueryFinish'"
        )
    )
    assert hits_second > 0

    instance.query("SYSTEM DROP QUERY CONDITION CACHE")

    query_id_after_drop = f"{TABLE_NAME}_after_drop"
    instance.query(select_query, query_id=query_id_after_drop, settings=settings)

    instance.query("SYSTEM FLUSH LOGS")

    misses_after_drop = int(
        instance.query(
            f"SELECT ProfileEvents['QueryConditionCacheMisses'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_after_drop}' AND type = 'QueryFinish'"
        )
    )
    hits_after_drop = int(
        instance.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_after_drop}' AND type = 'QueryFinish'"
        )
    )
    assert misses_after_drop > 0
    assert hits_after_drop == 0

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize(
    "extra_settings",
    [
        {"allow_suspicious_low_cardinality_types": 1},
        {"allow_experimental_funnel_functions": 1},
        {"allow_experimental_database_iceberg": 1},
    ],
    ids=["allow_suspicious", "experimental_tier", "beta_tier"],
)
def test_query_condition_cache_not_writable_with_relaxed_settings(
    started_cluster_iceberg_with_spark, storage_type, extra_settings
):
    """
    Regression test for issue #104203 — object-storage QCC write paths must not
    populate the cache when the query context has non-default `allow_suspicious_*`,
    `relaxed`, or non-`Production` `allow_experimental_*` settings. Such contexts
    can change `WHERE` evaluation outcomes (type coercion, constant folding, mark
    filtering) without changing the action-graph hash that keys the cache. A
    "no rows match" verdict produced under such a profile must not be served to a
    query running with default settings — otherwise it would silently return the
    wrong rows. Reads from the same context are still served.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_qcc_not_writable_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, val STRING)
        USING iceberg
        OPTIONS('format-version'='2')
        """
    )

    for i in range(1, 10):
        execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'value_{i}')")

    instance.query(
        get_creation_expression(
            storage_type,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            table_function=False,
        )
    )

    instance.query("SYSTEM DROP QUERY CONDITION CACHE")

    select_query = f"SELECT * FROM {TABLE_NAME} WHERE id % 7 = 0 FORMAT Null"
    settings = {
        "use_query_condition_cache": 1,
        "allow_experimental_analyzer": 1,
        **extra_settings,
    }

    instance.query(select_query, settings=settings)

    cache_size = int(instance.query("SELECT count() FROM system.query_condition_cache"))
    assert cache_size == 0, (
        f"Expected QCC to remain empty for relaxed/experimental context "
        f"({extra_settings}), got {cache_size} entries — the writability gate "
        f"in FormatFilterInfo / StorageObjectStorageSource is missing or wrong."
    )

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_query_condition_cache_nondeterministic_functions(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_qcc_nondeterministic_" + storage_type + "_" + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, val STRING)
        USING iceberg
        OPTIONS('format-version'='2')
        """
    )

    for i in range(1, 10):
        execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'value_{i}')")

    instance.query(
        get_creation_expression(
            storage_type,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            table_function=False,
        )
    )

    instance.query("SYSTEM DROP QUERY CONDITION CACHE")

    select_query = f"SELECT * FROM {TABLE_NAME} WHERE id = rand() FORMAT Null"
    settings = {
        "use_query_condition_cache": 1,
        "allow_experimental_analyzer": 1,
    }

    instance.query(select_query, settings=settings)

    cache_size = int(instance.query("SELECT count() FROM system.query_condition_cache"))
    assert cache_size == 0

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
