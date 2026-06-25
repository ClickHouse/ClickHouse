import logging

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

NUMBER_OF_MANIFESTS = 5
MAX_SELECT_SECONDS = 60.0
ROWS_PER_MANIFEST = 100

def elapsed(node, query, **kwargs):
    query_id = get_uuid_str()
    node.query(query, query_id=query_id, **kwargs)
    node.query("SYSTEM FLUSH LOGS query_log")
    duration_result = node.query(
        f"""SELECT query_duration_ms / 1000.0 as duration FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id = '{query_id}' LIMIT 1""")
    return float(duration_result.strip())


def test_manifest_read_performance(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_manifest_read_performance_" + get_uuid_str()

    # Create Iceberg table. Properties ensure (a) manifests don't get merged and
    # (b) old metadata.json files get deleted.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id BIGINT)
        USING iceberg
        partitioned by (id)
        TBLPROPERTIES ('commit.manifest-merge.enabled' = 'false',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10')""")

    # Each commit writes FILES_PER_MANIFEST data files in a single append, producing one
    # manifest with that many entries, to create a manifest-heavy table.
    for i in range(NUMBER_OF_MANIFESTS):
        spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range({ROWS_PER_MANIFEST});")
        logging.info("Inserted %s/%s commits", i + 1, NUMBER_OF_MANIFESTS)

    # Copy files Spark created to minio.
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "s3",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Generate icebergS3(...) expression.
    table_function_expr = get_creation_expression("s3", TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True)

    # Read from table. Simulate realistic manifest read latency.
    instance.query("SYSTEM ENABLE FAILPOINT iceberg_slow_manifest_read")
    try:
        duration = elapsed(
            instance,
            f"SELECT * FROM {table_function_expr} SETTINGS use_iceberg_metadata_files_cache = 0 FORMAT Null",
        )
    finally:
        instance.query("SYSTEM DISABLE FAILPOINT iceberg_slow_manifest_read")

    logging.info("SELECT * over %s manifests took %.3f seconds", NUMBER_OF_MANIFESTS, duration)

    assert duration < MAX_SELECT_SECONDS, (
        f"SELECT * over a table with ~{NUMBER_OF_MANIFESTS} manifests took "
        f"{duration:.3f}s, exceeding the {MAX_SELECT_SECONDS}s bound"
    )
