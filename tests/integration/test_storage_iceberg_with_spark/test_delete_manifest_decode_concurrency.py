from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
)

ROW_COUNT = 30
# One delete manifest per `DELETE`, see `write_table`.
DELETE_COUNT = 6

# Must match the delay the `iceberg_slow_manifest_read` failpoint injects.
SLEEP_PER_MANIFEST_SECONDS = 0.40

STORAGE_TYPE = "local"


def get_array(query_result: str):
    return sorted([int(x) for x in query_result.strip().split("\n")])


def write_table(started_cluster, table_name: str):
    spark = started_cluster.spark_session

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint, part int, data string) USING iceberg
        PARTITIONED BY (part)
        TBLPROPERTIES (
            'format-version' = '2',
            'commit.manifest-merge.enabled' = 'false',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    # `id % DELETE_COUNT` puts ids 0 .. DELETE_COUNT-1 in separate partitions, each keeping other rows
    # so that deleting one is a row-level delete rather than a metadata-only drop of the data file.
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, CAST(id % {DELETE_COUNT} AS int), char(id + ascii('a')) FROM range(0, {ROW_COUNT})
        """
    )
    for row_id in range(DELETE_COUNT):
        spark.sql(f"DELETE FROM {table_name} WHERE id = {row_id}")

    default_upload_directory(
        started_cluster,
        STORAGE_TYPE,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def check_delete_manifests(instance, table_expression: str, extra_settings=None) -> None:
    settings = {"iceberg_metadata_log_level": "manifest_list_entry"}
    settings.update(extra_settings or {})

    query_id = "check_delete_manifests_" + get_uuid_str()
    instance.query(
        f"SELECT id FROM {table_expression} FORMAT Null", query_id=query_id, settings=settings
    )
    instance.query("SYSTEM FLUSH LOGS iceberg_metadata_log")
    # Delete manifests have content = 1. (Data manifests have content = 0).
    count = instance.query(
        f"""
        SELECT uniqExact(JSONExtractString(content, 'manifest_path'))
        FROM system.iceberg_metadata_log
        WHERE query_id = '{query_id}'
          AND content_type = 'ManifestListEntry'
          AND JSONExtractInt(content, 'content') = 1
        """
    )
    count = int(count.strip())
    assert count == DELETE_COUNT

def elapsed(instance, query, **kwargs) -> float:
    query_id = get_uuid_str()
    instance.query(query, query_id=query_id, **kwargs)
    instance.query("SYSTEM FLUSH LOGS query_log")
    duration = instance.query(
        f"""SELECT query_duration_ms / 1000.0 FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id = '{query_id}' LIMIT 1"""
    )
    return float(duration.strip())


def test_delete_manifest_decode_concurrency(started_cluster_iceberg_with_spark):
    """The result must not depend on `iceberg_delete_manifest_decode_concurrency`."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_delete_manifest_decode_concurrency_" + get_uuid_str()

    write_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )
    check_delete_manifests(instance, TABLE_NAME)

    expected = list(range(DELETE_COUNT, ROW_COUNT))
    for concurrency in [1, 2, 4, 16]:
        assert (
            get_array(
                instance.query(
                    f"SELECT id FROM {TABLE_NAME} ORDER BY id",
                    settings={
                        "iceberg_delete_manifest_decode_concurrency": concurrency
                    },
                )
            )
            == expected
        ), f"wrong result with iceberg_delete_manifest_decode_concurrency={concurrency}"

    instance.query(f"DROP TABLE {TABLE_NAME}")

