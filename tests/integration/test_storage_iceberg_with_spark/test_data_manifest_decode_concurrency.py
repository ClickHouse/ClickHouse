from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
)

# One data manifest per `INSERT`, see `write_table`.
DATA_MANIFEST_COUNT = 8
ROWS_PER_INSERT = 4
ROW_COUNT = DATA_MANIFEST_COUNT * ROWS_PER_INSERT

STORAGE_TYPE = "local"


def get_array(query_result: str):
    return sorted([int(x) for x in query_result.strip().split("\n")])


def write_table(
    started_cluster, table_name: str, merge_on_read: bool = False, format_version: int = 2
):
    spark = started_cluster.spark_session

    extra_properties = ""
    if merge_on_read:
        extra_properties = """,
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'"""
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint, part int, data string) USING iceberg
        PARTITIONED BY (part)
        TBLPROPERTIES (
            'format-version' = '{format_version}',
            'commit.manifest-merge.enabled' = 'false'{extra_properties}
        )
        """
    )
    # One commit per partition so that every `INSERT` produces its own data manifest.
    for part in range(DATA_MANIFEST_COUNT):
        spark.sql(
            f"""
            INSERT INTO {table_name}
            SELECT id, {part}, char(id % 26 + ascii('a'))
            FROM range({part * ROWS_PER_INSERT}, {(part + 1) * ROWS_PER_INSERT})
            """
        )

    default_upload_directory(
        started_cluster,
        STORAGE_TYPE,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def check_data_manifests(instance, table_expression: str) -> None:
    settings = {"iceberg_metadata_log_level": "manifest_list_entry"}

    query_id = "check_data_manifests_" + get_uuid_str()
    instance.query(
        f"SELECT id FROM {table_expression} FORMAT Null", query_id=query_id, settings=settings
    )
    instance.query("SYSTEM FLUSH LOGS iceberg_metadata_log")
    # Data manifests have content = 0. (Delete manifests have content = 1).
    count = instance.query(
        f"""
        SELECT uniqExact(JSONExtractString(content, 'manifest_path'))
        FROM system.iceberg_metadata_log
        WHERE query_id = '{query_id}'
          AND content_type = 'ManifestListEntry'
          AND JSONExtractInt(content, 'content') = 0
        """
    )
    count = int(count.strip())
    assert count == DATA_MANIFEST_COUNT


def test_data_manifest_decode_concurrency(started_cluster_iceberg_with_spark):
    """The result must not depend on how many threads read the table: one thread parses
    the manifests in order, several threads parse different manifests concurrently."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_data_manifest_decode_concurrency_" + get_uuid_str()

    write_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )
    check_data_manifests(instance, TABLE_NAME)

    expected = list(range(ROW_COUNT))
    for max_threads in [1, 16]:
        assert (
            get_array(
                instance.query(
                    f"SELECT id FROM {TABLE_NAME} ORDER BY id",
                    settings={"max_threads": max_threads},
                )
            )
            == expected
        ), f"wrong result with max_threads={max_threads}"

    # A filter on the partition column makes every thread parsing a manifest evaluate the same
    # shared filter DAG (with its lazily materialized IN set) while pruning manifest entries.
    filtered_parts = [1, 3, 6]
    expected_filtered = sorted(
        row_id
        for row_id in range(ROW_COUNT)
        if row_id // ROWS_PER_INSERT in filtered_parts
    )
    for max_threads in [1, 16]:
        result = get_array(
            instance.query(
                f"SELECT id FROM {TABLE_NAME} WHERE part IN (1, 3, 6) ORDER BY id",
                settings={"max_threads": max_threads},
            )
        )
        assert (
            result == expected_filtered
        ), f"wrong filtered result with max_threads={max_threads}"

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_data_and_delete_manifest_decode_concurrency(
    started_cluster_iceberg_with_spark,
):
    """Data and delete manifests are parsed concurrently and share one filter DAG;
    the result must not depend on how many threads read the table."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_data_and_delete_manifest_decode_concurrency_" + get_uuid_str()

    write_table(started_cluster_iceberg_with_spark, TABLE_NAME, merge_on_read=True)
    spark = started_cluster_iceberg_with_spark.spark_session
    # One delete manifest per `DELETE`; ids 4, 13 and 25 fall inside the filtered
    # partitions below, id 0 outside them.
    deleted_ids = [0, 4, 13, 25]
    for row_id in deleted_ids:
        spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = {row_id}")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        STORAGE_TYPE,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    filtered_parts = [1, 3, 6]
    expected = sorted(
        row_id
        for row_id in range(ROW_COUNT)
        if row_id // ROWS_PER_INSERT in filtered_parts and row_id not in deleted_ids
    )
    for max_threads in [1, 16]:
        result = get_array(
            instance.query(
                f"SELECT id FROM {TABLE_NAME} WHERE part IN (1, 3, 6) ORDER BY id",
                settings={"max_threads": max_threads},
            )
        )
        assert result == expected, f"wrong result with max_threads={max_threads}"

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_data_manifest_decode_concurrency_row_lineage(started_cluster_iceberg_with_spark):
    """Format-version 3 manifests carry an inherited `first_row_id`, which the manifest
    iterator materializes per entry before yielding anything; the row ids and the result
    must not depend on how many threads read the table."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_data_manifest_decode_concurrency_row_lineage_" + get_uuid_str()

    write_table(started_cluster_iceberg_with_spark, TABLE_NAME, format_version=3)
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    serial = instance.query(
        f"SELECT id, _row_id FROM {TABLE_NAME} ORDER BY id",
        settings={
            "max_threads": 1,
            "use_iceberg_metadata_files_cache": 0,
        },
    )
    assert serial.strip(), "the serial read returned no rows; fixture broken"
    result = instance.query(
        f"SELECT id, _row_id FROM {TABLE_NAME} ORDER BY id",
        settings={
            "max_threads": 16,
            "use_iceberg_metadata_files_cache": 0,
        },
    )
    assert result == serial, "row lineage changed with max_threads=16"

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_data_manifest_decode_large_manifest(started_cluster_iceberg_with_spark):
    """A single manifest with 300 entries, read by one thread or by several threads
    taking turns on it; the result must not depend on how many threads read the table."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_data_manifest_decode_large_manifest_" + get_uuid_str()

    entry_count = 300
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint) USING iceberg
        PARTITIONED BY (id)
        TBLPROPERTIES ('commit.manifest-merge.enabled' = 'false')
        """
    )
    # One commit writes one data file per partition value, producing a single
    # manifest with `entry_count` entries.
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range({entry_count})")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        STORAGE_TYPE,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    expected = list(range(entry_count))
    for max_threads in [1, 16]:
        result = get_array(
            instance.query(
                f"SELECT id FROM {TABLE_NAME} ORDER BY id",
                settings={
                    "max_threads": max_threads,
                    "use_iceberg_metadata_files_cache": 0,
                },
            )
        )
        assert result == expected, f"wrong result with max_threads={max_threads}"

    # A filter pruning every entry keeps the row loop inside `ManifestFileIterator::next`
    # busy for the whole manifest without yielding.
    for max_threads in [1, 16]:
        count = instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE id < 0",
            settings={
                "max_threads": max_threads,
                "use_iceberg_metadata_files_cache": 0,
            },
        )
        assert (
            int(count.strip()) == 0
        ), f"fully pruned read returned rows with max_threads={max_threads}"

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_data_manifest_decode_concurrency_subquery_filter(
    started_cluster_iceberg_with_spark,
):
    """`part IN (SELECT ...)` is backed by `FutureSetFromSubquery`, which the threads
    parsing manifests concurrently use through the shared filter DAG; the result must not
    depend on how many threads read the table. (Literal tuple `IN` goes through
    `FutureSetFromTuple` instead and is covered above.)"""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_data_manifest_decode_concurrency_subquery_" + get_uuid_str()

    write_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    create_iceberg_table(
        STORAGE_TYPE, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    # The subquery yields parts {1, 3, 5}.
    filtered_parts = [1, 3, 5]
    expected = sorted(
        row_id
        for row_id in range(ROW_COUNT)
        if row_id // ROWS_PER_INSERT in filtered_parts
    )
    for max_threads in [1, 16]:
        for _ in range(2):
            result = get_array(
                instance.query(
                    f"SELECT id FROM {TABLE_NAME} "
                    "WHERE part IN (SELECT toInt32(number * 2 + 1) FROM numbers(3)) "
                    "ORDER BY id",
                    settings={
                        "max_threads": max_threads,
                        "use_iceberg_metadata_files_cache": 0,
                    },
                )
            )
            assert result == expected, (
                f"wrong subquery-filtered result with max_threads={max_threads}"
            )

    instance.query(f"DROP TABLE {TABLE_NAME}")

