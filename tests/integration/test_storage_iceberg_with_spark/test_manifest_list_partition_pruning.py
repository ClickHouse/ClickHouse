import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

NUM_PARTITIONS = 8
ROWS_PER_PARTITION = 3
SELECTED_TAG = 3


def profile_event(instance, query_id, event):
    return int(
        instance.query(
            f"""
            SELECT sum(ProfileEvents['{event}'])
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            """
        )
    )


def count_opened_manifest_files(instance, query_id):
    return int(
        instance.query(
            f"""
            SELECT uniqExact(file_path)
            FROM system.iceberg_metadata_log
            WHERE query_id = '{query_id}' AND content_type = 'ManifestFileMetadata'
            """
        )
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_manifest_list_partition_pruning(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_manifest_list_partition_pruning_" + storage_type + "_" + get_uuid_str()
    )

    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                tag INT,
                number BIGINT
            )
            USING iceberg
            PARTITIONED BY (identity(tag))
            TBLPROPERTIES ('format-version' = '2', 'commit.manifest-merge.enabled' = 'false')
        """
    )

    for tag in range(NUM_PARTITIONS):
        values = ", ".join(
            f"({tag}, {tag * 100 + i})" for i in range(ROWS_PER_PARTITION)
        )
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES {values}")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    summaries = spark.sql(
        f"SELECT partition_summaries FROM spark_catalog.default.{TABLE_NAME}.manifests"
    ).collect()
    assert len(summaries) == NUM_PARTITIONS
    for row in summaries:
        partition_summaries = row["partition_summaries"]
        assert len(partition_summaries) == 1
        assert (
            partition_summaries[0]["lower_bound"]
            == partition_summaries[0]["upper_bound"]
        )

    creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    settings = {"iceberg_metadata_log_level": "manifest_file_metadata"}

    query_id_all = f"{TABLE_NAME}-all"
    assert instance.query(
        f"SELECT sum(number) FROM {creation_expression}",
        query_id=query_id_all,
        settings=settings,
    ).strip() == str(
        sum(
            tag * 100 + i
            for tag in range(NUM_PARTITIONS)
            for i in range(ROWS_PER_PARTITION)
        )
    )

    query_id_one = f"{TABLE_NAME}-one-partition"
    assert instance.query(
        f"SELECT sum(number) FROM {creation_expression} WHERE tag = {SELECTED_TAG}",
        query_id=query_id_one,
        settings=settings,
    ).strip() == str(sum(SELECTED_TAG * 100 + i for i in range(ROWS_PER_PARTITION)))

    # Pruning is a part of partition pruning and follows its setting, so disabling that setting has
    # to bring every manifest back.
    query_id_disabled = f"{TABLE_NAME}-one-partition-pruning-disabled"
    assert instance.query(
        f"SELECT sum(number) FROM {creation_expression} WHERE tag = {SELECTED_TAG}",
        query_id=query_id_disabled,
        settings={**settings, "use_iceberg_partition_pruning": 0},
    ).strip() == str(sum(SELECTED_TAG * 100 + i for i in range(ROWS_PER_PARTITION)))

    instance.query("SYSTEM FLUSH LOGS")

    assert count_opened_manifest_files(instance, query_id_all) == NUM_PARTITIONS

    assert count_opened_manifest_files(instance, query_id_one) == 1

    assert count_opened_manifest_files(instance, query_id_disabled) == NUM_PARTITIONS

    # Every manifest here holds the single data file of its partition, so skipping the manifest skips
    # that file too and both counters have to say so.
    assert (
        profile_event(instance, query_id_one, "IcebergPartitionPrunedManifestFiles")
        == NUM_PARTITIONS - 1
    )
    assert (
        profile_event(instance, query_id_one, "IcebergPartitionPrunedFiles")
        == NUM_PARTITIONS - 1
    )

    for query_id in (query_id_all, query_id_disabled):
        assert (
            profile_event(instance, query_id, "IcebergPartitionPrunedManifestFiles") == 0
        )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_manifest_list_partition_pruning_after_type_promotion(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_manifest_list_partition_pruning_promoted_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                tag INT,
                number BIGINT
            )
            USING iceberg
            PARTITIONED BY (identity(tag))
            TBLPROPERTIES ('format-version' = '2', 'commit.manifest-merge.enabled' = 'false')
        """
    )

    for tag in range(NUM_PARTITIONS):
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({tag}, {tag * 100})")

    # The manifests above keep the bound of the partition value as an `int`, while the column is a
    # `long` from here on: a bound is stored as written and is not rewritten by a promotion.
    spark.sql(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN tag TYPE BIGINT")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({NUM_PARTITIONS}, {NUM_PARTITIONS * 100})")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert instance.query(f"SELECT sum(number) FROM {creation_expression}").strip() == str(
        sum(tag * 100 for tag in range(NUM_PARTITIONS + 1))
    )

    for tag in (SELECTED_TAG, NUM_PARTITIONS):
        assert instance.query(
            f"SELECT sum(number) FROM {creation_expression} WHERE tag = {tag}"
        ).strip() == str(tag * 100)

    assert instance.query(
        f"SELECT sum(number) FROM {creation_expression} WHERE tag >= {SELECTED_TAG}"
    ).strip() == str(sum(tag * 100 for tag in range(SELECTED_TAG, NUM_PARTITIONS + 1)))
