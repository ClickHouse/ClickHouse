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


SEVERAL_FIELDS_A_VALUES = [1, 5]
SEVERAL_FIELDS_B_VALUES = [10, 20, 30]
SEVERAL_FIELDS_UNMATCHED_B = 40


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_manifest_list_partition_pruning_with_several_fields(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_manifest_list_pruning_several_fields_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                a INT,
                b INT,
                number BIGINT
            )
            USING iceberg
            PARTITIONED BY (identity(a), identity(b))
            TBLPROPERTIES ('format-version' = '2', 'commit.manifest-merge.enabled' = 'false')
        """
    )

    for b in SEVERAL_FIELDS_B_VALUES:
        values = ", ".join(
            f"({a}, {b}, {a * 1000 + b})" for a in SEVERAL_FIELDS_A_VALUES
        )
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES {values}")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    manifests = spark.sql(
        f"SELECT partition_summaries FROM spark_catalog.default.{TABLE_NAME}.manifests"
    ).collect()
    assert len(manifests) == len(SEVERAL_FIELDS_B_VALUES)
    for manifest in manifests:
        a_summary, b_summary = manifest["partition_summaries"]
        assert a_summary["lower_bound"] != a_summary["upper_bound"]
        assert b_summary["lower_bound"] == b_summary["upper_bound"]

    creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    def select(tag, where, extra_settings=None):
        query_id = f"{TABLE_NAME}-{tag}"
        rows = instance.query(
            f"SELECT count(), sum(number) FROM {creation_expression} WHERE {where}",
            query_id=query_id,
            settings={
                "iceberg_metadata_log_level": "manifest_file_metadata",
                **(extra_settings or {}),
            },
        ).strip()
        return query_id, rows

    selected_b = SEVERAL_FIELDS_B_VALUES[-1]
    selected_a = SEVERAL_FIELDS_A_VALUES[0]

    trailing_id, trailing_rows = select("trailing", f"b = {selected_b}")
    unpruned_id, unpruned_rows = select(
        "trailing-pruning-disabled",
        f"b = {selected_b}",
        {"use_iceberg_manifest_list_partition_pruning": 0},
    )
    leading_id, leading_rows = select("leading", f"a = {selected_a}")
    unmatched_id, unmatched_rows = select(
        "unmatched", f"b = {SEVERAL_FIELDS_UNMATCHED_B}"
    )

    instance.query("SYSTEM FLUSH LOGS")

    expected_for_selected_b = "\t".join(
        [
            str(len(SEVERAL_FIELDS_A_VALUES)),
            str(sum(a * 1000 + selected_b for a in SEVERAL_FIELDS_A_VALUES)),
        ]
    )
    files_per_manifest = len(SEVERAL_FIELDS_A_VALUES)

    assert trailing_rows == expected_for_selected_b
    assert (
        profile_event(instance, trailing_id, "IcebergPartitionPrunedManifestFiles")
        == len(SEVERAL_FIELDS_B_VALUES) - 1
    )
    assert (
        profile_event(instance, trailing_id, "IcebergPartitionPrunedFiles")
        == (len(SEVERAL_FIELDS_B_VALUES) - 1) * files_per_manifest
    )
    assert count_opened_manifest_files(instance, trailing_id) == 1

    assert unpruned_rows == expected_for_selected_b
    assert (
        profile_event(instance, unpruned_id, "IcebergPartitionPrunedManifestFiles") == 0
    )
    assert count_opened_manifest_files(instance, unpruned_id) == len(
        SEVERAL_FIELDS_B_VALUES
    )

    assert leading_rows == "\t".join(
        [
            str(len(SEVERAL_FIELDS_B_VALUES)),
            str(sum(selected_a * 1000 + b for b in SEVERAL_FIELDS_B_VALUES)),
        ]
    )
    assert (
        profile_event(instance, leading_id, "IcebergPartitionPrunedManifestFiles") == 0
    )
    assert count_opened_manifest_files(instance, leading_id) == len(
        SEVERAL_FIELDS_B_VALUES
    )

    assert unmatched_rows == "0\t\\N"
    assert profile_event(
        instance, unmatched_id, "IcebergPartitionPrunedManifestFiles"
    ) == len(SEVERAL_FIELDS_B_VALUES)
    assert count_opened_manifest_files(instance, unmatched_id) == 0
