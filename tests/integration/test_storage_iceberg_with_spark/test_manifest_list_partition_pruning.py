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


# `a` takes two values and `b` one per commit, so every manifest below spans a range of `a` while
# holding a single `b`.
MULTI_FIELD_A_VALUES = [1, 5]
MULTI_FIELD_B_VALUES = [10, 20, 30]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_manifest_list_partition_pruning_with_several_fields(
    started_cluster_iceberg_with_spark, storage_type
):
    """A manifest list summarises every partition field on its own, so the bounds of one manifest
    are a hyperrectangle and not a range of the partition key tuple. Reading them as a range of the
    tuple loses every field after the first one that is not a single point: the tuples between
    `(1, 10)` and `(5, 10)` in lexicographic order include `(2, 999)`, so nothing follows from the
    bounds of `b`. Each manifest here spans `a` and pins `b`, so a filter on `b` alone has to skip
    whole manifests."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_manifest_list_pruning_several_fields_" + storage_type + "_" + get_uuid_str()
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

    # One commit per `b`, holding one data file per `a`, so one manifest holds both partitions.
    for b in MULTI_FIELD_B_VALUES:
        values = ", ".join(f"({a}, {b}, {a * 1000 + b})" for a in MULTI_FIELD_A_VALUES)
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES {values}")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # The premise of the test: `a` is a range in every manifest and `b` is a point. Without this the
    # two readings of the bounds coincide and the test proves nothing.
    summaries = spark.sql(
        f"SELECT partition_summaries FROM spark_catalog.default.{TABLE_NAME}.manifests"
    ).collect()
    assert len(summaries) == len(MULTI_FIELD_B_VALUES)
    for row in summaries:
        partition_summaries = row["partition_summaries"]
        assert len(partition_summaries) == 2
        assert partition_summaries[0]["lower_bound"] != partition_summaries[0]["upper_bound"]
        assert partition_summaries[1]["lower_bound"] == partition_summaries[1]["upper_bound"]

    creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )
    settings = {"iceberg_metadata_log_level": "manifest_file_metadata"}

    def run(tag, where, settings_override=None):
        query_id = f"{TABLE_NAME}-{tag}"
        result = instance.query(
            f"SELECT count(), sum(number) FROM {creation_expression} WHERE {where}",
            query_id=query_id,
            settings={**settings, **(settings_override or {})},
        ).strip()
        return query_id, result

    selected_b = MULTI_FIELD_B_VALUES[-1]
    expected_for_selected_b = "\t".join(
        [
            str(len(MULTI_FIELD_A_VALUES)),
            str(sum(a * 1000 + selected_b for a in MULTI_FIELD_A_VALUES)),
        ]
    )

    # A filter on the trailing field only: the two manifests whose `b` differs cannot match.
    trailing_id, trailing_result = run("trailing", f"b = {selected_b}")
    assert trailing_result == expected_for_selected_b

    # The same filter with manifest-list pruning off has to return the same rows.
    disabled_id, disabled_result = run(
        "trailing-disabled",
        f"b = {selected_b}",
        {"use_iceberg_manifest_list_partition_pruning": 0},
    )
    assert disabled_result == expected_for_selected_b

    # A filter on the leading field only: every manifest spans it, so none can be skipped.
    leading_id, leading_result = run("leading", f"a = {MULTI_FIELD_A_VALUES[0]}")
    assert leading_result == "\t".join(
        [
            str(len(MULTI_FIELD_B_VALUES)),
            str(sum(MULTI_FIELD_A_VALUES[0] * 1000 + b for b in MULTI_FIELD_B_VALUES)),
        ]
    )

    # A filter no manifest can match.
    none_id, none_result = run("none", "b = 40")
    assert none_result == "0\t0"

    instance.query("SYSTEM FLUSH LOGS")

    # Each manifest holds one data file per `a`, so a skipped manifest skips that many files.
    files_per_manifest = len(MULTI_FIELD_A_VALUES)

    assert (
        profile_event(instance, trailing_id, "IcebergPartitionPrunedManifestFiles")
        == len(MULTI_FIELD_B_VALUES) - 1
    )
    assert (
        profile_event(instance, trailing_id, "IcebergPartitionPrunedFiles")
        == (len(MULTI_FIELD_B_VALUES) - 1) * files_per_manifest
    )
    assert count_opened_manifest_files(instance, trailing_id) == 1

    assert (
        profile_event(instance, none_id, "IcebergPartitionPrunedManifestFiles")
        == len(MULTI_FIELD_B_VALUES)
    )
    assert count_opened_manifest_files(instance, none_id) == 0

    # The leading field spans its bounds in every manifest, so it rules nothing out. The per-entry
    # pruner still drops the data files of the other `a`.
    assert profile_event(instance, leading_id, "IcebergPartitionPrunedManifestFiles") == 0
    assert count_opened_manifest_files(instance, leading_id) == len(MULTI_FIELD_B_VALUES)

    assert (
        profile_event(instance, disabled_id, "IcebergPartitionPrunedManifestFiles") == 0
    )
    assert count_opened_manifest_files(instance, disabled_id) == len(MULTI_FIELD_B_VALUES)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_manifest_list_partition_pruning_after_dropped_partition_field(
    started_cluster_iceberg_with_spark, storage_type
):
    """Dropping a partition field leaves it in the spec as a `void` transform, and its summaries
    carry no bounds from then on. Read as a range of the key tuple, a leading field with no bounds
    leaves every field after it unconstrained, so one dropped field used to disable manifest-list
    pruning for the whole spec. Per field it only disables itself."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_manifest_list_pruning_dropped_field_" + storage_type + "_" + get_uuid_str()
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
    # `a` is dropped from the spec, so it becomes a `void` field: every later manifest reports it as
    # all-null with no bounds, while `b` keeps its own.
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP PARTITION FIELD a")

    for b in MULTI_FIELD_B_VALUES:
        values = ", ".join(f"({a}, {b}, {a * 1000 + b})" for a in MULTI_FIELD_A_VALUES)
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES {values}")

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

    selected_b = MULTI_FIELD_B_VALUES[-1]
    query_id = f"{TABLE_NAME}-void-leading-field"
    assert instance.query(
        f"SELECT count(), sum(number) FROM {creation_expression} WHERE b = {selected_b}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_metadata"},
    ).strip() == "\t".join(
        [
            str(len(MULTI_FIELD_A_VALUES)),
            str(sum(a * 1000 + selected_b for a in MULTI_FIELD_A_VALUES)),
        ]
    )

    instance.query("SYSTEM FLUSH LOGS")
    assert (
        profile_event(instance, query_id, "IcebergPartitionPrunedManifestFiles")
        == len(MULTI_FIELD_B_VALUES) - 1
    )
