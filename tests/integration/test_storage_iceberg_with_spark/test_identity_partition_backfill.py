import glob

import pyarrow.parquet as pq
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


# Spark writes the identity-partition source column INTO the Parquet data files, so to reproduce the
# "column absent from the data files" layout (Hive-partitioned / Fabric-virtualized Iceberg, issue
# #110216) we rewrite every data file dropping that column. Its value then lives only in the manifest
# partition tuple, exactly as with externally written tables. Without this rewrite the reader could
# read the column straight from the file and the test would pass even without the backfill.
def _strip_column_from_data_files(cluster, storage_type, table_name, column):
    root = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(cluster, storage_type, root, root)
    data_files = glob.glob(f"{root}data/**/*.parquet", recursive=True)
    assert data_files, f"no data files under {root}data/"
    for pq_file in data_files:
        # Read the file by its own footer. pq.read_table(path) uses the dataset API, which infers a
        # Hive partition from the region=<v>/ path and fails to merge it with the physical column.
        table = pq.ParquetFile(pq_file).read()
        if column in table.column_names:
            table = table.drop([column])
            pq.write_table(table, pq_file)
        # Precondition the test depends on: the column must be absent from every data file, so its
        # value comes only from the manifest partition tuple (verify the end state, not Spark's behavior).
        assert column not in pq.read_schema(pq_file).names, f"{column} still present in {pq_file}"
    default_upload_directory(cluster, storage_type, root, root)


# Regression for issue #110216: an Iceberg identity-partition column absent from the data files (its
# value stored only in the manifest partition tuple) was read as NULL for every row, and a WHERE
# filter on it was pushed below the read and dropped rows. It is now backfilled from the manifest and
# excluded from PREWHERE. default_download_directory supports only local/s3, so the rewrite tests skip azure.
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_identity_partition_backfill(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_identity_partition_backfill_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, query
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (id BIGINT, region STRING, val STRING)
            USING iceberg
            PARTITIONED BY (region)
            OPTIONS('format-version'='2')
        """
    )
    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'East', 'a'), (2, 'West', 'b'), (3, 'East', 'c'), (4, 'North', 'd')
    """
    )
    _strip_column_from_data_files(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "region"
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    # region backfilled from the manifest (read as NULL before the fix).
    assert (
        instance.query(
            f"SELECT id, region, val FROM {table_function} ORDER BY id"
        ).strip()
        == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc\n4\tNorth\td"
    )

    # Filter on the identity-partition column. PREWHERE on it previously read NULL and dropped every
    # row; assert the same result under both prewhere modes.
    for prewhere in ("1", "0"):
        assert (
            instance.query(
                f"SELECT id, val FROM {table_function} WHERE region = 'East' ORDER BY id"
                f" SETTINGS optimize_move_to_prewhere = {prewhere}"
            ).strip()
            == "1\ta\n3\tc"
        )

    # count() with the same filter (statistics path) and GROUP BY on the backfilled column.
    assert int(instance.query(f"SELECT count() FROM {table_function} WHERE region = 'East'")) == 2
    assert (
        instance.query(
            f"SELECT region, count() FROM {table_function} GROUP BY region ORDER BY region"
        ).strip()
        == "East\t2\nNorth\t1\nWest\t1"
    )


# Regression for issue #110216 (spec evolution): the identity-partition exclusion collects source
# columns across ALL partition specs, not only the current default_spec_id. After the identity
# partition is dropped from the spec, files written under the old spec still omit the column, so it
# must stay backfilled and PREWHERE-excluded for those files.
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_identity_partition_backfill_spec_evolution(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_identity_partition_backfill_spec_evolution_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, query
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (id BIGINT, region STRING, val STRING)
            USING iceberg
            PARTITIONED BY (region)
            OPTIONS('format-version'='2')
        """
    )
    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'East', 'a'), (2, 'West', 'b')")
    # Strip region from the old-spec files so its value is only in the partition tuple.
    _strip_column_from_data_files(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "region"
    )
    # Drop the identity partition. New files carry region physically.
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} DROP PARTITION FIELD region")
    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'East', 'c')")

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    # Old-spec rows still resolve region (would be NULL if only the current spec were consulted).
    assert (
        instance.query(
            f"SELECT id, region, val FROM {table_function} ORDER BY id"
        ).strip()
        == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc"
    )
    # Filter across both specs; the old-spec East row must survive PREWHERE.
    assert (
        instance.query(
            f"SELECT id, val FROM {table_function} WHERE region = 'East' ORDER BY id"
        ).strip()
        == "1\ta\n3\tc"
    )


# Regression for issue #110216 (row-level security): a row policy on an identity-partition column
# was pushed into the in-source filter path and evaluated against the synthetic NULL before the
# backfill, enforcing the wrong visibility. The column is now kept out of that pushdown.
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_identity_partition_backfill_row_policy(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_identity_partition_backfill_row_policy_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, query
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (id BIGINT, region STRING, val STRING)
            USING iceberg
            PARTITIONED BY (region)
            OPTIONS('format-version'='2')
        """
    )
    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'East', 'a'), (2, 'West', 'b'), (3, 'East', 'c')"
    )
    _strip_column_from_data_files(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "region"
    )

    # A row policy needs a named table (not a table function).
    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )
    policy = f"pol_{TABLE_NAME}"
    instance.query(f"DROP ROW POLICY IF EXISTS {policy} ON {TABLE_NAME}")
    instance.query(
        f"CREATE ROW POLICY {policy} ON {TABLE_NAME} USING region = 'East' AS PERMISSIVE TO ALL"
    )
    try:
        # Only region='East' rows are visible; the policy must not evaluate against NULL and drop all.
        for analyzer in ("1", "0"):
            assert (
                instance.query(
                    f"SELECT id, region, val FROM {TABLE_NAME} ORDER BY id"
                    f" SETTINGS enable_analyzer = {analyzer}"
                ).strip()
                == "1\tEast\ta\n3\tEast\tc"
            )
            assert (
                int(
                    instance.query(
                        f"SELECT count() FROM {TABLE_NAME} SETTINGS enable_analyzer = {analyzer}"
                    )
                )
                == 2
            )
    finally:
        instance.query(f"DROP ROW POLICY IF EXISTS {policy} ON {TABLE_NAME}")
