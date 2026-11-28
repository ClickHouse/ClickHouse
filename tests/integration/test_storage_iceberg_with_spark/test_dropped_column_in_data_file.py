import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)


def create_merging_table(spark, table_name, columns):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({columns}) USING iceberg
        TBLPROPERTIES ('format-version' = '2',
            'commit.manifest.min-count-to-merge' = '1',
            'commit.manifest-merge.enabled' = 'true');
        """
    )


def expire_all_snapshots_but_current(spark, table_name):
    spark.sql(
        f"""
        CALL system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '2100-01-01 00:00:00',
            retain_last => 1)
        """
    )


def read_table(started_cluster, storage_type, table_name, query):
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    table_expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster,
        table_function=True,
    )
    instance = started_cluster.instances["node1"]
    return [
        instance.query(
            query.format(table=table_expression),
            settings={"input_format_parquet_use_native_reader_v3": use_v3},
        )
        for use_v3 in [1, 0]
    ]


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_dropped_column_in_data_file(started_cluster_iceberg_with_spark, storage_type):
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_dropped_column_in_data_file_" + storage_type + "_" + get_uuid_str()

    create_merging_table(spark, TABLE_NAME, "id INT NOT NULL, legacy_col STRING")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'dropped');")
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP COLUMN legacy_col;")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2);")
    expire_all_snapshots_but_current(spark, TABLE_NAME)

    for result in read_table(
        started_cluster_iceberg_with_spark,
        storage_type,
        TABLE_NAME,
        "SELECT id FROM {table} ORDER BY id",
    ):
        assert result == "1\n2\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_dropped_subcolumn_in_data_file(started_cluster_iceberg_with_spark, storage_type):
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_dropped_subcolumn_in_data_file_" + storage_type + "_" + get_uuid_str()

    create_merging_table(spark, TABLE_NAME, "id INT NOT NULL, s STRUCT<a: INT, b: STRING>")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, named_struct('a', 10, 'b', 'dropped'));")
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP COLUMN s.b;")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2, named_struct('a', 20));")
    expire_all_snapshots_but_current(spark, TABLE_NAME)

    for result in read_table(
        started_cluster_iceberg_with_spark,
        storage_type,
        TABLE_NAME,
        "SELECT id, s.a FROM {table} ORDER BY id",
    ):
        assert result == "1\t10\n2\t20\n"
