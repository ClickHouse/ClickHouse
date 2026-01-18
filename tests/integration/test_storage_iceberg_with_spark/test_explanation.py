import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_explanation(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    for format in ["Parquet", "ORC", "Avro"]:
        TABLE_NAME = (
            "test_explanation_"
            + format
            + "_"
            + format_version
            + "_"
            + storage_type
            + "_"
            + get_uuid_str()
        )

        # Types time, timestamptz, fixed are not supported in Spark.
        spark.sql(
            f"CREATE TABLE {TABLE_NAME} (x int) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}', 'write.format.default' = '{format}')"
        )

        spark.sql(f"insert into {TABLE_NAME} select 42")
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        create_iceberg_table(
            storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, format=format
        )

        res = instance.query(f"EXPLAIN SELECT * FROM {TABLE_NAME}")
        res = list(
            map(
                lambda x: x.split("\t"),
                filter(lambda x: len(x) > 0, res.strip().split("\n")),
            )
        )

        expected = [
            [
                "Expression ((Project names + (Projection + Change column names to column identifiers)))"
            ],
            [f"  ReadFromObjectStorage"],
        ]

        assert res == expected

        # Check that we can parse data
        instance.query(f"SELECT * FROM {TABLE_NAME}")
