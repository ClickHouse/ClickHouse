
import pytest

from helpers.iceberg_utils import (
    convert_schema_and_data_to_pandas_df,
    get_raw_schema_and_data,
    get_uuid_str,
    default_upload_directory,
    get_creation_expression
)
from pandas.testing import assert_frame_equal

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_correct_column_mapper_is_chosen(
    started_cluster_iceberg_schema_evolution,
    storage_type,
    format_version
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_correct_column_mapper_is_chosen_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            a int NOT NULL,
            b int NOT NULL
        )
        USING iceberg
        OPTIONS ('format-version'='{format_version}');
        """
    )

    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES (0, 1);
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN b TO c;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN a TO b;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN c TO a;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER a;
        """
    )
    
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES (1, 0);
        """
    )

    table_function = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_schema_evolution,
        table_function=True,
        is_cluster=True,
    )


    default_upload_directory(
        started_cluster_iceberg_schema_evolution,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    raw_schema, raw_data = get_raw_schema_and_data(instance, table_function)

    _schema_df, data_df = convert_schema_and_data_to_pandas_df(raw_schema, raw_data)

    reference_df = spark.sql(
        f"""
        SELECT * FROM {TABLE_NAME} ORDER BY a;
        """
    ).toPandas()

    assert_frame_equal(data_df, reference_df)
