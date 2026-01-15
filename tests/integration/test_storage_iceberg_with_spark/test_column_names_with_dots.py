import pytest

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_column_names_with_dots(started_cluster_iceberg_with_spark, storage_type):
    """
    Test that Iceberg tables with dot-separated column names are read correctly.
    This tests the fix for field ID-based column name mapping in Parquet V3 reader.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_column_names_with_dots_" + storage_type + "_" + get_uuid_str()

    # Create DataFrame with column names containing dots
    data = [
        (1, "value1", "multi_dot_value1"),
        (2, "value2", "multi_dot_value2"),
        (3, "value3", "multi_dot_value3"),
    ]
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name.column", StringType()),
        StructField("double.column.dot", StringType()),
    ])
    df = spark.createDataFrame(data=data, schema=schema)

    write_iceberg_from_df(spark, df, TABLE_NAME, mode="overwrite", format_version="2")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Test via table function
    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    # Verify single-dot column name
    result = instance.query(
        f"SELECT `name.column` FROM {table_function_expr} ORDER BY id"
    ).strip()
    assert result == "value1\nvalue2\nvalue3", f"Expected values, got: {result}"

    # Verify multi-dot column name
    result = instance.query(
        f"SELECT `double.column.dot` FROM {table_function_expr} ORDER BY id"
    ).strip()
    assert result == "multi_dot_value1\nmulti_dot_value2\nmulti_dot_value3", f"Expected values, got: {result}"

    # Verify all columns together
    result = instance.query(
        f"SELECT id, `name.column`, `double.column.dot` FROM {table_function_expr} ORDER BY id"
    ).strip()
    expected = "1\tvalue1\tmulti_dot_value1\n2\tvalue2\tmulti_dot_value2\n3\tvalue3\tmulti_dot_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"

    # Test via table engine
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT `name.column`, `double.column.dot` FROM {TABLE_NAME} ORDER BY id"
    ).strip()
    expected = "value1\tmulti_dot_value1\nvalue2\tmulti_dot_value2\nvalue3\tmulti_dot_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"
