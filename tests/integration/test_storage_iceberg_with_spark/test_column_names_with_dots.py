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


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_nested_struct_with_dotted_field(started_cluster_iceberg_with_spark, storage_type):
    """
    Test that nested struct fields with dot-separated names are read correctly.
    This tests the fix for prefix stripping in useColumnMapperIfNeeded.
    E.g., for my_struct.weird.field we should return "weird.field", not just "field".
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_nested_struct_with_dotted_field_" + storage_type + "_" + get_uuid_str()

    # Create DataFrame with nested struct containing a dotted field
    data = [
        (1, (100, "nested_dot_value1")),
        (2, (200, "nested_dot_value2")),
        (3, (300, "nested_dot_value3")),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField(
                "my_struct",
                StructType(
                    [
                        StructField("normal_field", IntegerType()),
                        StructField("weird.field", StringType()),
                    ]
                )
            )
        ]
    )
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

    # Verify nested struct with dotted field via table function
    result = instance.query(
        f"SELECT my_struct.normal_field, `my_struct.weird.field` FROM {table_function_expr} ORDER BY id"
    ).strip()
    expected = "100\tnested_dot_value1\n200\tnested_dot_value2\n300\tnested_dot_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"

    # Test via table engine
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT my_struct.normal_field, `my_struct.weird.field` FROM {TABLE_NAME} ORDER BY id"
    ).strip()
    expected = "100\tnested_dot_value1\n200\tnested_dot_value2\n300\tnested_dot_value3"
    assert result == expected, f"Expected:\n{expected}\nGot\n{result}"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deeply_nested_struct_with_dotted_names(started_cluster_iceberg_with_spark, storage_type):
    """
    Test deeply nested structs where EVERY level has dots in the name.
    Structure: my.struct -> some_dot.separated_parent -> weird.field
    Full path: my.struct.some_dot.separated_parent.weird.field

    This verifies that prefix stripping works correctly at all nesting depths.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deeply_nested_struct_with_dotted_names_" + storage_type + "_" + get_uuid_str()

    # Create DataFrame with deeply nested struct containing dotted names
    data = [
        (1, (("deep_value1",),)),
        (2, (("deep_value2",),)),
        (3, (("deep_value3",),)),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField(
                "my.struct",
                StructType(
                    [
                        StructField(
                            "some_dot.separated_parent",
                            StructType(
                                [
                                    StructField("weird.field", StringType()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )
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

    # Query the deeply nested dotted field
    result = instance.query(
        f"SELECT `my.struct.some_dot.separated_parent.weird.field` FROM {table_function_expr} ORDER BY id"
    ).strip()
    expected = "deep_value1\ndeep_value2\ndeep_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"

    # Test via table engine
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT `my.struct.some_dot.separated_parent.weird.field` FROM {TABLE_NAME} ORDER BY id"
    ).strip()
    expected = "deep_value1\ndeep_value2\ndeep_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_dotted_array_column(started_cluster_iceberg_with_spark, storage_type):
    """
    Regression test for issue #90731.
    A top-level ARRAY column whose name literally contains a dot (e.g. `a.b`)
    must be returned with its actual values, not as an empty array.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_dotted_array_column_" + storage_type + "_" + get_uuid_str()

    from pyspark.sql.types import ArrayType

    data = [(["a", "b", "c"],)]
    schema = StructType([
        StructField("a.b", ArrayType(StringType())),
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

    result = instance.query(
        f"SELECT `a.b` FROM {table_function_expr}"
    ).strip()
    assert result == "['a','b','c']", f"Expected ['a','b','c'], got: {result}"

    # Test via table engine
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT `a.b` FROM {TABLE_NAME}"
    ).strip()
    assert result == "['a','b','c']", f"Expected ['a','b','c'], got: {result}"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_dotted_array_alongside_real_nested(started_cluster_iceberg_with_spark, storage_type):
    """
    Regression guard: a lone dotted Array column (`a.b`) must not interfere with
    a genuine flat-Nested group (`c.x`, `c.y`) that shares a different prefix.
    All three columns must round-trip correctly.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_dotted_array_alongside_real_nested_" + storage_type + "_" + get_uuid_str()

    from pyspark.sql.types import ArrayType, IntegerType as SparkIntegerType

    data = [(["a", "b", "c"], [1, 2], ["p", "q"])]
    schema = StructType([
        StructField("a.b", ArrayType(StringType())),
        StructField("c.x", ArrayType(SparkIntegerType())),
        StructField("c.y", ArrayType(StringType())),
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

    result = instance.query(
        f"SELECT `a.b`, `c.x`, `c.y` FROM {table_function_expr}"
    ).strip()
    assert result == "['a','b','c']\t[1,2]\t['p','q']", \
        f"Unexpected result via table function: {result}"

    # Test via table engine
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT `a.b`, `c.x`, `c.y` FROM {TABLE_NAME}"
    ).strip()
    assert result == "['a','b','c']\t[1,2]\t['p','q']", \
        f"Unexpected result via table engine: {result}"
