import pytest

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from helpers.iceberg_engine import Struct
from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_column_names_with_dots(engine, node, storage_type):
    table = engine.unique_table(f"test_column_names_with_dots_{storage_type}")
    engine.create_table(
        table,
        [("id", "int"), ("name.column", "string"), ("double.column.dot", "string")],
        format_version=2,
    )
    engine.insert(table, [
        (1, "value1", "multi_dot_value1"),
        (2, "value2", "multi_dot_value2"),
        (3, "value3", "multi_dot_value3"),
    ])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    assert node.query(
        f"SELECT `name.column` FROM {target} ORDER BY id"
    ).strip() == "value1\nvalue2\nvalue3"

    assert node.query(
        f"SELECT `double.column.dot` FROM {target} ORDER BY id"
    ).strip() == "multi_dot_value1\nmulti_dot_value2\nmulti_dot_value3"

    assert node.query(
        f"SELECT id, `name.column`, `double.column.dot` FROM {target} ORDER BY id"
    ).strip() == "1\tvalue1\tmulti_dot_value1\n2\tvalue2\tmulti_dot_value2\n3\tvalue3\tmulti_dot_value3"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_nested_struct_with_dotted_field(engine, node, storage_type):
    table = engine.unique_table(f"test_nested_struct_with_dotted_field_{storage_type}")
    fields = [("normal_field", "int"), ("weird.field", "string")]
    engine.create_table(
        table,
        [("id", "int"), ("my_struct", "struct(normal_field:int,weird.field:string)")],
        format_version=2,
    )
    engine.insert(table, [
        (1, Struct(fields, [100, "nested_dot_value1"])),
        (2, Struct(fields, [200, "nested_dot_value2"])),
        (3, Struct(fields, [300, "nested_dot_value3"])),
    ])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    assert node.query(
        f"SELECT my_struct.normal_field, `my_struct.weird.field` FROM {target} ORDER BY id"
    ).strip() == "100\tnested_dot_value1\n200\tnested_dot_value2\n300\tnested_dot_value3"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deeply_nested_struct_with_dotted_names(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deeply_nested_struct_with_dotted_names_" + storage_type + "_" + get_uuid_str()

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

    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    result = instance.query(
        f"SELECT `my.struct.some_dot.separated_parent.weird.field` FROM {table_function_expr} ORDER BY id"
    ).strip()
    expected = "deep_value1\ndeep_value2\ndeep_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    result = instance.query(
        f"SELECT `my.struct.some_dot.separated_parent.weird.field` FROM {TABLE_NAME} ORDER BY id"
    ).strip()
    expected = "deep_value1\ndeep_value2\ndeep_value3"
    assert result == expected, f"Expected:\n{expected}\nGot:\n{result}"
