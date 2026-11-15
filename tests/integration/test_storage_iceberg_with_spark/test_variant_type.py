import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

from helpers.test_tools import TSV


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_variant_type(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    if int(spark.version.split(".")[0]) < 4:
        pytest.skip(f"the VARIANT type needs Spark 4.0, the runner has {spark.version}")
    TABLE_NAME = "test_variant_type_" + storage_type + "_" + get_uuid_str()

    # `variant` was introduced in Iceberg format version 3. Iceberg writes it as an unshredded
    # group of two plain BYTE_ARRAY leaves annotated with the `VARIANT` logical type:
    #
    #   optional group v (Variant(1)) {
    #     required binary metadata;
    #     required binary value;
    #   }
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, v VARIANT)
        USING iceberg
        TBLPROPERTIES ('format-version'='3')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, cast(100000 as variant)),
        (2, cast('hello' as variant))
        """
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        format_version=3,
    )

    table_function_expr = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        format_version=3,
    )

    assert instance.query(f"DESCRIBE {table_function_expr} FORMAT TSV") == TSV(
        [
            ["id", "Nullable(Int32)"],
            ["v", "Dynamic"],
        ]
    )

    assert instance.query(
        f"SELECT id, dynamicType(v), v FROM {table_function_expr} ORDER BY id FORMAT TSV"
    ).strip() == ("1\tInt32\t100000\n" "2\tString\thello")
