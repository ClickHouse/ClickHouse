import pytest

from helpers.paimon_utils import (
    TYPES_COLUMNS_SQL,
    TYPES_EXPECTED_DATA,
    TYPES_EXPECTED_SCHEMA,
    TYPES_NULLS_ROW,
    TYPES_VALUES_ROW,
    create_spark_paimon_table,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    spark_table,
    upload_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_all_types(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_all_types_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, TYPES_COLUMNS_SQL)
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES {TYPES_VALUES_ROW}")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES {TYPES_NULLS_ROW}")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    schema, data = get_schema_and_data(
        instance,
        table_expr,
        order_by="f_int_nn",
        settings={"session_timezone": "UTC"},
    )
    assert schema == TYPES_EXPECTED_SCHEMA
    assert data == TYPES_EXPECTED_DATA
