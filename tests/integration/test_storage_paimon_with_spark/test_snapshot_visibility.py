import pytest

from helpers.paimon_utils import (
    create_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    prepare_simple_paimon_table,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_table_function_sees_new_snapshot(
    started_cluster_paimon_with_spark, storage_type
):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_tf_new_snapshot_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    assert instance.query(f"SELECT count() FROM {table_expr}") == "2\n"

    # Table functions read fresh metadata per query: a new snapshot must be
    # visible immediately after upload, without any SYSTEM command.
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')")
    upload_paimon_table(started_cluster, storage_type, table_name)
    assert instance.query(f"SELECT count() FROM {table_expr}") == "3\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_engine_sees_new_snapshot(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_engine_new_snapshot_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    create_paimon_table(storage_type, instance, table_name, started_cluster)
    with drop_tables_at_exit(instance, table_name):
        assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"

        spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')")
        upload_paimon_table(started_cluster, storage_type, table_name)
        # Metadata refresh may be asynchronous — poll instead of asserting immediately.
        wait_until_query_result(instance, f"SELECT count() FROM {table_name}", "3\n")
