import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    generate_data_complex,
    create_iceberg_table,
    get_uuid_str
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_single_iceberg_file(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_single_iceberg_file_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_multistage_prewhere(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_single_iceberg_file_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    data = generate_data_complex(spark, 0, 1000, 100)
    data.writeTo(TABLE_NAME).tableProperty(
        "format-version", format_version
    ).tableProperty(
        "write.parquet.row-group-size-bytes", "1000000000"
    ).using("iceberg").create()


    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()
    target_0 = ""
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a = 0 AND a = 1") == target_0
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a = 0 AND a = 1", query_id=query_id) == target_0
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1000\t1\n'

    query_id = get_uuid_str()
    target_1 = ""
    for i in range(100):
        target_1 += f"0\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a = 0") == target_1
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a = 0", query_id=query_id) == target_1
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1000\t1\n'

    query_id = get_uuid_str()
    target_2 = ""
    for i in range(100, 1000):
        target_2 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0") == target_2
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0", query_id=query_id) == target_2
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1000\t1\n'

    query_id = get_uuid_str()
    target_3 = ""
    for i in range(100, 1000):
        if (i + 1000) // 100 == 13:
            continue
        target_3 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c != 13") == target_3
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c != 13", query_id=query_id) == target_3
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1900\t2\n'

    query_id = get_uuid_str()
    target_4 = ""
    for i in range(100, 1000):
        c_val = (i + 1000) // 100
        if c_val >= 12 and c_val < 14:
            target_4 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c >= 12 AND c < 14") == target_4
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c >= 12 AND c < 14", query_id=query_id) == target_4
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1900\t2\n'

    query_id = get_uuid_str()
    target_5 = ""
    for i in range(100, 1000):
        c_val = (i + 1000) // 100
        b_val = (i + 1) // 100
        if c_val != 13 and b_val != 12:
            target_5 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c != 13 AND b != '12'") == target_5
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c != 13 AND b != '12'", query_id=query_id) == target_5
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '2700\t3\n'
