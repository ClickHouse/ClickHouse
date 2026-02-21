import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
    get_creation_expression
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_create_table(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version)

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version)

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version, "", True)    

    assert '`x` String' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"2")

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2

@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_create_table_order_by(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int32, y String)", order_by="(x)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (4, 'bc'), (2, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '1\n2\n4\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int32, y String)", order_by="(identity(x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (4, 'bc'), (2, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '1\n2\n4\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int32, y String)", order_by="(icebergBucket(16, x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (4, 'bc'), (2, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '1\n2\n4\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x UInt32, y String)", order_by="(icebergTruncate(16, x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (48, 'bc'), (32, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '1\n32\n48\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x UInt32, y String)", order_by="(x DESC)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (4, 'bc'), (2, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '4\n2\n1\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x DateTime, y String)", order_by="(toYearNumSinceEpoch(x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('2019-01-01 00:00:00', 'abc'), ('2021-01-03 00:00:00', 'bc'), ('2020-01-02 00:00:00', 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '2019-01-01 00:00:00.000000\n2020-01-02 00:00:00.000000\n2021-01-03 00:00:00.000000\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x DateTime, y String)", order_by="(toMonthNumSinceEpoch(x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('2019-01-01 00:00:00', 'abc'), ('2021-03-03 00:00:00', 'bc'), ('2020-02-02 00:00:00', 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '2019-01-01 00:00:00.000000\n2020-02-02 00:00:00.000000\n2021-03-03 00:00:00.000000\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x DateTime, y String)", order_by="(toRelativeDayNum(x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('2019-01-01 00:00:00', 'abc'), ('2021-03-03 00:00:00', 'bc'), ('2020-02-02 00:00:00', 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '2019-01-01 00:00:00.000000\n2020-02-02 00:00:00.000000\n2021-03-03 00:00:00.000000\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x DateTime, y String)", order_by="(toRelativeHourNum(x))", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('2019-01-01 03:00:00', 'abc'), ('2021-03-03 01:00:00', 'bc'), ('2020-02-02 02:00:00', 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT x FROM {TABLE_NAME}") == '2019-01-01 03:00:00.000000\n2020-02-02 02:00:00.000000\n2021-03-03 01:00:00.000000\n'

    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x UInt32, y String)", order_by="(x DESC, y ASC)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'abc'), (4, 'bc'), (4, 'd');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == '4\tbc\n4\td\n1\tabc\n'

@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_create_table_bug_partitioning(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_create_table_" + storage_type + "_" + get_uuid_str()

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(c0 Int)", format_version, partition_by="(min(c0) OVER ())")

@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_create_table_bug_tuple(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_create_table_bug_tuple_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int, y Int)", order_by="(x, y)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 2), (1, 3);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == '1\t2\n1\t3\n'

    TABLE_NAME = "test_writes_create_table_bug_tuple_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int, y Tuple(c2 Int))", order_by="(x, y)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, (2)), (1, (3));", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == '1\t(2)\n1\t(3)\n'

    TABLE_NAME = "test_writes_create_table_bug_tuple_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int, y Int)", order_by="(x ASC, y DESC)", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 2), (1, 3);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == '1\t3\n1\t2\n'

    TABLE_NAME = "test_writes_create_table_bug_tuple_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int, y Int)", order_by="tuple()", format_version=format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 2), (1, 3);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\t2\n1\t3\n'

@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
@pytest.mark.parametrize(
    "schema,order_by",
    [
        ("(c0 Int)", "(1)"),
        ("(c0 Int)", "(1+1)"),
        ("(c0 Int)", "rand(c0)"),
        ("(c0 Int)", "(now())"),
        ("(c0 Int)", "identity(1)"),
        ("(c0 Int)", "identity(1+1)"),
        ("(c0 Int, c1 Int)", "icebergBucket(c0, c1)"),
        ("(c0 Int, c1 Int)", "icebergBucket(1, 2)"),
        ("(c0 Int)", "icebergBucket(c0, 1)"),
        ("(c0 Int)", "icebergBucket(-1, c0)"),
    ]
)
def test_order_by_bad_arguments_ast_parsing(
    started_cluster_iceberg_with_spark, format_version, storage_type, schema, order_by
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_order_by_bad_ast_" + storage_type + "_" + get_uuid_str()

    query = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        schema,
        format_version=format_version,
        order_by=order_by,
    )
    
    error = instance.query_and_get_error(query)
    assert (
        "Code: 36" in error or "BAD_ARGUMENTS" in error
    ), f"Expected BAD_ARGUMENTS error (Code: 36), got: {error}"
    assert (
        "Invalid iceberg sort order" in error 
        or "Unsupported function" in error 
        or "expected a column identifier" in error.lower()
        or "expected (integer_literal, column_identifier)" in error.lower()
        or "expected 1 or 2 arguments" in error.lower()
        or "expected a non-negative integer literal" in error.lower()
        or "arguments are missing" in error.lower()
    ), f"Expected error message about invalid sort order, got: {error}"


@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
@pytest.mark.parametrize(
    "schema,order_by",
    [
        ("(c0 Int)", "identity()"),
        ("(c0 Int, c1 Int)", "identity(c0, c1, c0)"),
    ]
)
def test_order_by_bad_arguments_wrong_count(
    started_cluster_iceberg_with_spark, format_version, storage_type, schema, order_by
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_order_by_bad_count_" + storage_type + "_" + get_uuid_str()

    query = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        schema,
        format_version=format_version,
        order_by=order_by,
    )
    
    error = instance.query_and_get_error(query)
    assert (
        "Code: 42" in error or "NUMBER_OF_ARGUMENTS_DOESNT_MATCH" in error
    ), f"Expected NUMBER_OF_ARGUMENTS_DOESNT_MATCH error (Code: 42), got: {error}"
    assert (
        "doesn't match" in error.lower()
    ), f"Expected error message about argument count mismatch, got: {error}"


@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
@pytest.mark.parametrize(
    "schema,order_by",
    [
        ("(c0 Int)", "icebergTruncate(1.5, c0)"),
    ]
)
def test_order_by_bad_arguments_wrong_type(
    started_cluster_iceberg_with_spark, format_version, storage_type, schema, order_by
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_order_by_bad_type_" + storage_type + "_" + get_uuid_str()

    query = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        schema,
        format_version=format_version,
        order_by=order_by,
    )
    
    error = instance.query_and_get_error(query)
    assert (
        "Code: 43" in error or "ILLEGAL_TYPE_OF_ARGUMENT" in error
    ), f"Expected ILLEGAL_TYPE_OF_ARGUMENT error (Code: 43), got: {error}"
    assert (
        "should be" in error.lower()
    ), f"Expected error message about invalid argument type, got: {error}"
