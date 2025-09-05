import glob
import json
import logging
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

import pyspark
import pytest
from azure.storage.blob import BlobServiceClient
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    list_s3_objects,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
    default_download_directory,
    execute_spark_query_general,
    get_creation_expression,
    write_iceberg_from_df,
    generate_data,
    create_iceberg_table,
    drop_iceberg_table,
    check_schema_and_data,
    get_uuid_str,
    check_validity_and_get_prunned_files_general,
    get_last_snapshot
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_complex_types(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    schema = "(x Array(Nullable(Int32)), z Map(Int32, Nullable(Int64)), y Tuple(zip Nullable(Int32), foo Nullable(Int32)))"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, schema, format_version)

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, schema, format_version)

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, schema, format_version, "", True)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    map_value = "{5:6}"
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ([1,2], {map_value}, (3,4));", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '[1,2]\t{5:6}\t(3,4)\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"1")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_compressed_metadata(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_compressed_metadata_" + storage_type + "_" + get_uuid_str()

    table_properties = {
        "write.metadata.compression": "gzip"
    }

    df = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob")
    ], ["id", "name"])

    # for some reason write.metadata.compression is not working :(
    df.writeTo(TABLE_NAME) \
        .tableProperty("write.metadata.compression", "gzip") \
        .using("iceberg") \
        .create()

    # manual compression of metadata file before upload, still test some scenarios
    subprocess.check_output(f"gzip /iceberg_data/default/{TABLE_NAME}/metadata/v1.metadata.json", shell=True)

    # Weird but compression extension is really in the middle of the file name, not in the end...
    subprocess.check_output(f"mv /iceberg_data/default/{TABLE_NAME}/metadata/v1.metadata.json.gz /iceberg_data/default/{TABLE_NAME}/metadata/v1.gz.metadata.json", shell=True)

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, explicit_metadata_path="")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE not ignore(*)") == "1\tAlice\n2\tBob\n"


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = "test_row_based_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id int) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}')")

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (42);")

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '42\n123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '42\n123\n456\n'

    if storage_type != "local":
        return

    initial_files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"4")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 3

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup")
    with pytest.raises(Exception):
        instance.query(f"INSERT INTO {TABLE_NAME} VALUES (777777777777);", settings={"allow_experimental_insert_into_iceberg": 1})


    files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(initial_files) == len(files)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_from_zero(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = "test_row_based_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id int) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}')")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"3")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_with_partitioned_table(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                id INT,
                name STRING,
                value DECIMAL(10, 2),
                created_at DATE,
                event_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (bucket(3, id), bucket(2, name), bucket(5, created_at), bucket(3, event_time))
            OPTIONS('format-version'='{format_version}')
        """
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    instance.query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (2, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (3, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (4, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (5, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_experimental_insert_into_iceberg": 1}
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'

    instance.query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (10, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (20, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (30, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (40, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (50, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_experimental_insert_into_iceberg": 1}
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n10\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n20\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n30\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n40\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n50\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"3")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 10

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_minmax_pruning_for_arrays_and_maps_subfields_disabled(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_disable_minmax_pruning_for_arrays_and_maps_subfields_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            measurements ARRAY<DOUBLE>
            ) USING iceberg
            TBLPROPERTIES (
            'write.metadata.metrics.max' = 'measurements.element',
            'write.metadata.metrics.min' = 'measurements.element'
            );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
            (1, array(23.5, 24.1, 22.8, 25.3, 23.9)),
            (2, array(18.2, 19.5, 17.8, 20.1, 19.3, 18.7)),
            (3, array(30.0, 31.2, 29.8, 32.1, 30.5, 29.9, 31.0)),
            (4, array(15.5, 16.2, 14.8, 17.1, 16.5)),
            (5, array(27.3, 28.1, 26.9, 29.2, 28.5, 27.8, 28.3, 27.6));
        """
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=True,
    )

    table_select_expression = table_creation_expression

    instance.query(f"SELECT * FROM {table_select_expression} ORDER BY ALL")


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_create_table(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String)", format_version)

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String)", format_version)

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String)", format_version, "", True)    

    assert '`x` String' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"2")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("partition_type", ["y", "identity(y)", "(identity(y))", "icebergTruncate(3, y)", "(identity(y), icebergBucket(3, x))", "(x, y)"])
def test_writes_create_partitioned_table(started_cluster, format_version, storage_type, partition_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String, y Int64)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"2")

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_relevant_iceberg_schema_chosen(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_relevant_iceberg_schema_chosen_" + storage_type + "_" + get_uuid_str()
    
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            a INT NOT NULL
        ) using iceberg
        TBLPROPERTIES ('format-version' = '2',
            'commit.manifest.min-count-to-merge' = '1',
            'commit.manifest-merge.enabled' = 'true');
        """
    )

    values_list = ", ".join(["(1)" for _ in range(5)])
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES {values_list};
        """
    )

    spark.sql(
    f"""
        ALTER TABLE {TABLE_NAME} ADD COLUMN b INT;
    """
    )

    values_list = ", ".join(["(1, 2)" for _ in range(5)])
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES {values_list};
        """
    )

    spark.sql(f"CALL system.rewrite_manifests('{TABLE_NAME}')")

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )


    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=True,
    )

    instance.query(f"SELECT * FROM {table_creation_expression} WHERE b >= 2", settings={"input_format_parquet_filter_push_down": 0, "input_format_parquet_bloom_filter_push_down": 0})


def test_writes_create_table_bugs(started_cluster):
    instance = started_cluster.instances["node1"]
    TABLE_NAME = "test_relevant_iceberg_schema_chosen_" + get_uuid_str()
    TABLE_NAME_1 = "test_relevant_iceberg_schema_chosen_" + get_uuid_str()
    instance.query(
        f"CREATE TABLE {TABLE_NAME} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME}/', 'CSV') AS (SELECT 1 OFFSET 1 ROW);",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
        f"CREATE TABLE {TABLE_NAME_1} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME_1}/', 'CSV') PARTITION BY (icebergTruncate(c0));",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_with_compression_metadata(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String, y Int64)", format_version, use_version_hint=True, compression_method="gzip")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_experimental_insert_into_iceberg": 1, "iceberg_metadata_compression_method": "gzip"})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_create_version_hint(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String, y Int64)", format_version, use_version_hint=True)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    target_suffix = b'v1.metadata.json'
    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "rb") as f:
        assert f.read()[-len(target_suffix):] == target_suffix

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL", ) == '123\t1\n'

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    target_suffix = b'v2.metadata.json'
    with open(f"/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "rb") as f:
        assert f.read()[-len(target_suffix):] == target_suffix

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_statistics_by_minmax_pruning(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_minmax_pruning_" + storage_type + "_" + get_uuid_str()

    schema = """
    (tag Int32,
    date Date32,
    ts DateTime,
    name String,
    number Int64)
    """
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, schema, format_version)

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, '2024-01-20',
        '2024-02-20 10:00:00',
        'vasya', 5);
    """,
    settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, '2024-02-20',
        '2024-03-20 15:00:00',
        'vasilisa', 6);
    """,
    settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, '2025-03-20',
        '2024-04-30 14:00:00',
        'icebreaker', 7);
    """,
    settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, '2025-04-20',
        '2024-05-30 14:00:00',
        'iceberg', 8);
    """,
    settings={"allow_experimental_insert_into_iceberg": 1}
    )


    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergMinMaxIndexPrunedFiles', select_expression
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL"
        )
        == 0
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE date <= '2024-01-25' ORDER BY ALL"
        )
        == 3
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag == 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag <= 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name == 'vasilisa' ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name < 'kek' ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number == 8 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number <= 5 ORDER BY ALL"
        )
        == 3
    )

@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_optimize(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    def get_array(query_result: str):
        arr = sorted([int(x) for x in query_result.strip().split("\n")])
        print(arr)
        return arr

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    snapshot_id = get_last_snapshot(f"/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 110)")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction" : 1})

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 90)"
    )

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )
    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 90


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_drop_table(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String, y Int64)", format_version, use_version_hint=True)

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL", ) == '123\t1\n'

    drop_iceberg_table(instance, TABLE_NAME)
    with pytest.raises(Exception):
        drop_iceberg_table(instance, TABLE_NAME)
    drop_iceberg_table(instance, TABLE_NAME, True)

    files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    # drop should not delete user data
    assert len(files) > 0


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_writes_schema_evolution(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    with pytest.raises(Exception):
        instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Int64;", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Nullable(Int64);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert '`x` Nullable(Int64)' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN y Nullable(Float64);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t\\N\n'
    assert '`y` Nullable(Float64)' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (124, 4.56);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t\\N\n124\t4.5600000000000005\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN x;", settings={"allow_experimental_insert_into_iceberg": 1})
    assert '`x` Nullable(Int64)' not in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '4.5600000000000005\n\\N\n'

    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2


@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
@pytest.mark.parametrize("partition_type", ["", "identity(x)", "icebergBucket(3, x)"])
def test_writes_mutate_delete(started_cluster, storage_type, partition_type):
    format_version = 2
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (999);", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '123';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '999';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n'

    if storage_type != "local":
        return
    initial_files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup")
    with pytest.raises(Exception):
        instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '456';", settings={"allow_experimental_insert_into_iceberg": 1})

    files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(initial_files) == len(files)

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1



@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_pruning_nullable_bug(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (c0 STRING)
        USING iceberg
    """)

    spark.sql(f"""
        INSERT INTO {TABLE_NAME} VALUES ('Pasha Technick'), (NULL)
    """)
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE c0 IS NULL;")) == 1
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE c0 IS NULL;") == '\\N\n'


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_nullable_bugs2(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(c0 Nullable(String))", format_version, "(c0)")
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (NULL), ('Monetochka'), ('Maneskin'), ('Noize MC');", settings={"allow_experimental_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'Maneskin\nMonetochka\nNoize MC\n\\N\n'

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_system_iceberg_metadata(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_system_iceberg_metadata_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    def get_iceberg_metadata_to_dict(query_id: str):
        instance = started_cluster.instances["node1"]
        result = dict()
        for name in ['content', 'content_type', 'table_path', 'file_path', 'row_in_file']:
            # We are ok with duplicates in the table itself but for test purposes we want to remove duplicates here
            select_distinct_expression = f"SELECT DISTINCT(*) FROM (SELECT content, content_type, table_path, file_path, row_in_file FROM system.iceberg_metadata_log WHERE query_id = '{query_id}') ORDER BY ALL"
            query_result = instance.query(f"SELECT {name} FROM ({select_distinct_expression})")
            result[name] = query_result.split('\n')
            result[name] = list(filter(lambda x: len(x) > 0, result[name]))
        result['row_in_file'] = list(map(lambda x : int(x) if x.isdigit() else None, result['row_in_file']))
        return result
    
    def verify_result_dictionary(diction : dict, allowed_content_types : set):
        # Expected content_type and only it is present
        if set(diction['content_type']) != allowed_content_types:
            raise ValueError("Content type mismatch. Expected: {}, got: {}".format(allowed_content_types, set(diction['content_type'])))
        # For all entries we have the same table_path
        if not (len(set(diction['table_path'])) == 1 or (len(allowed_content_types) == 0 and len(diction['table_path']) == 0)):
            raise ValueError("Unexpected number of table paths are found for one query. Set: {}".format(set(diction['table_path'])))
        extensions = list(map(lambda x: x.split('.')[-1], diction['file_path']))
        for i in range(len(diction['content_type'])):
            if diction['content_type'][i] == 'Metadata':
                # File with content_type 'Metadata' has json extension
                if extensions[i] != 'json':
                    raise ValueError("Unexpected file extension for Metadata. Expected: json, got: {}".format(extensions[i]))
            else:
                # File with content_types except 'Metadata' has avro extension
                if extensions[i] != 'avro':
                    raise ValueError("Unexpected file extension for {}. Expected: avro, got: {}".format(diction['content_type'][i], extensions[i]))

        # All content is json-serializable
        for content in diction['content']:
            try:
                json.loads(content)
            except:
                raise ValueError("Content is not valid JSON. Content: {}".format(content))
        for file_path in set(diction['file_path']):
            row_values = set()
            number_of_missing_row_values = 0
            number_of_rows = 0
            for i in range(len(diction['file_path'])):
                if file_path == diction['file_path'][i]:
                    if diction['row_in_file'][i] is not None:
                        row_values.add(diction['row_in_file'][i])
                        # If row is present the type is entry
                        if diction['content_type'][i] not in ['ManifestFileEntry', 'ManifestListEntry']:
                            raise ValueError("Row should not be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))
                        number_of_rows += 1
                    else:
                        # If row is not present that the type is metadata
                        if diction['content_type'][i] not in ['Metadata', 'ManifestFileMetadata', 'ManifestListMetadata']:
                            raise ValueError("Row should be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))

                        number_of_missing_row_values += 1
                    
            # We have exactly one metadata file
            if number_of_missing_row_values != 1:
                raise ValueError("Not a one row value (corresponding to metadata file) is missing for file path: {}".format(file_path))

            # Rows in avro files are consistent
            if len(row_values) != number_of_rows:
                raise ValueError("Unexpected number of row values for file path: {}".format(file_path))
            for i in range(number_of_rows):
                if not i in row_values:
                    raise ValueError("Missing row value for file path: {}, missing row index: {}".format(file_path, i))


    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    content_types = ["Metadata", "ManifestListMetadata", "ManifestListEntry", "ManifestFileMetadata", "ManifestFileEntry"]
    settings = ["none", "metadata", "manifest_list_metadata", "manifest_list_entry", "manifest_file_metadata", "manifest_file_entry"]

    for i in range(len(settings)):
        allowed_content_types = set(content_types[:i])

        query_id = TABLE_NAME + "_" + str(i) + "_" + uuid.uuid4().hex

        assert instance.query(f"SELECT * FROM {TABLE_NAME}", query_id = query_id,  settings={"iceberg_metadata_log_level":settings[i]})

        instance.query("SYSTEM FLUSH LOGS iceberg_metadata_log")

        diction = get_iceberg_metadata_to_dict(query_id)

        try:
            verify_result_dictionary(diction, allowed_content_types)
        except:
            print("Dictionary: {}, Allowed Content Types: {}".format(diction, allowed_content_types))
            raise

    
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_writes_field_partitioning(started_cluster, storage_type):
    format_version = 2
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    partition_spec = "(identity(id), identity(i32), identity(u32), identity(d), identity(d32), identity(i64), identity(u64), identity(dt), identity(s))"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(id UInt32,i32 Int32,u32 UInt32,d Date,d32 Date32,i64 Int64,u64 UInt64,dt DateTime,f32 Float32,f64 Float64,s String)", format_version, partition_spec)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"""
    INSERT INTO {TABLE_NAME} VALUES
    (
        1,
        -123,
        123,
        '2025-08-27',
        '2025-08-27',
        -123456789,
        123456789,
        '2025-08-27 12:34:56',
        3.14,
        2.718281828,
        '@capibarsci'
    );""", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\t-123\t123\t2025-08-27\t2025-08-27\t-123456789\t123456789\t2025-08-27 12:34:56.000000\t3.14\t2.718281828\t@capibarsci\n'
    if storage_type != "local":
        return

    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    df.sort()
    assert str(df) == "[Row(id=1, i32=-123, u32=123, d=datetime.date(2025, 8, 27), d32=datetime.date(2025, 8, 27), i64=-123456789, u64=123456789, dt=datetime.datetime(2025, 8, 27, 12, 34, 56), f32=3.140000104904175, f64=2.718281828, s='@capibarsci')]"


@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
@pytest.mark.parametrize("partition_type", ["", "identity(x)", "icebergBucket(3, x)"])
def test_writes_mutate_update(started_cluster, storage_type, partition_type):
    format_version = 2
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x String, y Int32)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('456', 2);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n456\t2\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('999', 3);", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = '777' WHERE x = '123';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\t2\n777\t1\n999\t3\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = 'goshan dr' WHERE x = '777';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\t2\n999\t3\ngoshan dr\t1\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = 'pudge1000-7' WHERE y = 2;", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '999\t3\ngoshan dr\t1\npudge1000-7\t2\n'

    if storage_type != "local":
        return
    default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    df.sort()
    assert str(df) == "[Row(x='999', y=3), Row(x='goshan dr', y=1), Row(x='pudge1000-7', y=2)]"

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_different_path_format_error(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = "test_row_based_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id string) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}')")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('maneskin');", settings={"allow_experimental_insert_into_iceberg": 1})
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('radiohead');", settings={"allow_experimental_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": True})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL;") == 'maneskin\nradiohead\n'

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_multiple_files(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x Int32)", format_version)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    data = ""
    expected_result = ""
    for i in range(1058449):
        data += f"({i}), "
        expected_result += f"{i}\n"
    data = data[:len(data) - 2]
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES {data};", settings={"allow_experimental_insert_into_iceberg": 1, "max_iceberg_data_file_rows" : 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == expected_result

    files = default_download_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert sum([file[-7:] == "parquet" for file in files]) == 2
