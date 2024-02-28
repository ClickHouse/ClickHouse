import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import pyspark
import logging
import os
import json
import pytest
import time
import glob
import uuid
import os

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import current_timestamp
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from minio.deleteobjects import DeleteObject

from helpers.s3_tools import (
    prepare_s3_bucket,
    upload_directory,
    get_file_contents,
    list_s3_objects,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )
    return builder.master("local").getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = get_spark()

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def write_iceberg_from_file(
    spark, path, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            spark.read.load(f"file://{path}").writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            spark.read.load(f"file://{path}").writeTo(table_name).partitionedBy(
                partition_by
            ).tableProperty("format-version", format_version).using("iceberg").create()
    else:
        spark.read.load(f"file://{path}").writeTo(table_name).append()


def write_iceberg_from_df(
    spark, df, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).partitionedBy(partition_by).using("iceberg").create()
    else:
        df.writeTo(table_name).append()


def generate_data(spark, start, end):
    a = spark.range(start, end, 1).toDF("a")
    b = spark.range(start + 1, end + 1, 1).toDF("b")
    b = b.withColumn("b", b["b"].cast(StringType()))

    a = a.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    b = b.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df = a.join(b, on=["row_index"]).drop("row_index")
    return df


def create_iceberg_table(node, table_name, format="Parquet", bucket="root"):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=Iceberg(s3, filename = 'iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"""
    )


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none"
):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            s3_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_single_iceberg_file(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_single_iceberg_file_" + format_version

    inserted_data = "SELECT number, toString(number) as string FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    write_iceberg_from_file(
        spark, parquet_data_path, TABLE_NAME, format_version=format_version
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_partition_by(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_partition_by_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
        partition_by="a",
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )
    assert len(files) == 14  # 10 partitiions + 4 metadata files

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_multiple_iceberg_files(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_multiple_iceberg_files_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", ""
    )
    # ['/iceberg_data/default/test_multiple_iceberg_files/data/00000-1-35302d56-f1ed-494e-a85b-fbf85c05ab39-00001.parquet',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/version-hint.text',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/3127466b-299d-48ca-a367-6b9b1df1e78c-m0.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/snap-5220855582621066285-1-3127466b-299d-48ca-a367-6b9b1df1e78c.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/v1.metadata.json']
    assert len(files) == 5

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
        format_version=format_version,
    )
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", ""
    )
    assert len(files) == 9

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_types(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_types_" + format_version

    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
        )
    ]
    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType()),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    write_iceberg_from_df(
        spark, df, TABLE_NAME, mode="overwrite", format_version=format_version
    )

    upload_directory(minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", "")

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"iceberg(s3, filename='iceberg_data/default/{TABLE_NAME}/')"
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_delete_files(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_delete_files_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 0")
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 0
    assert instance.contains_in_log("Processing delete file for path")

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="upsert",
        format_version=format_version,
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 150")
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_evolved_schema(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_evolved_schema_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    expected_data = instance.query(f"SELECT * FROM {TABLE_NAME} order by a, b")

    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMNS (x bigint)")
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME}")
    assert "UNSUPPORTED_METHOD" in error

    data = instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS iceberg_engine_ignore_schema_evolution=1"
    )
    assert data == expected_data


def test_row_based_deletes(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_row_based_deletes"

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100)"
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 10")
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME}")
    assert "UNSUPPORTED_METHOD" in error


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_schema_inference(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    for format in ["Parquet", "ORC", "Avro"]:
        TABLE_NAME = "test_schema_inference_" + format + "_" + format_version

        # Types time, timestamptz, fixed are not supported in Spark.
        spark.sql(
            f"CREATE TABLE {TABLE_NAME} (intC int, longC long, floatC float, doubleC double, decimalC1 decimal(10, 3), decimalC2 decimal(20, 10), decimalC3 decimal(38, 30), dateC date,  timestampC timestamp, stringC string, binaryC binary, arrayC1 array<int>, mapC1 map<string, string>, structC1 struct<field1: int, field2: string>, complexC array<struct<field1: map<string, array<map<string, int>>>, field2: struct<field3: int, field4: string>>>) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}', 'write.format.default' = '{format}')"
        )

        spark.sql(
            f"insert into {TABLE_NAME} select 42, 4242, 42.42, 4242.4242, decimal(42.42), decimal(42.42), decimal(42.42), date('2020-01-01'), timestamp('2020-01-01 20:00:00'), 'hello', binary('hello'), array(1,2,3), map('key', 'value'), struct(42, 'hello'), array(struct(map('key', array(map('key', 42))), struct(42, 'hello')))"
        )

        files = upload_directory(
            minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
        )

        create_iceberg_table(instance, TABLE_NAME, format)

        res = instance.query(
            f"DESC {TABLE_NAME} FORMAT TSVRaw", settings={"print_pretty_type_names": 0}
        )
        expected = TSV(
            [
                ["intC", "Nullable(Int32)"],
                ["longC", "Nullable(Int64)"],
                ["floatC", "Nullable(Float32)"],
                ["doubleC", "Nullable(Float64)"],
                ["decimalC1", "Nullable(Decimal(10, 3))"],
                ["decimalC2", "Nullable(Decimal(20, 10))"],
                ["decimalC3", "Nullable(Decimal(38, 30))"],
                ["dateC", "Nullable(Date)"],
                ["timestampC", "Nullable(DateTime64(6, 'UTC'))"],
                ["stringC", "Nullable(String)"],
                ["binaryC", "Nullable(String)"],
                ["arrayC1", "Array(Nullable(Int32))"],
                ["mapC1", "Map(String, Nullable(String))"],
                ["structC1", "Tuple(field1 Nullable(Int32), field2 Nullable(String))"],
                [
                    "complexC",
                    "Array(Tuple(field1 Map(String, Array(Map(String, Nullable(Int32)))), field2 Tuple(field3 Nullable(Int32), field4 Nullable(String))))",
                ],
            ]
        )

        assert res == expected

        # Check that we can parse data
        instance.query(f"SELECT * FROM {TABLE_NAME}")


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_metadata_file_selection(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_metadata_selection_" + format_version

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )

    for i in range(50):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_metadata_file_format_with_uuid(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_metadata_selection_with_uuid_" + format_version

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )

    for i in range(50):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    for i in range(50):
        os.rename(
            f"/iceberg_data/default/{TABLE_NAME}/metadata/v{i + 1}.metadata.json",
            f"/iceberg_data/default/{TABLE_NAME}/metadata/{str(i).zfill(5)}-{uuid.uuid4()}.metadata.json",
        )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500


def test_restart_broken(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = "broken2"
    TABLE_NAME = "test_restart_broken_table_function"

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    write_iceberg_from_file(spark, parquet_data_path, TABLE_NAME, format_version="1")
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )
    create_iceberg_table(instance, TABLE_NAME, bucket=bucket)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    s3_objects = list_s3_objects(minio_client, bucket, prefix="")
    assert (
        len(
            list(
                minio_client.remove_objects(
                    bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )
    minio_client.remove_bucket(bucket)

    instance.restart_clickhouse()

    assert "NoSuchBucket" in instance.query_and_get_error(
        f"SELECT count() FROM {TABLE_NAME}"
    )

    minio_client.make_bucket(bucket)

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
