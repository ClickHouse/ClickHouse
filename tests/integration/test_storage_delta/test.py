import glob
import json
import logging
import os
import random
import string
import time
import uuid
from datetime import datetime

import delta
import pyarrow as pa
import pyarrow.parquet as pq
import pyspark
import pytest
from delta import *
from deltalake.writer import write_deltalake
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.s3_tools import (
    get_file_contents,
    list_s3_objects,
    prepare_s3_bucket,
    upload_directory,
)
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local")
    )

    return builder.master("local").getOrCreate()


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)

        cluster.spark_session = get_spark()

        yield cluster

    finally:
        cluster.shutdown()


def write_delta_from_file(spark, path, result_path, mode="overwrite"):
    spark.read.load(path).write.mode(mode).option("compression", "none").format(
        "delta"
    ).option("delta.columnMapping.mode", "name").save(result_path)


def write_delta_from_df(spark, df, result_path, mode="overwrite", partition_by=None):
    if partition_by is None:
        df.write.mode(mode).option("compression", "none").format("delta").option(
            "delta.columnMapping.mode", "name"
        ).save(result_path)
    else:
        df.write.mode(mode).option("compression", "none").format("delta").option(
            "delta.columnMapping.mode", "name"
        ).partitionBy("a").save(result_path)


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


def get_delta_metadata(delta_metadata_file):
    jsons = [json.loads(x) for x in delta_metadata_file.splitlines()]
    combined_json = {}
    for d in jsons:
        combined_json.update(d)
    return combined_json


def create_delta_table(node, table_name, bucket="root"):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=DeltaLake(s3, filename = '{table_name}/', url = 'http://minio1:9001/{bucket}/')"""
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


def test_single_log_file(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_single_log_file")

    inserted_data = "SELECT number as a, toString(number + 1) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2  # 1 metadata files + 1 data file

    create_delta_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


def test_partition_by(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_by")

    write_delta_from_df(
        spark,
        generate_data(spark, 0, 10),
        f"/{TABLE_NAME}",
        mode="overwrite",
        partition_by="a",
    )

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 11  # 10 partitions and 1 metadata file

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


def test_checkpoint(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_checkpoint")

    write_delta_from_df(
        spark,
        generate_data(spark, 0, 1),
        f"/{TABLE_NAME}",
        mode="overwrite",
    )
    for i in range(1, 25):
        write_delta_from_df(
            spark,
            generate_data(spark, i, i + 1),
            f"/{TABLE_NAME}",
            mode="append",
        )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    # 25 data files
    # 25 metadata files
    # 1 last_metadata file
    # 2 checkpoints
    assert len(files) == 25 * 2 + 3

    ok = False
    for file in files:
        if file.endswith("last_checkpoint"):
            ok = True
    assert ok

    create_delta_table(instance, TABLE_NAME)
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} SETTINGS input_format_parquet_allow_missing_columns=1"
            )
        )
        == 25
    )

    table = DeltaTable.forPath(spark, f"/{TABLE_NAME}")
    table.delete("a < 10")
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 15

    for i in range(0, 5):
        write_delta_from_df(
            spark,
            generate_data(spark, i, i + 1),
            f"/{TABLE_NAME}",
            mode="append",
        )
    # + 1 metadata files (for delete)
    # + 5 data files
    # + 5 metadata files
    # + 1 checkpoint file
    # + 1 ?
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 53 + 1 + 5 * 2 + 1 + 1
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 20

    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1").strip()
        == instance.query(
            "SELECT * FROM ("
            "SELECT number, toString(number + 1) FROM numbers(5) "
            "UNION ALL SELECT number, toString(number + 1) FROM numbers(10, 15) "
            ") ORDER BY 1"
        ).strip()
    )


def test_multiple_log_files(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_multiple_log_files")

    write_delta_from_df(
        spark, generate_data(spark, 0, 100), f"/{TABLE_NAME}", mode="overwrite"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2  # 1 metadata files + 1 data file

    s3_objects = list(
        minio_client.list_objects(bucket, f"{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 1

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_delta_from_df(
        spark, generate_data(spark, 100, 200), f"/{TABLE_NAME}", mode="append"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 4  # 2 metadata files + 2 data files

    s3_objects = list(
        minio_client.list_objects(bucket, f"{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 2

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


def test_metadata(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_metadata")

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    data = get_file_contents(
        minio_client,
        bucket,
        f"/{TABLE_NAME}/_delta_log/00000000000000000000.json",
    )
    delta_metadata = get_delta_metadata(data)

    stats = json.loads(delta_metadata["add"]["stats"])
    assert stats["numRecords"] == 100
    assert next(iter(stats["minValues"].values())) == 0
    assert next(iter(stats["maxValues"].values())) == 99

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


def test_types(started_cluster):
    TABLE_NAME = randomize_table_name("test_types")
    spark = started_cluster.spark_session
    result_file = randomize_table_name(f"{TABLE_NAME}_result_2")

    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "ARRAY<STRING>")
        .addColumn("e", "BOOLEAN")
        .execute()
    )
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
    df.write.mode("append").format("delta").saveAsTable(TABLE_NAME)

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    upload_directory(minio_client, bucket, f"/{result_file}", "")

    instance = started_cluster.instances["node1"]
    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"""
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"
    assert (
        instance.query(f"SELECT * FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )


def test_restart_broken(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = "broken"
    TABLE_NAME = randomize_table_name("test_restart_broken")

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    create_delta_table(instance, TABLE_NAME, bucket=bucket)
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

    s3_disk_no_key_errors_metric_value = int(
        instance.query(
            """
            SELECT value
            FROM system.metrics
            WHERE metric = 'DiskS3NoSuchKeyErrors'
            """
        ).strip()
    )

    assert s3_disk_no_key_errors_metric_value == 0

    minio_client.make_bucket(bucket)

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


def test_restart_broken_table_function(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = "broken2"
    TABLE_NAME = randomize_table_name("test_restart_broken_table_function")

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME}
        AS deltaLake(s3, filename = '{TABLE_NAME}/', url = 'http://minio1:9001/{bucket}/')"""
    )
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

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


def test_partition_columns(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_columns")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["b", "c", "d", "e"]

    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "INT")
        .addColumn("e", "BOOLEAN")
        .partitionedBy(partition_columns)
        .execute()
    )
    num_rows = 9

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", IntegerType()),
            StructField("e", BooleanType()),
        ]
    )

    for i in range(1, num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-0{i}", "%Y-%m-%d"),
                i,
                False if i % 2 == 0 else True,
            )
        ]
        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        df.write.mode("append").format("delta").partitionBy(partition_columns).save(
            f"/{TABLE_NAME}"
        )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    result = instance.query(
        f"describe table deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"
    ).strip()

    assert (
        result
        == "a\tNullable(Int32)\t\t\t\t\t\nb\tNullable(String)\t\t\t\t\t\nc\tNullable(Date32)\t\t\t\t\t\nd\tNullable(Int32)\t\t\t\t\t\ne\tNullable(Bool)"
    )

    result = int(
        instance.query(
            f"""SELECT count()
            FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')
            """
        )
    )
    assert result == num_rows
    result = int(
        instance.query(
            f"""SELECT count()
            FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')
            WHERE c == toDateTime('2000/01/05')
            """
        )
    )
    assert result == 1

    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (a Nullable(Int32), b Nullable(String), c Nullable(Date32), d Nullable(Int32), e Nullable(Bool))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"""
    )
    assert (
        """1	test1	2000-01-01	1	true
2	test2	2000-01-02	2	false
3	test3	2000-01-03	3	true
4	test4	2000-01-04	4	false
5	test5	2000-01-05	5	true
6	test6	2000-01-06	6	false
7	test7	2000-01-07	7	true
8	test8	2000-01-08	8	false
9	test9	2000-01-09	9	true"""
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b").strip()
    )

    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/05')"
            )
        )
        == 1
    )

    # Subset of columns should work.
    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (b Nullable(String), c Nullable(Date32), d Nullable(Int32))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"""
    )
    assert (
        """test1	2000-01-01	1
test2	2000-01-02	2
test3	2000-01-03	3
test4	2000-01-04	4
test5	2000-01-05	5
test6	2000-01-06	6
test7	2000-01-07	7
test8	2000-01-08	8
test9	2000-01-09	9"""
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b").strip()
    )

    for i in range(num_rows + 1, 2 * num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-{i}", "%Y-%m-%d"),
                i,
                False if i % 2 == 0 else True,
            )
        ]
        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        df.write.mode("append").format("delta").partitionBy(partition_columns).save(
            f"/{TABLE_NAME}"
        )

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    ok = False
    for file in files:
        if file.endswith("last_checkpoint"):
            ok = True
    assert ok

    result = int(
        instance.query(
            f"""SELECT count()
            FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')
            """
        )
    )
    assert result == num_rows * 2

    assert (
        """1	test1	2000-01-01	1	true
2	test2	2000-01-02	2	false
3	test3	2000-01-03	3	true
4	test4	2000-01-04	4	false
5	test5	2000-01-05	5	true
6	test6	2000-01-06	6	false
7	test7	2000-01-07	7	true
8	test8	2000-01-08	8	false
9	test9	2000-01-09	9	true
10	test10	2000-01-10	10	false
11	test11	2000-01-11	11	true
12	test12	2000-01-12	12	false
13	test13	2000-01-13	13	true
14	test14	2000-01-14	14	false
15	test15	2000-01-15	15	true
16	test16	2000-01-16	16	false
17	test17	2000-01-17	17	true
18	test18	2000-01-18	18	false"""
        == instance.query(
            f"""
SELECT * FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123') ORDER BY c
        """
        ).strip()
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/15')"
            )
        )
        == 1
    )


def test_complex_types(started_cluster):
    node = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("name", pa.string()),
            (
                "address",
                pa.struct(
                    [
                        ("street", pa.string()),
                        ("city", pa.string()),
                        ("state", pa.string()),
                    ]
                ),
            ),
            ("interests", pa.list_(pa.string())),
            (
                "metadata",
                pa.map_(
                    pa.string(), pa.string()
                ),  # Map with string keys and string values
            ),
        ]
    )

    # Create sample data
    data = [
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["John Doe", "Jane Smith", "Jake Johnson"], type=pa.string()),
        pa.array(
            [
                {"street": "123 Elm St", "city": "Springfield", "state": "IL"},
                {"street": "456 Maple St", "city": "Shelbyville", "state": "IL"},
                {"street": "789 Oak St", "city": "Ogdenville", "state": "IL"},
            ],
            type=schema.field("address").type,
        ),
        pa.array(
            [
                pa.array(["dancing", "coding", "hiking"]),
                pa.array(["dancing", "coding", "hiking"]),
                pa.array(["dancing", "coding", "hiking"]),
            ],
            type=schema.field("interests").type,
        ),
        pa.array(
            [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value3", "key2": "value4"},
                {"key1": "value5", "key2": "value6"},
            ],
            type=schema.field("metadata").type,
        ),
    ]

    endpoint_url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"
    aws_access_key_id = "minio"
    aws_secret_access_key = "minio123"
    table_name = randomize_table_name("test_complex_types")

    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{table_name}"
    table = pa.Table.from_arrays(data, schema=schema)

    write_deltalake(path, table, storage_options=storage_options)

    assert "1\n2\n3\n" in node.query(
        f"SELECT id FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', 'minio123')"
    )
    assert (
        "('123 Elm St','Springfield','IL')\n('456 Maple St','Shelbyville','IL')\n('789 Oak St','Ogdenville','IL')"
        in node.query(
            f"SELECT address FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', 'minio123')"
        )
    )
    assert (
        "{'key1':'value1','key2':'value2'}\n{'key1':'value3','key2':'value4'}\n{'key1':'value5','key2':'value6'}"
        in node.query(
            f"SELECT metadata FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', 'minio123')"
        )
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_filesystem_cache(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    TABLE_NAME = randomize_table_name("test_filesystem_cache")
    bucket = started_cluster.minio_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    create_delta_table(instance, TABLE_NAME, bucket=bucket)

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS filesystem_cache_name = 'cache1'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    count = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS filesystem_cache_name = 'cache1'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert count == int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )


def test_replicated_database_and_unavailable_s3(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]

    DB_NAME = randomize_table_name("db")
    TABLE_NAME = randomize_table_name("test_replicated_database_and_unavailable_s3")
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_restricted_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    node1.query(
        f"CREATE DATABASE {DB_NAME} ENGINE=Replicated('/clickhouse/databases/{DB_NAME}', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE {DB_NAME} ENGINE=Replicated('/clickhouse/databases/{DB_NAME}', 'shard1', 'node2')"
    )

    parquet_data_path = create_initial_data_file(
        started_cluster,
        node1,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
    )

    endpoint_url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"
    aws_access_key_id = "minio"
    aws_secret_access_key = "minio123"

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("name", pa.string()),
        ]
    )

    data = [
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["John Doe", "Jane Smith", "Jake Johnson"], type=pa.string()),
    ]
    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{TABLE_NAME}"
    table = pa.Table.from_arrays(data, schema=schema)

    write_deltalake(path, table, storage_options=storage_options)

    with PartitionManager() as pm:
        pm_rule_reject = {
            "probability": 1,
            "destination": node2.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm_rule_drop_all = {
            "destination": node2.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "DROP",
        }
        pm._add_rule(pm_rule_reject)

        node1.query(
            f"""
            DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME};
            CREATE TABLE {DB_NAME}.{TABLE_NAME}
            AS deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{TABLE_NAME}' , 'minio', 'minio123')
            """
        )

        assert TABLE_NAME in node1.query(
            f"select name from system.tables where database = '{DB_NAME}'"
        )
        assert TABLE_NAME in node2.query(
            f"select name from system.tables where database = '{DB_NAME}'"
        )

        replica_path = f"/clickhouse/databases/{DB_NAME}/replicas/shard1|node2"
        zk = started_cluster.get_kazoo_client("zoo1")
        zk.set(replica_path + "/digest", "123456".encode())

        assert "123456" in node2.query(
            f"SELECT * FROM system.zookeeper WHERE path = '{replica_path}'"
        )

        node2.restart_clickhouse()

        assert "123456" not in node2.query(
            f"SELECT * FROM system.zookeeper WHERE path = '{replica_path}'"
        )
