import json
import logging
import os
import random
import string
from datetime import datetime

import pyspark
import pytest
from delta import DeltaTable
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    DecimalType,
    StructField,
    StructType,
    TimestampType,
)
from decimal import Decimal
from pyspark.sql.window import Window

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import (
    S3Uploader,
    upload_directory,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, with_spark=True)


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
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/stateless_worker.xml",
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
                "configs/config.d/stateless_worker.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            with_remote_database_disk=False,  # Disable `with_remote_database_disk` as in `test_replicated_database_and_unavailable_s3``, minIO rejects node2 connections
        )
        cluster.add_instance(
            "node_with_environment_credentials",
            with_minio=True,
            main_configs=[
                "configs/config.d/stateless_worker.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/use_environment_credentials.xml",
            ],
            env_variables={
                "AWS_ACCESS_KEY_ID": minio_access_key,
                "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            },
            with_remote_database_disk=False,
        )

        logging.info("Starting cluster...")
        cluster.start()

        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip(
                "DeltaLake engine is not available"
            )

        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.minio_restricted_bucket = "{}-with-auth".format(cluster.minio_bucket)
        if cluster.minio_client.bucket_exists(cluster.minio_restricted_bucket):
            cluster.minio_client.remove_bucket(cluster.minio_restricted_bucket)

        cluster.minio_client.make_bucket(cluster.minio_restricted_bucket)

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


def create_delta_table(
    instance,
    storage_type,
    table_name,
    cluster,
    format="Parquet",
    table_function=False,
    allow_dynamic_metadata_for_data_lakes=False,
    run_on_cluster=False,
    use_delta_kernel=False,
    **kwargs,
):
    allow_dynamic_metadata_for_datalakes_suffix = (
        " SETTINGS allow_dynamic_metadata_for_data_lakes = 1"
        if allow_dynamic_metadata_for_data_lakes
        else ""
    )

    if storage_type == "s3":
        if "bucket" in kwargs:
            bucket = kwargs["bucket"]
        else:
            bucket = cluster.minio_bucket

        if run_on_cluster:
            assert table_function
            instance.query(
                f"deltalakeS3Cluster('cluster_simple', s3, filename = '{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
                f"SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}"
            )
        else:
            if table_function:
                instance.query(
                    f"deltalakeS3(s3, filename = '{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
                    f"SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}"
                )
            else:
                instance.query(
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {table_name}
                    ENGINE=DeltaLake(s3, filename = '{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')
                    SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}"""
                    + allow_dynamic_metadata_for_datalakes_suffix
                )
    else:
        raise Exception(f"Unknown delta lake storage type: {storage_type}")


def default_upload_directory(
    started_cluster, storage_type, local_path, remote_path, **kwargs
):
    if storage_type == "s3":
        print(kwargs)
        return started_cluster.default_s3_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    else:
        raise Exception(f"Unknown delta storage type: {storage_type}")


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none", node_name="node1"
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
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{node_name}/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


@pytest.mark.parametrize(
    "use_delta_kernel, storage_type",
    [("1", "s3"), ("0", "s3")],
)
def test_single_log_file(started_cluster, use_delta_kernel, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = randomize_table_name("test_single_log_file")

    inserted_data = "SELECT number as a, toString(number + 1) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    # For local storage, we need to use the absolute path
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
    )
    table_path = os.path.join(user_files_path, TABLE_NAME)

    # We need to exclude the leading slash for local storage protocol file://
    delta_path = table_path if storage_type == "local" else f"/{TABLE_NAME}"
    write_delta_from_file(spark, parquet_data_path, delta_path)

    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )

    assert len(files) == 2  # 1 metadata files + 1 data file

    create_delta_table(
        instance,
        storage_type,
        TABLE_NAME,
        started_cluster,
        use_delta_kernel=use_delta_kernel,
    )

    # A bare `count()` is answered by the trivial-count optimization from
    # `DeltaLake` metadata without reading any data, so it does not exercise
    # the `Stateless Worker` exchange at all - it would stay green even if
    # that path regressed. Disabling the optimization does force a real
    # `AggregatingStep` on top of the read, but `ReadFromObjectStorageStep`
    # only implements `isSerializable` under `CLICKHOUSE_CLOUD` (see
    # src/Processors/QueryPlan/ReadFromObjectStorageStep.h), so shipping a
    # fragment containing it to a `Stateless Worker` is unsupported in this
    # (OSS) build and fails outright rather than silently falling back to a
    # single local stage. Pin that today's behaviour is a clean error, not a
    # silent no-op, so implementing distributed `DeltaLake` reads in OSS is a
    # deliberate, visible change to this test.
    with pytest.raises(QueryRuntimeException, match="is not serializable for remote execution"):
        instance.query(
            f"SELECT count() FROM {TABLE_NAME}",
            settings={
                "make_distributed_plan": 1,
                "distributed_plan_max_rows_to_broadcast": 0,
                "optimize_trivial_count_query": 0,
            },
        )

    assert instance.query(f"SELECT * FROM {TABLE_NAME}", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}) == instance.query(
        inserted_data
    )

@pytest.mark.parametrize(
    "use_delta_kernel",
    ["0", "1"],
)
def test_partition_columns(started_cluster, use_delta_kernel):
    pytest.skip("Not stable with some JAVA stack traces")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_columns")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["b", "c", "d", "e", "f", "g"]

    (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "INT")
        .addColumn("e", "TIMESTAMP")
        .addColumn("f", "BOOLEAN")
        .addColumn("g", "DECIMAL(10,2)")
        .addColumn("h", "BOOLEAN")
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
            StructField("e", TimestampType()),
            StructField("f", BooleanType()),
            StructField("g", DecimalType(10, 2)),
            StructField("h", BooleanType()),
        ]
    )

    now = datetime.now()
    for i in range(1, num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-0{i}", "%Y-%m-%d"),
                i,
                (
                    now
                    if i % 2 == 0
                    else datetime.strptime(
                        f"2012-01-0{i} 12:34:56.789123", "%Y-%m-%d %H:%M:%S.%f"
                    )
                ),
                True if i % 2 == 0 else False,
                Decimal(f"{i * 1.11:.2f}"),
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

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}', SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel})"

    result = instance.query(f"describe table {table_function}").strip()
    assert (
        result == "a\tNullable(Int32)\t\t\t\t\t\n"
        "b\tNullable(String)\t\t\t\t\t\n"
        "c\tNullable(Date32)\t\t\t\t\t\n"
        "d\tNullable(Int32)\t\t\t\t\t\n"
        "e\tNullable(DateTime64(6))\t\t\t\t\t\n"
        "f\tNullable(Bool)\t\t\t\t\t\n"
        "g\tNullable(Decimal(10, 2))\t\t\t\t\t\n"
        "h\tNullable(Bool)"
    )

    result = int(instance.query(f"SELECT count() FROM {table_function}", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}))
    assert result == num_rows

    expected_output = f"""1	test1	2000-01-01	1	2012-01-01 12:34:56.789123	false	1.11	true
2	test2	2000-01-02	2	{now}	true	2.22	false
3	test3	2000-01-03	3	2012-01-03 12:34:56.789123	false	3.33	true
4	test4	2000-01-04	4	{now}	true	4.44	false
5	test5	2000-01-05	5	2012-01-05 12:34:56.789123	false	5.55	true
6	test6	2000-01-06	6	{now}	true	6.66	false
7	test7	2000-01-07	7	2012-01-07 12:34:56.789123	false	7.77	true
8	test8	2000-01-08	8	{now}	true	8.88	false
9	test9	2000-01-09	9	2012-01-09 12:34:56.789123	false	9.99	true"""

    assert (
        expected_output
        == instance.query(f"SELECT * FROM {table_function} ORDER BY b", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}).strip()
    )

    query_id = f"query_with_filter_{TABLE_NAME}"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE c == toDateTime('2000/01/05')
            """,
            query_id=query_id, settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}
        )
    )
    assert result == 1

    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE e = toDateTime64('{now}', 6)
            """,
            query_id=query_id, settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}
        )
    )
    assert result == 4

    if use_delta_kernel == 1:
        instance.query("SYSTEM FLUSH LOGS")
        assert num_rows - 1 == int(
            instance.query(
                f"""
            SELECT ProfileEvents['DeltaLakePartitionPrunedFiles']
            FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        """
            )
        )

    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (a Nullable(Int32), b Nullable(String), c Nullable(Date32), d Nullable(Int32), h Nullable(Bool))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')
       SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}
        """
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
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}).strip()
    )

    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/05')", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}
            )
        )
        == 1
    )

    # Subset of columns should work.
    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (b Nullable(String), c Nullable(Date32), d Nullable(Int32))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')
       SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}
       """
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
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast": 0}).strip()
    )

    for i in range(num_rows + 1, 2 * num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-{i}", "%Y-%m-%d"),
                i,
                (
                    now
                    if i % 2 == 0
                    else datetime.strptime(
                        f"2012-01-{i} 12:34:56.789123", "%Y-%m-%d %H:%M:%S.%f"
                    )
                ),
                True if i % 2 == 0 else False,
                Decimal(f"{i * 1.1:.2f}"),
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

    result = int(instance.query(f"SELECT count() FROM {table_function}", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}))
    assert result == num_rows * 2

    assert (
        f"""1	test1	2000-01-01	1	2012-01-01 12:34:56.789123	false	1.11	true
2	test2	2000-01-02	2	{now}	true	2.22	false
3	test3	2000-01-03	3	2012-01-03 12:34:56.789123	false	3.33	true
4	test4	2000-01-04	4	{now}	true	4.44	false
5	test5	2000-01-05	5	2012-01-05 12:34:56.789123	false	5.55	true
6	test6	2000-01-06	6	{now}	true	6.66	false
7	test7	2000-01-07	7	2012-01-07 12:34:56.789123	false	7.77	true
8	test8	2000-01-08	8	{now}	true	8.88	false
9	test9	2000-01-09	9	2012-01-09 12:34:56.789123	false	9.99	true
10	test10	2000-01-10	10	{now}	true	11	false
11	test11	2000-01-11	11	2012-01-11 12:34:56.789123	false	12.1	true
12	test12	2000-01-12	12	{now}	true	13.2	false
13	test13	2000-01-13	13	2012-01-13 12:34:56.789123	false	14.3	true
14	test14	2000-01-14	14	{now}	true	15.4	false
15	test15	2000-01-15	15	2012-01-15 12:34:56.789123	false	16.5	true
16	test16	2000-01-16	16	{now}	true	17.6	false
17	test17	2000-01-17	17	2012-01-17 12:34:56.789123	false	18.7	true
18	test18	2000-01-18	18	{now}	true	19.8	false"""
        == instance.query(f"SELECT * FROM {table_function} ORDER BY c", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}).strip()
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/15')", settings={"make_distributed_plan": 1, "distributed_plan_max_rows_to_broadcast" : 0}
            )
        )
        == 1
    )
