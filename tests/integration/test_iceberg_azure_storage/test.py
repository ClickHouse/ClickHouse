import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
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

import tempfile

import io
import avro.schema
import avro.io
import avro.datafile
import pandas as pd

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

from tests.integration.helpers.s3_tools import (
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

    for bucket in minio_client.list_buckets():
        for object in minio_client.list_objects(bucket.name, recursive=True):
            print("Object: ", object.object_name)
            extension = object.object_name.split(".")[-1]
            print("File extension: ", extension)
            try:
                response = minio_client.get_object(
                    object.bucket_name, object.object_name
                )

                if extension == "avro":
                    avro_bytes = response.read()

                    # Use BytesIO to create a file-like object from the byte string
                    avro_file = io.BytesIO(avro_bytes)

                    # Read the Avro data
                    reader = avro.datafile.DataFileReader(
                        avro_file, avro.io.DatumReader()
                    )
                    records = [record for record in reader]

                    # Close the reader
                    reader.close()

                    # Now you can work with the records
                    for record in records:
                        # print(json.dumps(record, indent=4, sort_keys=True))
                        print(str(record))
                        # my_json = (
                        #     str(record)
                        #     .replace("'", '"')
                        #     .replace("None", "null")
                        #     .replace('b"', '"')
                        # )
                        # print(my_json)
                        # data = json.loads(my_json)
                        # s = json.dumps(data, indent=4, sort_keys=True)
                        # print(s)
                elif extension == "json":
                    my_bytes_value = response.read()
                    my_json = my_bytes_value.decode("utf8").replace("'", '"')
                    data = json.loads(my_json)
                    s = json.dumps(data, indent=4, sort_keys=True)
                    print(s)
                elif extension == "parquet":
                    # print("To be continued...")
                    # # Your byte string containing the Parquet data
                    # parquet_bytes = response.read()

                    # # Use BytesIO to create a file-like object from the byte string
                    # parquet_file = io.BytesIO(parquet_bytes)

                    # # Read the Parquet data into a PyArrow Table
                    # table = pq.read_table(parquet_file)

                    # # Convert the PyArrow Table to a Pandas DataFrame
                    # df = table.to_pandas()

                    # # Now you can work with s DataFrame
                    # print(df)
                    parquet_bytes = (
                        response.read()
                    )  # Replace with your actual byte string

                    # Create a temporary file and write the byte string to it
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        tmp_file.write(parquet_bytes)
                        tmp_file_path = tmp_file.name

                    # Read the Parquet file using PySpark
                    df = spark.read.parquet(tmp_file_path)

                    # Show the DataFrame
                    print(df.toPandas())
                else:
                    print(response.read())

            finally:
                print("----------------")
                response.close()
                response.release_conn()

    create_iceberg_table(instance, TABLE_NAME)

    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )

    assert 0 == 1
