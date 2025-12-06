import pytest
import logging
import pyspark
import os
import shutil
import tempfile
import json

from helpers.cluster import ClickHouseCluster, minio_access_key, minio_secret_key
from helpers.s3_tools import (
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    S3Downloader,
    prepare_s3_bucket,
)
from helpers.iceberg_utils import (
    get_uuid_str,
    default_upload_directory,
    default_download_directory,
)


def get_spark(cluster: ClickHouseCluster):
    iceberg_version = "1.4.3"
    spark_version = "3.5.1"
    hadoop_aws_version = "3.3.4"
    jdk_bundle = "1.12.262"

    builder = (
        pyspark.sql.SparkSession.builder
        .appName("IcebergMultiStorageExample")
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version},"
            f"org.apache.spark:spark-avro_2.12:{spark_version},"
            f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},"
            f"com.amazonaws:aws-java-sdk-bundle:{jdk_bundle}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "s3a://root/var/lib/clickhouse/user_files/iceberg_data",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{cluster.minio_ip}:{cluster.minio_port}/",
        )
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .master("local")
    )
    return builder.master("local").getOrCreate()


@pytest.fixture(scope="package")
def started_cluster_iceberg():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)

        cluster.spark_session = get_spark(cluster)

        cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
        cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)
        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])

        yield cluster

    finally:
        cluster.shutdown()


def test_table_metadata_in_different_bucket(started_cluster_iceberg):
    """
    Test Iceberg table stored in 2 different S3 buckets.

    More complex cases (e.g. metadata on S3, data local) require AVRO manifest file manipulation.
    """
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_multistorage_" + get_uuid_str()
    format_version = "2"
    
    # Create a second bucket for metadata
    metadata_bucket = f"{started_cluster_iceberg.minio_bucket}-metadata"
    data_bucket = started_cluster_iceberg.minio_bucket  # Original bucket for data
    
    try:
        if not started_cluster_iceberg.minio_client.bucket_exists(metadata_bucket):
            started_cluster_iceberg.minio_client.make_bucket(metadata_bucket)
    except Exception as e:
        logging.warning(f"Error creating bucket {metadata_bucket}: {e}")

    # Create table in Spark (everything goes to data_bucket initially)
    spark.conf.set("spark.sql.iceberg.commit.sync", "true")

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id INT,
            value STRING
        )
        USING iceberg
        OPTIONS('format-version'='{format_version}');
    """
    )

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'first'), (2, 'second'), (3, 'third');")

    default_upload_directory(
        started_cluster_iceberg,
        "s3",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Copy ONLY metadata files to metadata_bucket via download_directory
    temp_dir = tempfile.mkdtemp()
    host_download_path = os.path.join(temp_dir, TABLE_NAME)
    os.makedirs(host_download_path, exist_ok=True)
    
    default_download_directory(
        started_cluster_iceberg,
        "s3",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        host_download_path,
    )
    
    # Verify download worked
    assert os.path.exists(host_download_path), f"Download path doesn't exist: {host_download_path}"
    if os.path.exists(host_download_path):
        downloaded_items = os.listdir(host_download_path)
    
    # Upload only metadata directory to metadata_bucket
    host_metadata_dir = os.path.join(host_download_path, "metadata")
    assert os.path.exists(host_metadata_dir), f"Metadata directory not found at {host_metadata_dir}"
    
    metadata_uploader = S3Uploader(started_cluster_iceberg.minio_client, metadata_bucket)
    uploaded_count = 0

    # Find and update metadata.json files to point location to data_bucket
    metadata_json_files = []
    for root, dirs, files in os.walk(host_metadata_dir):
        for file in files:
            if file.endswith(".metadata.json"):
                metadata_json_files.append(os.path.join(root, file))
    
    # Update metadata.json files to use absolute location
    data_bucket_location = f"s3a://{data_bucket}/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    for metadata_json_file in metadata_json_files:
        with open(metadata_json_file, 'r') as f:
            metadata = json.load(f)
        
        if "location" in metadata:
            old_location = metadata["location"]
            metadata["location"] = data_bucket_location
        
        with open(metadata_json_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    # Now upload all metadata files (including updated metadata.json)
    for root, dirs, files in os.walk(host_metadata_dir):
        for file in files:
            host_file = os.path.join(root, file)
            rel_path = os.path.relpath(host_file, host_download_path)
            s3_key = f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/{rel_path}"
            metadata_uploader.upload_file(host_file, s3_key)
            uploaded_count += 1
    
    assert uploaded_count > 0, f"No metadata files found to upload from {host_metadata_dir}"
    
    # Verify files were actually uploaded
    objects = list(started_cluster_iceberg.minio_client.list_objects(
        metadata_bucket, 
        prefix=f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/",
        recursive=True
    ))
    assert len(objects) > 0, f"No metadata files found in {metadata_bucket} after upload!"
    
    shutil.rmtree(temp_dir)

    data_s3_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    data_s3_url = f"http://minio1:9001/{data_bucket}/"
    
    result_baseline = instance.query(
        f"SELECT * FROM icebergS3(s3, filename='{data_s3_path}', format=Parquet, url='{data_s3_url}') ORDER BY id"
    )

    expected_rows = ["1\tfirst", "2\tsecond", "3\tthird"]
    rows_baseline = result_baseline.strip().split("\n")
    assert len(rows_baseline) == 3, f"Expected 3 rows from baseline, got {len(rows_baseline)}"
    for i, row in enumerate(rows_baseline):
        assert row == expected_rows[i], f"Row {i}: expected {expected_rows[i]}, got {row}"
