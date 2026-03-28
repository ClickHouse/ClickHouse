import pytest
import subprocess

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,      
    default_upload_directory
)

@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_compressed_metadata(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
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
    subprocess.check_output(f"gzip /var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/v1.metadata.json", shell=True)

    # Weird but compression extension is really in the middle of the file name, not in the end...
    subprocess.check_output(f"mv /var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/v1.metadata.json.gz /var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/v1.gz.metadata.json", shell=True)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path="")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE not ignore(*)") == "1\tAlice\n2\tBob\n"