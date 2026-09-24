import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)


# A ClickHouse-created table has no snapshots. A `refs.main` entry pointing at
# snapshot -1 does not comply with the Iceberg spec, which requires every ref to
# point at an existing snapshot. Iceberg Java rejects such a table on load with
# "Snapshot for reference ... does not exist".
@pytest.mark.parametrize("format_version", [1, 2])
def test_writes_spark_reads_empty_table(started_cluster_iceberg_with_spark, format_version):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_spark_reads_empty_table_" + str(format_version) + "_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(a Int32, b String)",
        format_version,
        use_version_hint=True,
    )
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == "0\n"

    default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, f"{local_path}/", f"{local_path}/"
    )

    # Spark must open the empty table before any ClickHouse insert.
    assert spark.read.format("iceberg").load(local_path).collect() == []

    # Spark must also be able to commit to it.
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD b")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'x')")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == "1\tx\n"
