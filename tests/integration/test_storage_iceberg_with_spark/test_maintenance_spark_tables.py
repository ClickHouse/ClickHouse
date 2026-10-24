import io
import shutil

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)
from helpers.s3_tools import minio_user
from helpers.test_tools import assert_eq_with_retry

SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "allow_iceberg_remove_orphan_files": 1,
    "allow_experimental_expire_snapshots": 1,
    "iceberg_orphan_files_min_retention_seconds": 0,
}


def metrics(instance, table, command):
    rows = instance.query(f"ALTER TABLE {table} EXECUTE {command}", settings=SETTINGS)
    return {name: int(value) for name, value in (line.split("\t") for line in rows.splitlines())}


@pytest.mark.parametrize("format_version", ["2", "3"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_maintenance_on_spark_table(started_cluster_iceberg_with_spark, format_version, storage_type):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table = f"test_maintenance_spark_{storage_type}_{format_version}_{get_uuid_str()}"
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table}"

    # Merge-on-read deletes give position delete files in v2 and deletion vectors in v3.
    spark.sql(
        f"""
        CREATE TABLE {table} (id INT, p INT) USING iceberg PARTITIONED BY (p)
        TBLPROPERTIES ('format-version' = '{format_version}', 'write.delete.mode' = 'merge-on-read')
        """
    )
    for start in (0, 100, 200):
        spark.sql(f"INSERT INTO {table} SELECT id, id % 3 FROM range({start}, {start + 100})")
    spark.sql(f"DELETE FROM {table} WHERE id % 10 = 0")
    first_snapshot = spark.sql(f"SELECT snapshot_id FROM default.{table}.snapshots ORDER BY committed_at LIMIT 1").first()[0]
    # The files replaced here stay reachable from older snapshots until they expire.
    spark.sql(f"CALL spark_catalog.system.rewrite_data_files(table => 'default.{table}', options => map('rewrite-all', 'true'))")
    spark.sql(f"CALL spark_catalog.system.compute_table_stats(table => 'default.{table}')")
    expected = spark.sql(f"SELECT count(*), sum(id) FROM {table}").first()

    default_upload_directory(cluster, storage_type, f"/iceberg_data/default/{table}/", f"/iceberg_data/default/{table}/")
    create_iceberg_table(storage_type, instance, table, cluster)

    expired = metrics(instance, table, "expire_snapshots(retention_period = '1ms', retain_last = 1)")
    assert expired["deleted_data_files_count"] > 0
    # v3 deletion vectors go with the data files they belong to. v2 position delete files stay in the
    # current snapshot: the rewritten files keep the starting sequence number, so Spark does not treat
    # the deletes as old enough to drop, and expire must not delete files a snapshot still references.
    if format_version == "3":
        assert expired["deleted_position_delete_files_count"] > 0
    else:
        assert expired["deleted_position_delete_files_count"] == 0

    orphan = f"{local_path}/data/orphan.parquet"
    if storage_type == "s3":
        cluster.minio_client.put_object(cluster.minio_bucket, orphan.lstrip("/"), io.BytesIO(b"junk"), 4)
    elif storage_type == "azure":
        cluster.blob_service_client.get_blob_client(cluster.azure_container_name, orphan).upload_blob(b"junk")
    else:
        instance.exec_in_container(["bash", "-c", f"echo junk > {orphan}"])
    cutoff = instance.query("SELECT toString(now() + 1)").strip()
    assert_eq_with_retry(instance, f"SELECT now() > toDateTime('{cutoff}')", "1")
    removed = metrics(instance, table, f"remove_orphan_files(older_than = '{cutoff}')")
    # metadata.json files are left out: ClickHouse's own commit may leave one out of metadata-log.
    removed.pop("deleted_metadata_files_count")
    assert {name: value for name, value in removed.items() if value} == {"deleted_data_files_count": 1}

    expected_row = f"{expected[0]}\t{expected[1]}\n"
    assert instance.query(f"SELECT count(), sum(id) FROM {table}") == expected_row
    if storage_type == "s3":
        cluster_function = get_creation_expression(storage_type, table, cluster, table_function=True, run_on_cluster=True)
        assert instance.query(f"SELECT count(), sum(id) FROM {cluster_function}") == expected_row

    # Spark must read the table ClickHouse maintained, and only through the files that are left.
    shutil.rmtree(local_path)
    default_download_directory(cluster, storage_type, local_path, local_path)
    spark.sql(f"REFRESH TABLE {table}")
    assert tuple(spark.sql(f"SELECT count(*), sum(id) FROM {table}").first()) == tuple(expected)
    with pytest.raises(Exception, match="Cannot find snapshot"):
        spark.read.option("snapshot-id", first_snapshot).format("iceberg").load(local_path).count()


def test_maintenance_with_read_only_credentials(started_cluster_iceberg_with_spark):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    table = f"test_maintenance_read_only_{get_uuid_str()}"
    read_only_table = f"{table}_read_only"
    key_prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table}"

    create_iceberg_table("s3", instance, table, cluster, "(id Int64)")
    for i in range(3):
        instance.query(f"INSERT INTO {table} VALUES ({i})", settings=SETTINGS)
    expected = "0\n1\n2\n"

    orphan = f"{key_prefix}/data/orphan.parquet"
    cluster.minio_client.put_object(cluster.minio_bucket, orphan, io.BytesIO(b"junk"), 4)

    with minio_user(cluster, ["s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"]) as (access_key, secret_key):
        instance.query(
            f"CREATE TABLE {read_only_table} ENGINE = IcebergS3("
            f"'http://minio1:9001/{cluster.minio_bucket}/{key_prefix}/', '{access_key}', '{secret_key}')"
        )
        assert instance.query(f"SELECT id FROM {read_only_table} ORDER BY id") == expected

        cutoff = instance.query("SELECT toString(now() + 1)").strip()
        assert_eq_with_retry(instance, f"SELECT now() > toDateTime('{cutoff}')", "1")
        removed = metrics(instance, read_only_table, f"remove_orphan_files(older_than = '{cutoff}')")
        assert removed.pop("failed_deletions_count") > 0
        removed.pop("skipped_missing_metadata_count")
        assert sum(removed.values()) == 0, removed

    cluster.minio_client.stat_object(cluster.minio_bucket, orphan)
    assert instance.query(f"SELECT id FROM {table} ORDER BY id") == expected
