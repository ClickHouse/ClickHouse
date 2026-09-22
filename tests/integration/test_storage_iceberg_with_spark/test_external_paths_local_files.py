import pytest

from helpers.iceberg_utils import additional_upload_directory, default_upload_directory, get_uuid_str
from .external_paths_utils import (
    create_and_upload_table,
    _check_cluster_function_rejects_table,
    _create_iceberg_s3_table,
    _relocate_local_data_files_outside_table_directory,
    _rewrite_paths_to_local_uri,
)


def test_cluster_function_rejects_external_local_file(started_cluster_iceberg_with_spark):
    TABLE_NAME = f"test_cluster_external_local_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "localhost")

    _check_cluster_function_rejects_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path, "1\talpha\n2\tbeta\n3\tgamma\n")


def test_cluster_function_rejects_external_local_delete_file(started_cluster_iceberg_with_spark):
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_external_local_delete_{get_uuid_str()}"

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "localhost", deletes_only=True)

    _check_cluster_function_rejects_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path, "1\talpha\n3\tgamma\n")


@pytest.mark.parametrize("scheme", ["", "file://"])
def test_local_table_with_external_data_files(started_cluster_iceberg_with_spark, scheme):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_bare_external_local_{get_uuid_str()}"
    external_dir = f"/var/lib/clickhouse/user_files/iceberg_external_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    host_path = _relocate_local_data_files_outside_table_directory(started_cluster_iceberg_with_spark, TABLE_NAME, external_dir, scheme)
    for node in ("node2", "node3"):
        additional_upload_directory(
            started_cluster_iceberg_with_spark, node, "local", f"{host_path}/metadata", f"{host_path}/metadata")

    error = instance.query_and_get_error(
        f"SELECT * FROM icebergLocalCluster('cluster_simple', local, path = '{host_path}', format=Parquet) ORDER BY id")
    assert "cannot be read by a cluster function" in error

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergLocal(local, path = '{host_path}', format=Parquet)")
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


def test_trivial_count_rejects_path_outside_user_files(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_count_outside_user_files_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    # Keep the local files present so failure proves path rejection, not a missing file.
    outside_dir = f"/var/lib/clickhouse/iceberg_outside_user_files_{get_uuid_str()}"
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=outside_dir)
    expected_error = "outside of allowed `user_files` path"

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    assert expected_error in instance.query_and_get_error(f"SELECT count() FROM {TABLE_NAME}")

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")
