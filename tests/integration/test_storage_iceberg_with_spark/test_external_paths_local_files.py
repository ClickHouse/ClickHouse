import json
import os

import pytest

from helpers.iceberg_utils import additional_upload_directory, default_upload_directory, get_uuid_str
from helpers.s3_tools import S3Uploader
from .external_paths_utils import (
    ALL_ROWS,
    create_and_upload_table,
    external_bucket,
    _check_cluster_function_rejects_table,
    _create_iceberg_s3_table,
    _delete_file_names,
    _manifest_lists_delete_files,
    _relocate_local_data_files_outside_table_directory,
    _rewrite_paths_to_local_uri,
    find_files,
    modify_avro_file,
)


def test_cluster_function_rejects_external_local_file(started_cluster_iceberg_with_spark):
    TABLE_NAME = f"test_cluster_external_local_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "localhost")

    _check_cluster_function_rejects_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path, ALL_ROWS)


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
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


def test_cluster_function_rejects_bare_local_delete_file_with_external_s3_data(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_bare_local_delete_s3_data_{get_uuid_str()}"
    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    external_dir = f"/var/lib/clickhouse/user_files/iceberg_external_{get_uuid_str()}"
    bucket = external_bucket(started_cluster_iceberg_with_spark)
    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    # Data files go to S3, delete files to a scheme-less local path outside the table directory.
    metadata_dir = os.path.join(host_path, "metadata")
    delete_names = _delete_file_names(metadata_dir)
    assert delete_names

    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json) as f:
            metadata = json.load(f)
        metadata["location"] = host_path
        with open(metadata_json, "w") as f:
            json.dump(metadata, f)

    def relocate(path):
        name = os.path.basename(path)
        if name in delete_names:
            return f"{external_dir}/{name}"
        return f"{minio_url}/{bucket}/{TABLE_NAME}/{name}"

    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        # The delete file's bounds name the original data path, so they would prune it for the relocated data file.
        if _manifest_lists_delete_files(manifest):
            modify_avro_file(manifest, ["data_file", "lower_bounds"], lambda _: None)
            modify_avro_file(manifest, ["data_file", "upper_bounds"], lambda _: None)
        modify_avro_file(manifest, ["data_file", "file_path"], relocate)

    default_upload_directory(started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    for node in ("node2", "node3"):
        additional_upload_directory(
            started_cluster_iceberg_with_spark, node, "local", f"{host_path}/metadata", f"{host_path}/metadata")

    s3_uploader = S3Uploader(started_cluster_iceberg_with_spark.minio_client, bucket)
    for f in find_files(os.path.join(host_path, "data"), ".parquet"):
        name = os.path.basename(f)
        if name in delete_names:
            started_cluster_iceberg_with_spark.default_local_uploader.upload_file(f, f"{external_dir}/{name}")
        else:
            s3_uploader.upload_file(f, f"{TABLE_NAME}/{name}")
    instance.exec_in_container(["bash", "-c", f"rm -f {host_path}/data/*.parquet"])

    error = instance.query_and_get_error(
        f"SELECT * FROM icebergLocalCluster('cluster_simple', local, path = '{host_path}', format=Parquet) ORDER BY id "
        f"SETTINGS s3_allow_server_credentials_in_user_queries = 1")
    assert "cannot be read by a cluster function" in error


def test_cluster_function_reads_local_data_with_external_s3_delete_file(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_local_data_s3_delete_{get_uuid_str()}"
    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    # The default bucket allows anonymous reads, so the replicas need no S3 credentials for it.
    bucket = started_cluster_iceberg_with_spark.minio_bucket
    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    # Data files stay in the table directory, delete files move to S3.
    metadata_dir = os.path.join(host_path, "metadata")
    delete_names = _delete_file_names(metadata_dir)
    assert delete_names

    def relocate(path):
        name = os.path.basename(path)
        if name in delete_names:
            return f"{minio_url}/{bucket}/{TABLE_NAME}_deletes/{name}"
        return path

    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        modify_avro_file(manifest, ["data_file", "file_path"], relocate)

    s3_uploader = S3Uploader(started_cluster_iceberg_with_spark.minio_client, bucket)
    for f in find_files(os.path.join(host_path, "data"), ".parquet"):
        name = os.path.basename(f)
        if name in delete_names:
            s3_uploader.upload_file(f, f"{TABLE_NAME}_deletes/{name}")
            os.remove(f)

    default_upload_directory(started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    for node in ("node2", "node3"):
        additional_upload_directory(started_cluster_iceberg_with_spark, node, "local", host_path, host_path)

    assert instance.query(
        f"SELECT * FROM icebergLocalCluster('cluster_simple', local, path = '{host_path}', format=Parquet) ORDER BY id "
        f"SETTINGS s3_allow_server_credentials_in_user_queries = 1") == "1\talpha\n3\tgamma\n"


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


def test_copied_local_table_reads_files_from_its_own_directory(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_copied_local_table_{get_uuid_str()}"
    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    copy_path = f"{host_path}_copy"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    # Spell every path as a `file://` URI inside the source `location`.
    metadata_dir = os.path.join(host_path, "metadata")
    to_metadata_uri = lambda p: f"file://{host_path}/metadata/{os.path.basename(p)}"
    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json) as f:
            metadata = json.load(f)
        metadata["location"] = f"file://{host_path}"
        for snapshot in metadata.get("snapshots", []):
            snapshot["manifest-list"] = to_metadata_uri(snapshot["manifest-list"])
        with open(metadata_json, "w") as f:
            json.dump(metadata, f)
    for avro_file in find_files(metadata_dir, ".avro"):
        if os.path.basename(avro_file).startswith("snap-"):
            modify_avro_file(avro_file, ["manifest_path"], to_metadata_uri)
        else:
            modify_avro_file(avro_file, ["data_file", "file_path"], lambda p: f"file://{host_path}/data/{os.path.basename(p)}")

    default_upload_directory(started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    # The copy keeps the source `location` and file URIs, while the source loses its data files.
    instance.exec_in_container(["bash", "-c", f"cp -r {host_path} {copy_path} && rm -f {host_path}/data/*.parquet"])

    assert instance.query(f"SELECT * FROM icebergLocal(local, path = '{copy_path}', format=Parquet) ORDER BY id") == ALL_ROWS
