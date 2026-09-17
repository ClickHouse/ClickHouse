import os
import json
import pytest

from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .external_paths_utils import (
    _check_cluster_function_rejects_table,
    _create_iceberg_s3_table,
    _relocate_local_data_files_outside_table_directory,
    _rewrite_paths_to_local_uri,
    find_files,
    modify_avro_file,
)


# RFC 8089 makes `file://localhost/path` equivalent to `file:///path`, so it is read from the local
# filesystem -- but only by the server that holds the file. A cluster function hands every file to an
# arbitrary replica, which need not share the coordinator's filesystem, so it fails closed on a local
# file outside the table directory while a plain read on the node that holds it still returns it.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3735408259
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3789962927
def test_cluster_function_rejects_external_local_file(started_cluster_iceberg_with_spark):
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_external_local_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "localhost")

    _check_cluster_function_rejects_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path, "1\talpha\n2\tbeta\n3\tgamma\n")


# The delete files attached to a task reach the worker as metadata paths it resolves on its own, so the
# same locality problem applies to them.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3955701103
def test_cluster_function_rejects_external_local_delete_file(started_cluster_iceberg_with_spark):
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_external_local_delete_{get_uuid_str()}"

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    # Only the delete file becomes node1-local, so the data file's path gives the distributor nothing
    # to reject.
    base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "localhost", deletes_only=True)

    _check_cluster_function_rejects_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path, "1\talpha\n3\tgamma\n")


# A `file://` data path that escapes the table directory gets its own storage rooted at that path: the
# table's own `local` storage resolves keys relative to the table root and rejects anything above it.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3789962911
def test_local_table_with_data_files_outside_table_directory(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_local_outside_dir_{get_uuid_str()}"
    external_dir = f"/var/lib/clickhouse/user_files/iceberg_external_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    # A `file://` location, as a catalog with a `file:` warehouse writes it, so the data paths below
    # share its scheme and authority.
    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json, "r") as f:
            metadata = json.load(f)
        metadata["location"] = f"file://{host_path}"
        with open(metadata_json, "w") as f:
            json.dump(metadata, f, indent=2)

    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        modify_avro_file(manifest, ["data_file", "file_path"], lambda p: f"file://{external_dir}/{os.path.basename(p)}")

    default_upload_directory(started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    for f in find_files(data_dir, ".parquet"):
        started_cluster_iceberg_with_spark.default_local_uploader.upload_file(f, f"{external_dir}/{os.path.basename(f)}")
    instance.exec_in_container(["bash", "-c", f"rm -f {host_path}/data/*.parquet"])

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergLocal(local, path = '{host_path}', format=Parquet)")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


# The same for the scheme-less spelling, which `write_full_path_in_iceberg_metadata = 0` writes and is
# therefore the default for a local table: the locality problem is in the path, not in its spelling.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3959138419
def test_cluster_function_rejects_bare_absolute_external_local_file(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_cluster_bare_external_local_{get_uuid_str()}"
    external_dir = f"/var/lib/clickhouse/user_files/iceberg_external_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    host_path = _relocate_local_data_files_outside_table_directory(started_cluster_iceberg_with_spark, TABLE_NAME, external_dir)

    error = instance.query_and_get_error(
        f"SELECT * FROM icebergLocalCluster('cluster_simple', local, path = '{host_path}', format=Parquet) ORDER BY id")
    assert "cannot be read by a cluster function" in error

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergLocal(local, path = '{host_path}', format=Parquet)")
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


# `count()` answered from the manifests stands in for a scan, so it must not answer for a table whose
# files the scan would refuse to open: both rejections below are on the path itself, whatever the user
# is granted.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r4025761739
@pytest.mark.parametrize("rejection", ["remote_authority", "outside_user_files"])
def test_trivial_count_not_answered_for_unopenable_external_path(started_cluster_iceberg_with_spark, rejection):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_count_unopenable_{rejection}_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    # The files are put in place locally too, so the query fails on the path, not on a missing file.
    if rejection == "remote_authority":
        base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "other-host")
        expected_error = "refers to host 'other-host'"
    else:
        outside_dir = f"/var/lib/clickhouse/iceberg_outside_user_files_{get_uuid_str()}"
        base_path = _rewrite_paths_to_local_uri(started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=outside_dir)
        expected_error = "outside of allowed `user_files` path"

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    assert expected_error in instance.query_and_get_error(f"SELECT count() FROM {TABLE_NAME}")

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")
