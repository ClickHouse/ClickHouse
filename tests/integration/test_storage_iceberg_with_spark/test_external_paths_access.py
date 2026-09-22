import os
import shutil

from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .external_paths_utils import (
    _download_table_for_relocation,
    _relocate_data_files_to_bucket_by_ip,
    _rewrite_manifests_and_reupload,
    external_bucket,
    find_files,
    path_modifier,
    relocate_data_files_within_base_bucket,
)


# `object_storage_propagate_credentials_to_other_storages` hands the base storage's S3 credentials to a
# storage built for a file the metadata places elsewhere, and is honoured only in a session that
# `s3_allow_server_credentials_in_user_queries` already trusts with the server's own credentials.
# Endpoints are compared by scheme and authority, so addressing the same MinIO by IP instead of by host
# name makes the target a different endpoint while the object stays where it is.
def test_propagate_credentials_to_other_endpoint(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_propagate_creds_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = _relocate_data_files_to_bucket_by_ip(started_cluster_iceberg_with_spark, TABLE_NAME, data_bucket)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = f"icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster_iceberg_with_spark.minio_bucket}/')"

    # Another endpoint, so the base credentials do not apply and the read fails closed.
    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id")
    assert "not allowed to use the server's own credentials" in error, error

    # The base identity can be server-managed, so the setting alone changes nothing while the session is
    # fenced off from the server's own credentials.
    # https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3993934228
    error = instance.query_and_get_error(
        f"SELECT * FROM {table_function} ORDER BY id "
        f"SETTINGS object_storage_propagate_credentials_to_other_storages = 1")
    assert "not allowed to use the server's own credentials" in error, error

    result = instance.query(
        f"SELECT * FROM {table_function} ORDER BY id "
        f"SETTINGS object_storage_propagate_credentials_to_other_storages = 1, "
        f"s3_allow_server_credentials_in_user_queries = 1")
    assert result == "1\talpha\n2\tbeta\n3\tgamma\n"


# `ITableFunction::checkSourceAccess` authorizes only the URI the table function names, so a file the
# metadata points at elsewhere -- here elsewhere in the same bucket -- is authorized on its own.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3999373995
def test_same_bucket_external_path_requires_source_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_same_bucket_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = relocate_data_files_within_base_bucket(started_cluster_iceberg_with_spark, TABLE_NAME, external_prefix)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3('{minio_url}/{base_bucket}/{base_path}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    # Counting from the manifests must not answer what a read of the data may not.
    error = instance.query_and_get_error(f"SELECT count() FROM {table_function}", user=user)
    assert "ACCESS_DENIED" in error, error

    # The grant is spelled as the metadata spells the path.
    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{external_prefix}/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == "1\talpha\n2\tbeta\n3\tgamma\n"
    assert instance.query(f"SELECT count() FROM {table_function}", user=user) == "3\n"

    instance.query(f"DROP USER {user}")


# The target need not be on the same source: metadata on `S3` can name a local file, which a
# `READ ON S3` grant must not cover.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3999373995
def test_external_local_file_requires_file_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_local_file_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    user = f"user_{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    # The data files live only on the node's filesystem, so the read has to reach the `File` source.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(started_cluster_iceberg_with_spark, host_path, base_path,
                                    lambda p: path_modifier(p, "local", started_cluster_iceberg_with_spark, base_path))
    for f in find_files(os.path.join(host_path, "data"), ".parquet"):
        rel = os.path.relpath(f, host_path)
        started_cluster_iceberg_with_spark.default_local_uploader.upload_file(f, f"{base_path}/{rel}")
        started_cluster_iceberg_with_spark.minio_client.remove_object(base_bucket, f"{base_path}/{rel}")
    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3 TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    # A `file://` URI and the bare path it decomposes to name the same file.
    instance.query(f"GRANT READ ON FILE('/{base_path}/data/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == "1\talpha\n2\tbeta\n3\tgamma\n"

    instance.query(f"DROP USER {user}")
