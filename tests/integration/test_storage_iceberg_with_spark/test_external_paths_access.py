
from helpers.iceberg_utils import get_uuid_str
from .external_paths_utils import (
    ALL_ROWS,
    _create_iceberg_s3_table,
    _rewrite_paths_to_local_uri,
    create_and_upload_table,
    external_bucket,
    relocate_data_files_to_bucket,
)


def test_propagate_credentials_to_other_endpoint(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_propagate_creds_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(
        started_cluster_iceberg_with_spark, TABLE_NAME, data_bucket,
        endpoint=f"http://{started_cluster_iceberg_with_spark.minio_ip}:{started_cluster_iceberg_with_spark.minio_port}")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = f"icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster_iceberg_with_spark.minio_bucket}/')"

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id")
    assert "not allowed to use the server's own credentials" in error, error

    error = instance.query_and_get_error(
        f"SELECT * FROM {table_function} ORDER BY id "
        f"SETTINGS object_storage_propagate_credentials_to_other_storages = 1")
    assert "not allowed to use the server's own credentials" in error, error

    result = instance.query(
        f"SELECT * FROM {table_function} ORDER BY id "
        f"SETTINGS object_storage_propagate_credentials_to_other_storages = 1, "
        f"s3_allow_server_credentials_in_user_queries = 1")
    assert result == ALL_ROWS


def test_same_bucket_external_path_requires_source_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_same_bucket_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(started_cluster_iceberg_with_spark, TABLE_NAME, base_bucket, prefix=external_prefix)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")
    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    iceberg_files_query = (
        f"SELECT file_path FROM system.iceberg_files "
        f"WHERE database = 'default' AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY file_path")
    expected_paths = instance.query(iceberg_files_query)
    assert expected_paths

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT SHOW TABLES ON {TABLE_NAME} TO {user}")
    instance.query(f"GRANT SELECT ON system.iceberg_files TO {user}")
    instance.query(f"GRANT READ ON S3('{minio_url}/{base_bucket}/{base_path}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    error = instance.query_and_get_error(f"SELECT count() FROM {table_function}", user=user)
    assert "ACCESS_DENIED" in error, error

    assert instance.query(iceberg_files_query, user=user) == ""

    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{external_prefix}/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS
    assert instance.query(f"SELECT count() FROM {table_function}", user=user) == "3\n"
    assert instance.query(iceberg_files_query, user=user) == expected_paths

    instance.query(f"DROP USER {user}")
    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_external_local_file_requires_file_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_local_file_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    target_dir = f"/var/lib/clickhouse/user_files/external data/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = _rewrite_paths_to_local_uri(
        started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=target_dir)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3 TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"GRANT READ ON FILE('{target_dir}/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS

    instance.query(f"DROP USER {user}")
