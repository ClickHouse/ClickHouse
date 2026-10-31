
import os
import shutil

import pytest

from helpers.iceberg_utils import get_uuid_str
from .external_paths_utils import (
    ALL_ROWS,
    _create_iceberg_s3_table,
    _distribute_table_components,
    _download_table_for_relocation,
    _rewrite_manifests_and_reupload,
    _rewrite_paths_to_local_uri,
    create_and_upload_table,
    external_bucket,
    get_query_args,
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

    # The metadata-only count must hit the same credential restriction as the scan.
    error = instance.query_and_get_error(f"SELECT count() FROM {table_function}")
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

    # The join estimate from manifest statistics is visible in `EXPLAIN`, which does not run the scan.
    explain_query = (
        f"SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 "
        f"SELECT count() FROM {table_function} AS a JOIN {table_function} AS b ON a.id = b.id) "
        f"WHERE explain LIKE '%Join: %' "
        f"SETTINGS use_iceberg_manifest_statistics = 1, explain_query_plan_default = 'legacy'")
    error = instance.query_and_get_error(explain_query, user=user)
    assert "ACCESS_DENIED" in error, error

    assert instance.query(iceberg_files_query, user=user) == ""

    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{external_prefix}/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS
    assert instance.query(f"SELECT count() FROM {table_function}", user=user) == "3\n"
    assert "[3]" in instance.query(explain_query, user=user)
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


def test_columns_cache_does_not_expose_external_files(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_columns_cache_external_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    target_dir = f"/var/lib/clickhouse/user_files/external_columns_cache/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _rewrite_paths_to_local_uri(
        started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=target_dir)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")
    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    # The admission control of the cache can skip a write, so the files are read several times.
    instance.query("SYSTEM DROP COLUMNS CACHE")
    for _ in range(10):
        for source in (table_function, TABLE_NAME):
            instance.query(
                f"SELECT * FROM {source} FORMAT Null SETTINGS use_columns_cache = 1, "
                f"columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0")

    # The user may see the cached entries of the table and of `S3` files, but cannot read the external local files.
    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT SHOW TABLES, SHOW COLUMNS ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3 TO {user}")
    instance.query(f"GRANT SELECT ON system.columns_cache TO {user}")

    assert instance.query(
        f"SELECT count() FROM system.columns_cache WHERE startsWith(part, '{target_dir}/')", user=user) == "0\n"

    instance.query(f"DROP USER {user}")
    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_external_local_file_grant_rejects_parent_traversal(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_local_file_grant_escape_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    path_root = f"/var/lib/clickhouse/user_files/external data/{TABLE_NAME}"
    granted_dir = f"{path_root}/granted"
    target_dir = f"{granted_dir}/../outside"
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
    instance.query(f"GRANT READ ON FILE('{granted_dir}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"GRANT READ ON FILE('{path_root}/outside/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS

    instance.query(f"DROP USER {user}")


def test_external_local_file_grant_rejects_symlink_alias(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_local_file_grant_symlink_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    path_root = f"/var/lib/clickhouse/user_files/external data/{TABLE_NAME}"
    granted_dir = f"{path_root}/granted"
    user = f"user_{TABLE_NAME}"

    instance.exec_in_container(["mkdir", "-p", f"{path_root}/outside", granted_dir])
    instance.exec_in_container(["ln", "-s", "../outside", f"{granted_dir}/link"])

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = _rewrite_paths_to_local_uri(
        started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=f"{granted_dir}/link")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3 TO {user}")
    instance.query(f"GRANT READ ON FILE('{granted_dir}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"GRANT READ ON FILE('{path_root}/outside/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS

    instance.query(f"DROP USER {user}")


def test_percent_escape_in_external_key_is_not_decoded_for_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_percent_escape_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    granted_prefix = f"external_data/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    # The object key contains a literal `%2F`, so it is outside `granted_prefix/`.
    base_path = relocate_data_files_to_bucket(
        started_cluster_iceberg_with_spark, TABLE_NAME, base_bucket, prefix=f"{granted_prefix}%2Fsecret")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3('{minio_url}/{base_bucket}/{base_path}/.*') TO {user}")
    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{granted_prefix}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/external_data/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS

    instance.query(f"DROP USER {user}")


def test_dot_segments_in_external_key_are_not_normalized_for_grant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_key_segments_grant_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    # The key `outside/../granted/<name>` is distinct from `granted/<name>`; the grant is checked before the object is opened.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"s3a://{base_bucket}/{external_prefix}/outside/../granted/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3('{minio_url}/{base_bucket}/{base_path}/.*') TO {user}")
    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{external_prefix}/granted/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"DROP USER {user}")


@pytest.mark.parametrize("external_component", ["manifest_list", "manifest"])
def test_cached_external_metadata_requires_source_grant(started_cluster_iceberg_with_spark, external_component):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_cached_metadata_grant_{external_component}_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    other_bucket = external_bucket(started_cluster_iceberg_with_spark)
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    other = f"s3:{other_bucket}"
    base_path = _distribute_table_components(
        started_cluster_iceberg_with_spark, TABLE_NAME,
        metadata_storage="s3",
        manifest_list_storage=other if external_component == "manifest_list" else "s3",
        manifest_storage=other if external_component == "manifest" else "s3",
        data_storage="s3")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3('{minio_url}/{base_bucket}/{base_path}/.*') TO {user}")
    instance.query(f"GRANT READ ON S3('s3a://{base_bucket}/{base_path}/.*') TO {user}")

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    # A privileged query puts the external metadata into the shared cache.
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id") == ALL_ROWS

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"GRANT READ ON S3('s3a://{other_bucket}/{base_path}/.*') TO {user}")
    assert instance.query(f"SELECT * FROM {table_function} ORDER BY id", user=user) == ALL_ROWS

    instance.query(f"DROP USER {user}")


def test_external_uri_userinfo_not_in_storage_name(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_userinfo_disk_name_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)
    secret = f"secret_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(
        started_cluster_iceberg_with_spark, TABLE_NAME, data_bucket,
        endpoint=f"http://reader:{secret}@{started_cluster_iceberg_with_spark.minio_ip}:{started_cluster_iceberg_with_spark.minio_port}")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = f"icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster_iceberg_with_spark.minio_bucket}/')"

    # The storage name is reported in errors of the external storage.
    error = instance.query_and_get_error(
        f"SELECT * FROM {table_function} ORDER BY id "
        f"SETTINGS object_storage_propagate_credentials_to_other_storages = 1, "
        f"s3_allow_server_credentials_in_user_queries = 1")
    assert "on disk 'iceberg_external_" in error, error
    assert secret not in error, error


def test_missing_external_local_file_does_not_create_directories(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_local_missing_dir_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    missing_root = f"/var/lib/clickhouse/user_files/external_missing_{TABLE_NAME}"
    user = f"user_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _rewrite_paths_to_local_uri(
        started_cluster_iceberg_with_spark, TABLE_NAME, "", target_dir=f"{missing_root}/new/tree")
    instance.exec_in_container(["rm", "-rf", missing_root])

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = (f"icebergS3('{minio_url}/{base_bucket}/{base_path}/', "
                      f"'minio', 'ClickHouse_Minio_P@ssw0rd')")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    instance.query(f"GRANT READ ON S3 TO {user}")
    instance.query(f"GRANT READ ON FILE('{missing_root}/.*') TO {user}")

    instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY id", user=user)
    assert instance.exec_in_container(["bash", "-c", f"test -e {missing_root} && echo exists || echo absent"]).strip() == "absent"

    instance.query(f"DROP USER {user}")


def test_background_prefetch_does_not_read_external_manifests(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_background_prefetch_external_{get_uuid_str()}"
    other_bucket = external_bucket(started_cluster_iceberg_with_spark)

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _distribute_table_components(
        started_cluster_iceberg_with_spark, TABLE_NAME,
        metadata_storage="s3", manifest_list_storage="s3", manifest_storage=f"s3:{other_bucket}", data_storage="s3")

    args = get_query_args("s3", started_cluster_iceberg_with_spark, base_path)
    instance.query(
        f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args}) SETTINGS iceberg_metadata_async_prefetch_period_ms = 100")

    # The second prefetch after the cache is dropped starts with an empty cache.
    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")
    instance.wait_for_log_line(
        f"backgroundMetadataPrefetcherThread: interval=.*{TABLE_NAME}", repetitions=2, look_behind_lines=0)

    log_comment = f"{TABLE_NAME}_select"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id", settings={"log_comment": log_comment}) == ALL_ROWS
    instance.query("SYSTEM FLUSH LOGS query_log")
    manifests_read = instance.query(
        f"SELECT ProfileEvents['IcebergManifestFilesRead'] FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND log_comment = '{log_comment}'")
    assert int(manifests_read) > 0, manifests_read

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_cached_external_manifest_requires_same_credentials_policy(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_manifest_cache_credentials_{get_uuid_str()}"
    other_bucket = external_bucket(started_cluster_iceberg_with_spark)

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    # An endpoint other than the table's, which gets the table's credentials only when they are propagated.
    base_path = _distribute_table_components(
        started_cluster_iceberg_with_spark, TABLE_NAME,
        metadata_storage="s3", manifest_list_storage="s3", manifest_storage=f"ip:{other_bucket}", data_storage="s3")

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    table_function = f"icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster_iceberg_with_spark.minio_bucket}/')"
    query = f"SELECT * FROM {table_function} ORDER BY id SETTINGS s3_allow_server_credentials_in_user_queries = 1"

    cold_error = instance.query_and_get_error(query)
    assert "Access Denied" in cold_error, cold_error

    # A query that propagates the credentials puts the manifest into the shared cache.
    assert instance.query(f"{query}, object_storage_propagate_credentials_to_other_storages = 1") == ALL_ROWS

    error = instance.query_and_get_error(query)
    assert "Access Denied" in error, error
