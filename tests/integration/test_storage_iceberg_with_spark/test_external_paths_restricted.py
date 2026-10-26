import json
import os
import shutil

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import LocalUploader, S3Downloader, S3Uploader, prepare_s3_bucket
from helpers.spark_tools import ResilientSparkSession
from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .conftest import get_spark
from .external_paths_utils import (
    create_and_upload_table,
    ALL_ROWS,
    _create_iceberg_s3_table,
    external_bucket,
    _download_table_for_relocation,
    _external_data_files_in_another_bucket,
    _external_data_files_in_the_same_bucket,
    _move_files_to_bucket,
    _rewrite_manifests_and_reupload,
    _rewrite_paths_to_local_uri,
    find_files,
    modify_avro_file,
    relocate_data_files_to_bucket,
)


# `iceberg_delete_data_on_drop` and `remote_url_allow_hosts` require a separate cluster.
@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__, with_spark=True)
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_host_filter.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/iceberg_delete_data_on_drop.xml",
                "configs/users.d/allow_server_credentials.xml",
            ],
            with_minio=True,
            stay_alive=True,
        )

        cluster.start()

        prepare_s3_bucket(cluster)

        cluster.spark_session = ResilientSparkSession(lambda: get_spark(cluster.instances_dir))
        cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
        cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)
        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "make_external",
    [
        pytest.param(_external_data_files_in_another_bucket, id="another_bucket"),
        pytest.param(_external_data_files_in_the_same_bucket, id="same_bucket"),
    ],
)


def test_delete_data_on_drop_removes_external_files(started_cluster, make_external):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_external_{get_uuid_str()}"
    base_path, external_bucket, external_prefix = make_external(started_cluster, TABLE_NAME)

    def count_objects(bucket, prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS

    assert count_objects(external_bucket, external_prefix) > 0
    assert count_objects(started_cluster.minio_bucket, f"{base_path}/") > 0

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")

    assert count_objects(started_cluster.minio_bucket, f"{base_path}/") == 0
    assert count_objects(external_bucket, external_prefix) == 0


def test_delete_data_on_drop_uses_configured_metadata_head(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_stale_head_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    create_and_upload_table(started_cluster, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(started_cluster, TABLE_NAME, base_bucket, prefix=external_prefix)

    temp_dir, host_path, _ = _download_table_for_relocation(started_cluster, TABLE_NAME)
    metadata_files = sorted(find_files(os.path.join(host_path, "metadata"), ".metadata.json"))
    head = os.path.basename(metadata_files[-1])
    # An uncommitted higher version must not hide the configured head during deletion.
    started_cluster.default_s3_uploader.upload_file(
        metadata_files[0], f"{base_path}/metadata/99999-{get_uuid_str()}.metadata.json")
    shutil.rmtree(temp_dir)

    def count_objects(prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(base_bucket, prefix=prefix, recursive=True))

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(
        f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args}) "
        f"SETTINGS iceberg_metadata_file_path = 'metadata/{head}'")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS
    assert count_objects(f"{external_prefix}/") > 0

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")

    assert count_objects(f"{base_path}/") == 0
    assert count_objects(f"{external_prefix}/") == 0


def test_delete_data_on_drop_requires_write_grant_on_external_files(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_external_grant_{get_uuid_str()}"
    user = f"user_{TABLE_NAME}"
    base_path, bucket, external_prefix = _external_data_files_in_the_same_bucket(started_cluster, TABLE_NAME)
    external_uri_regex = f"s3a://{bucket}/{external_prefix}.*"

    def count_objects(prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT DROP TABLE ON {TABLE_NAME} TO {user}")
    instance.query(f"GRANT READ ON S3('{external_uri_regex}') TO {user}")

    error = instance.query_and_get_error(f"DROP TABLE {TABLE_NAME} SYNC", user=user)
    assert "ACCESS_DENIED" in error, error
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS

    instance.query(f"REVOKE READ ON S3('{external_uri_regex}') FROM {user}")
    instance.query(f"GRANT WRITE ON S3('{external_uri_regex}') TO {user}")
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC", user=user)

    assert count_objects(f"{base_path}/") == 0
    assert count_objects(external_prefix) == 0

    instance.query(f"DROP USER {user}")


def test_delete_data_on_drop_keeps_server_credentials_restriction(started_cluster):
    # The default profile allows server credentials, but the dropping user may not use them for the external endpoint.
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_server_creds_{get_uuid_str()}"
    user = f"user_{TABLE_NAME}"
    data_bucket = external_bucket(started_cluster)
    endpoint = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"

    create_and_upload_table(started_cluster, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(started_cluster, TABLE_NAME, data_bucket, endpoint=endpoint)
    external_prefix = f"{base_path}/data/"

    def count_objects(bucket, prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)
    assert count_objects(data_bucket, external_prefix) > 0

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user} SETTINGS s3_allow_server_credentials_in_user_queries = 0")
    instance.query(f"GRANT DROP TABLE ON {TABLE_NAME} TO {user}")
    instance.query(f"GRANT WRITE ON S3('{endpoint}/{data_bucket}/{external_prefix}.*') TO {user}")
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC", user=user)

    assert count_objects(started_cluster.minio_bucket, f"{base_path}/") == 0
    assert count_objects(data_bucket, external_prefix) > 0
    log = instance.grep_in_log("not allowed to use the server's own credentials")
    assert any(base_path in line and "Cannot enumerate the files" in line for line in log.splitlines()), log

    instance.query(f"DROP USER {user}")


def test_delete_data_on_drop_keeps_credentials_propagation(started_cluster):
    # The external endpoint is reachable only with the table's credentials, which the dropping session propagates.
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_propagate_creds_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster)
    endpoint = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"

    create_and_upload_table(started_cluster, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(started_cluster, TABLE_NAME, data_bucket, endpoint=endpoint)
    external_prefix = f"{base_path}/data/"

    def count_objects(bucket, prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)
    settings = {"object_storage_propagate_credentials_to_other_storages": 1}
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id", settings=settings) == ALL_ROWS
    assert count_objects(data_bucket, external_prefix) > 0

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC", settings=settings, timeout=120)

    assert count_objects(started_cluster.minio_bucket, f"{base_path}/") == 0
    assert count_objects(data_bucket, external_prefix) == 0


def test_delete_data_on_drop_write_grant_does_not_normalize_dot_segments(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_dot_segment_grant_{get_uuid_str()}"
    user = f"user_{TABLE_NAME}"
    bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    create_and_upload_table(started_cluster, TABLE_NAME)
    # The key `outside/../granted/<name>` is distinct from `granted/<name>`.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path,
        lambda p: f"s3a://{bucket}/{external_prefix}/outside/../granted/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)
    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT DROP TABLE ON {TABLE_NAME} TO {user}")
    instance.query(f"GRANT WRITE ON S3('s3a://{bucket}/{external_prefix}/granted/.*') TO {user}")

    error = instance.query_and_get_error(f"DROP TABLE {TABLE_NAME} SYNC", user=user)
    assert "ACCESS_DENIED" in error, error

    instance.query(f"DROP USER {user}")


def test_delete_data_on_drop_requires_write_grant_on_scheme_less_paths(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_drop_scheme_less_grant_{get_uuid_str()}"
    user = f"user_{TABLE_NAME}"
    bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"
    external_uri_regex = f"s3://{bucket}/{external_prefix}/.*"

    create_and_upload_table(started_cluster, TABLE_NAME)
    # A rooted path without a scheme names a key of the table's bucket, here outside the table directory.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path, lambda p: f"/{external_prefix}/{os.path.basename(p)}")
    _move_files_to_bucket(
        started_cluster, find_files(os.path.join(host_path, "data"), ".parquet"), bucket, host_path, base_path,
        external_prefix)
    shutil.rmtree(temp_dir)
    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    def count_objects(prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT DROP TABLE ON {TABLE_NAME} TO {user}")

    error = instance.query_and_get_error(f"DROP TABLE {TABLE_NAME} SYNC", user=user)
    assert "ACCESS_DENIED" in error, error
    assert count_objects(f"{external_prefix}/") > 0

    instance.query(f"GRANT WRITE ON S3('{external_uri_regex}') TO {user}")
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC", user=user)

    assert count_objects(f"{base_path}/") == 0
    assert count_objects(f"{external_prefix}/") == 0

    instance.query(f"DROP USER {user}")


def test_delete_data_on_drop_keeps_file_named_by_absolute_suffix_of_location(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_drop_absolute_suffix_{get_uuid_str()}"
    user = f"user_{TABLE_NAME}"
    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    outside_dir = f"/var/lib/clickhouse/user_files/outside_{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    # `file://<location>//<absolute path>` names a file below the table directory, not the absolute path.
    metadata_dir = os.path.join(host_path, "metadata")
    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json) as f:
            metadata = json.load(f)
        metadata["location"] = f"file://{host_path}"
        with open(metadata_json, "w") as f:
            json.dump(metadata, f)
    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        modify_avro_file(
            manifest, ["data_file", "file_path"], lambda p: f"file://{host_path}/{outside_dir}/{os.path.basename(p)}")

    default_upload_directory(started_cluster, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    instance.exec_in_container(["bash", "-c", f"mkdir -p {outside_dir} && mv {host_path}/data/*.parquet {outside_dir}/"])
    outside_files = instance.exec_in_container(["bash", "-c", f"ls {outside_dir}"]).split()
    assert outside_files

    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergLocal(local, path = '{host_path}', format=Parquet)")

    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT DROP TABLE ON {TABLE_NAME} TO {user}")
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC", user=user)

    assert instance.exec_in_container(["bash", "-c", f"ls {outside_dir}"]).split() == outside_files

    instance.query(f"DROP USER {user}")


def test_file_uri_with_remote_authority_is_rejected(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_file_remote_authority_{get_uuid_str()}"

    create_and_upload_table(started_cluster, TABLE_NAME)
    # Keep the local files present so failure proves path rejection, not a missing file.
    base_path = _rewrite_paths_to_local_uri(started_cluster, TABLE_NAME, "other-host")

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME} ORDER BY id")
    assert "refers to host 'other-host'" in error
    assert "refers to host 'other-host'" in instance.query_and_get_error(f"SELECT count() FROM {TABLE_NAME}")

    # A user without the source grant must not learn the external path from the rejection.
    user = f"user_{TABLE_NAME}"
    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT SELECT ON {TABLE_NAME} TO {user}")
    for query in [f"SELECT * FROM {TABLE_NAME} ORDER BY id", f"SELECT count() FROM {TABLE_NAME}"]:
        error = instance.query_and_get_error(query, user=user)
        assert "ACCESS_DENIED" in error, error
        assert "other-host" not in error, error
    instance.query(f"DROP USER {user}")

    paths = instance.query(
        f"SELECT file_path FROM system.iceberg_files "
        f"WHERE database = 'default' AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY file_path").split()
    assert paths, "The unreadable table was dropped from system.iceberg_files"
    assert all(p.startswith("file://other-host/") for p in paths), paths

    # Permanent path rejection must not make `DatabaseCatalog` retry this drop forever.
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


def test_external_endpoint_is_rejected_by_remote_host_filter(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_host_filter_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    create_and_upload_table(started_cluster, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path,
        lambda p: f"http://not-allowed-host:{started_cluster.minio_port}/{base_bucket}/{base_path}/data/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"

    error = instance.query_and_get_error(f"SELECT * FROM icebergS3({args})")
    assert "not allowed in configuration file" in error, error
    assert "not-allowed-host" in error, error

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    paths = instance.query(
        f"SELECT file_path FROM system.iceberg_files "
        f"WHERE database = 'default' AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY file_path").split()
    assert paths, "The unreadable table was dropped from system.iceberg_files"
    assert all(p.startswith("http://not-allowed-host:") for p in paths), paths

    # Permanent path rejection must not make `DatabaseCatalog` retry this drop forever.
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


def test_external_azure_endpoint_is_derived_from_authority(started_cluster):
    instance = started_cluster.instances["node1"]

    TABLE_NAME = f"test_azure_endpoint_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    create_and_upload_table(started_cluster, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path,
        lambda p: f"abfss://container@someaccount.dfs.core.windows.net/{base_path}/data/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"

    # The client must connect to the Blob endpoint of the account named by the path.
    error = instance.query_and_get_error(f"SELECT * FROM icebergS3({args})")
    assert "not allowed in configuration file" in error, error
    assert "https://someaccount.blob.core.windows.net" in error, error
