import os
import shutil

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import LocalUploader, S3Downloader, S3Uploader, prepare_s3_bucket
from helpers.spark_tools import ResilientSparkSession
from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .conftest import get_spark
from .external_paths_utils import (
    ALL_ROWS,
    _create_iceberg_s3_table,
    _download_table_for_relocation,
    _external_data_files_in_another_bucket,
    _external_data_files_in_the_same_bucket,
    _rewrite_manifests_and_reupload,
    _rewrite_paths_to_local_uri,
    find_files,
    relocate_data_files_within_base_bucket,
)


# A cluster of its own: `iceberg_delete_data_on_drop` is read from the default profile and
# `remote_url_allow_hosts` is server-level, so neither can be set without changing what every other test
# in the package sees. `get_spark` comes from the package conftest because `getOrCreate` hands back
# whichever session the worker built first.
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


# `IcebergMetadata::drop` walks the current metadata graph, so it deletes data files outside the table
# directory as well -- in another bucket, or under an absolute URI elsewhere in the same one.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3621619550
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3632505967
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


# The drop enumerates the files to delete from the configured `iceberg_metadata_file_path`, not from the
# highest `v*.metadata.json` in storage: an interrupted write leaves a higher version whose snapshot does
# not name the head's external files. The catalog cannot be asked here, `StorageObjectStorage::drop`
# removes the table from it first.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3990706553
def test_delete_data_on_drop_uses_configured_metadata_head(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_drop_stale_head_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = relocate_data_files_within_base_bucket(started_cluster, TABLE_NAME, external_prefix)

    temp_dir, host_path, _ = _download_table_for_relocation(started_cluster, TABLE_NAME)
    metadata_files = sorted(find_files(os.path.join(host_path, "metadata"), ".metadata.json"))
    head = os.path.basename(metadata_files[-1])
    # A stale higher version, as an interrupted write leaves behind: it has no snapshot, so it names no
    # data file at all.
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

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    assert count_objects(f"{external_prefix}/") > 0

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")

    assert count_objects(f"{base_path}/") == 0
    assert count_objects(f"{external_prefix}/") == 0


# A `file://` URI is read from the local filesystem of whichever server resolves it, so an authority
# naming another host is rejected rather than serving that server's own file under the path.
# `system.iceberg_files` still reports such a path, which is when it is most needed for diagnosis.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3735408259
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3859370183
def test_file_uri_with_remote_authority_is_rejected(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_file_remote_authority_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    # The files are put in place locally too, so the query fails on the authority, not on a missing file.
    base_path = _rewrite_paths_to_local_uri(started_cluster, TABLE_NAME, "other-host")

    _create_iceberg_s3_table(started_cluster, TABLE_NAME, base_path)

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME} ORDER BY id")
    assert "refers to host 'other-host'" in error

    paths = instance.query(
        f"SELECT file_path FROM system.iceberg_files "
        f"WHERE database = 'default' AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY file_path").split()
    assert paths, "The unreadable table was dropped from system.iceberg_files"
    assert all(p.startswith("file://other-host/") for p in paths), paths

    # The drop hits the poisoned path. That rejection is permanent, so it must be terminal rather than
    # retried by `DatabaseCatalog`, or `DROP TABLE ... SYNC` never returns.
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


# An endpoint named by Iceberg metadata passes `remote_url_allow_hosts` just like one named in a table
# definition, so a manifest cannot reach a host an ordinary `S3` table may not.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3990706589
def test_external_endpoint_is_rejected_by_remote_host_filter(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_host_filter_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    # The files stay where they are: the endpoint is refused before any request is made for them.
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
