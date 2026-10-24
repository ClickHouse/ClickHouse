import os
import json
import shutil
import tempfile
import time
import pytest

from helpers.s3_tools import S3Uploader
from helpers.iceberg_utils import default_download_directory, default_upload_directory, get_uuid_str
from .external_paths_utils import (
    ALL_ROWS,
    create_and_upload_table,
    _create_iceberg_s3_table,
    _distribute_table_components,
    _download_table_for_relocation,
    _external_delete_files_in_another_bucket,
    _move_files_to_bucket,
    _rewrite_manifests_and_reupload,
    external_bucket,
    find_files,
    get_absolute_path,
    get_query_args,
    get_uploader,
    modify_avro_file,
    relocate_data_files_to_bucket,
)


@pytest.mark.parametrize("spelling", ["s3", "url"])
def test_four_different_s3_buckets(started_cluster_iceberg_with_spark, spelling):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_four_buckets_{spelling}_{get_uuid_str()}"
    buckets = [
        started_cluster_iceberg_with_spark.minio_bucket,
        external_bucket(started_cluster_iceberg_with_spark, 1),
        external_bucket(started_cluster_iceberg_with_spark, 2),
        external_bucket(started_cluster_iceberg_with_spark, 3),
    ]

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, name STRING, score INT) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Carol', 92)")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = _distribute_table_components(started_cluster_iceberg_with_spark, TABLE_NAME, f"s3:{buckets[0]}",
                                             *(f"{spelling}:{b}" for b in buckets[1:]))

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    result = instance.query(f"SELECT * FROM icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{buckets[0]}/') ORDER BY id")

    assert result == "1\tAlice\t100\n2\tBob\t85\n3\tCarol\t92\n"


def test_num_rows_cache_no_collision_across_buckets(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket
    shared_key = f"shared_count_cache_{get_uuid_str()}/data/part-0.parquet"

    def prepare_table(table_name, values_sql, data_bucket):
        spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
        spark.sql(f"INSERT INTO {table_name} VALUES {values_sql}")

        default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")

        temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, table_name)
        metadata_dir = os.path.join(host_path, "metadata")
        data_dir = os.path.join(host_path, "data")

        data_files = find_files(data_dir, ".parquet")
        assert len(data_files) == 1, f"Expected a single data file, got: {data_files}"

        manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
        for mf in manifest_files:
            modify_avro_file(mf, ["data_file", "file_path"], lambda _: f"s3a://{data_bucket}/{shared_key}")
            # Set `record_count` negative and remove summary counts to force the file-count cache path.
            modify_avro_file(mf, ["data_file", "value_counts"], lambda _: None)
            modify_avro_file(mf, ["data_file", "record_count"], lambda _: -1)

        for mj in find_files(metadata_dir, ".metadata.json"):
            with open(mj, 'r') as f:
                data = json.load(f)
            for snap in data.get("snapshots", []):
                snap.get("summary", {}).pop("total-records", None)
            with open(mj, 'w') as f:
                json.dump(data, f, indent=2)

        for f in manifest_files + find_files(metadata_dir, ".metadata.json"):
            rel = os.path.relpath(f, host_path)
            started_cluster_iceberg_with_spark.default_s3_uploader.upload_file(f, f"{base_path}/{rel}")

        S3Uploader(started_cluster_iceberg_with_spark.minio_client, data_bucket).upload_file(data_files[0], shared_key)

        shutil.rmtree(temp_dir)
        return base_path

    # Upload both files before populating the cache: entries are reused only for older files.
    base_path_a = prepare_table(
        f"test_count_cache_a_{get_uuid_str()}", "(1, 'a'), (2, 'b'), (3, 'c')", external_bucket(started_cluster_iceberg_with_spark, 1)
    )
    base_path_b = prepare_table(
        f"test_count_cache_b_{get_uuid_str()}", "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", external_bucket(started_cluster_iceberg_with_spark, 2)
    )
    # Allow for the second-resolution `last_modified` comparison.
    time.sleep(3)

    def count(base_path, marker):
        result = instance.query(
            f"SELECT /* {marker} */ count() FROM icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/') "
            "SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1, use_cache_for_count_from_files = 1"
        ).strip()
        instance.query("SYSTEM FLUSH LOGS")
        cache_lookups = int(instance.query(
            "SELECT ProfileEvents['SchemaInferenceCacheHits'] + ProfileEvents['SchemaInferenceCacheMisses'] "
            f"FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE '%{marker}%' AND query NOT LIKE '%query_log%' "
            "ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip())
        return result, cache_lookups

    count_a, cache_lookups_a = count(base_path_a, "count_cache_marker_a")
    count_b, cache_lookups_b = count(base_path_b, "count_cache_marker_b")
    assert count_a == "3"
    assert count_b == "5"
    assert cache_lookups_a >= 1
    assert cache_lookups_b >= 1


def test_external_path_virtual_column_filter(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_path_filter_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = relocate_data_files_to_bucket(started_cluster_iceberg_with_spark, TABLE_NAME, data_bucket)

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster_iceberg_with_spark.minio_bucket}/'"

    paths = instance.query(f"SELECT DISTINCT _path FROM icebergS3({args})").strip().splitlines()
    assert len(paths) == 1
    external_path = paths[0]
    assert data_bucket in external_path

    assert instance.query(
        f"SELECT count() FROM icebergS3Cluster('cluster_simple', {args}) WHERE _path = '{external_path}' "
        "SETTINGS cluster_table_function_split_granularity = 'bucket'"
    ).strip() == "3"


def test_gs_path_is_not_served_by_same_named_s3_bucket(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_gs_same_bucket_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"gs://{started_cluster_iceberg_with_spark.minio_bucket}/{base_path}/data/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    # GCS is unreachable here; a successful read would mean the MinIO bucket was used incorrectly.
    error = instance.query_and_get_error(
        f"SELECT * FROM {TABLE_NAME} ORDER BY id "
        f"SETTINGS s3_request_timeout_ms = 3000, s3_connect_timeout_ms = 3000")
    assert "alpha" not in error

    instance.query(f"DETACH TABLE {TABLE_NAME}")


def test_versioned_s3_path_is_rejected(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_versioned_s3_path_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)
    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"{minio_url}/{data_bucket}/{base_path}/data/{os.path.basename(p)}?versionId=v1")
    shutil.rmtree(temp_dir)

    args = get_query_args("s3", started_cluster_iceberg_with_spark, base_path)
    error = instance.query_and_get_error(f"SELECT * FROM icebergS3({args}) ORDER BY id")
    assert "reading S3 object versions is not supported" in error, error


def test_unsupported_scheme_is_still_reported_by_system_iceberg_files(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_unsupported_scheme_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"viewfs://cluster/{base_path}/data/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    assert "Unsupported storage scheme" in instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME} ORDER BY id")

    paths = instance.query(
        f"SELECT file_path FROM system.iceberg_files "
        f"WHERE database = 'default' AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY file_path").split()
    assert paths, "The unreadable table was dropped from system.iceberg_files"
    assert all(p.startswith("viewfs://cluster/") for p in paths), paths

    assert "Unsupported storage scheme" in instance.query_and_get_error(f"SELECT count() FROM {TABLE_NAME}")

    instance.query(f"DETACH TABLE {TABLE_NAME}")


def test_cluster_function_reads_external_delete_file(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_cluster_external_delete_{get_uuid_str()}"
    base_path, _, _ = _external_delete_files_in_another_bucket(started_cluster_iceberg_with_spark, TABLE_NAME)

    args = get_query_args("s3", started_cluster_iceberg_with_spark, base_path)
    assert instance.query(f"SELECT * FROM icebergS3Cluster('cluster_simple', {args}) ORDER BY id") == "1\talpha\n3\tgamma\n"


@pytest.mark.parametrize("metadata_storage,manifest_list_storage,manifest_storage,data_storage", [
    ("s3", "local", "s3", "s3"),
    ("s3", "s3", "local", "s3"),
    ("azure", "azure", "azure", "local"),
])
def test_multi_storage_combinations(started_cluster_iceberg_with_spark, metadata_storage, manifest_list_storage, manifest_storage, data_storage):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_combo_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = _distribute_table_components(started_cluster_iceberg_with_spark, TABLE_NAME, metadata_storage,
                                             manifest_list_storage, manifest_storage, data_storage)

    func = {"s3": "icebergS3", "azure": "icebergAzure", "local": "icebergLocal"}[metadata_storage.split(":")[0]]
    args = get_query_args(metadata_storage, started_cluster_iceberg_with_spark, base_path)

    assert instance.query(f"SELECT * FROM {func}({args}) ORDER BY id") == ALL_ROWS


def test_lazy_materialization_rereads_external_data_file(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_lazy_external_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = relocate_data_files_to_bucket(
        started_cluster_iceberg_with_spark, TABLE_NAME, external_bucket(started_cluster_iceberg_with_spark))
    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    # Without ETag pinning, the lazy pass probes the metadata of the data file in its own bucket.
    query = f"SELECT value FROM {TABLE_NAME} ORDER BY id LIMIT 1"
    settings = {
        "enable_analyzer": 1,
        "query_plan_optimize_lazy_materialization": 1,
        "query_plan_optimize_lazy_materialization_for_object_storage": 1,
        "s3_validate_etag_on_read": 0,
    }
    assert "LazilyReadFromObjectStorage" in instance.query(f"EXPLAIN {query}", settings=settings)
    assert instance.query(query, settings=settings) == "alpha\n"

    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_file_filter_uses_resolved_file_name(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_file_filter_resolved_name_{get_uuid_str()}"
    data_bucket = external_bucket(started_cluster_iceberg_with_spark)
    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    # The query part is not part of the resolved key, so `_file` does not contain it.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"{minio_url}/{data_bucket}/{base_path}/data/{os.path.basename(p)}?X-Amz-Date=20261014T000000Z")
    _move_files_to_bucket(
        started_cluster_iceberg_with_spark, find_files(os.path.join(host_path, "data"), ".parquet"),
        data_bucket, host_path, base_path)
    shutil.rmtree(temp_dir)

    args = get_query_args("s3", started_cluster_iceberg_with_spark, base_path)
    file_names = instance.query(f"SELECT DISTINCT _file FROM icebergS3({args})").strip().splitlines()
    assert file_names and all("?" not in name for name in file_names), file_names

    in_list = ", ".join(f"'{name}'" for name in file_names)
    assert instance.query(f"SELECT count() FROM icebergS3({args}) WHERE _file IN ({in_list})").strip() == "3"


def test_dot_segments_in_s3_key_are_literal(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_dot_segments_key_{get_uuid_str()}"
    bucket = started_cluster_iceberg_with_spark.minio_bucket

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    for mj in find_files(os.path.join(host_path, "metadata"), ".metadata.json"):
        with open(mj) as f:
            data = json.load(f)
        data["location"] = f"s3a://{bucket}/{base_path}"
        with open(mj, "w") as f:
            json.dump(data, f)
        started_cluster_iceberg_with_spark.default_s3_uploader.upload_file(mj, f"{base_path}/{os.path.relpath(mj, host_path)}")

    # `..` is a literal key segment, so these keys do not name the data files still present inside `location`.
    _rewrite_manifests_and_reupload(
        started_cluster_iceberg_with_spark, host_path, base_path,
        lambda p: f"s3a://{bucket}/other/../{base_path}/data/{os.path.basename(p)}")
    shutil.rmtree(temp_dir)

    args = get_query_args("s3", started_cluster_iceberg_with_spark, base_path)
    error = instance.query_and_get_error(f"SELECT * FROM icebergS3({args}) ORDER BY id")
    assert "alpha" not in error, error


def test_insert_into_write_data_path_with_other_s3_spelling(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_insert_write_data_path_{get_uuid_str()}"
    bucket = started_cluster_iceberg_with_spark.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    # `location` is spelled with `s3a`, while `write.data.path` points outside it in the same bucket with `s3`.
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    metadata_dir = os.path.join(host_path, "metadata")
    for mf in find_files(metadata_dir, ".avro"):
        if os.path.basename(mf).startswith("snap-"):
            modify_avro_file(mf, ["manifest_path"],
                             lambda p: f"s3a://{bucket}/{base_path}/metadata/{os.path.basename(p)}")
        else:
            modify_avro_file(mf, ["data_file", "file_path"],
                             lambda p: f"s3a://{bucket}/{base_path}/data/{os.path.basename(p)}")
    for mj in find_files(metadata_dir, ".metadata.json"):
        with open(mj) as f:
            data = json.load(f)
        data["location"] = f"s3a://{bucket}/{base_path}"
        data.setdefault("properties", {})["write.data.path"] = f"s3://{bucket}/{external_prefix}"
        for snap in data.get("snapshots", []):
            snap["manifest-list"] = f"s3a://{bucket}/{base_path}/metadata/{os.path.basename(snap['manifest-list'])}"
        with open(mj, "w") as f:
            json.dump(data, f)
    for f in find_files(metadata_dir, ""):
        started_cluster_iceberg_with_spark.default_s3_uploader.upload_file(f, f"{base_path}/{os.path.relpath(f, host_path)}")
    shutil.rmtree(temp_dir)

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (4, 'delta')", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS + "4\tdelta\n"

    # Writing to `write.data.path` requires the `WRITE` grant on it, not `READ`.
    user = f"user_{TABLE_NAME}"
    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user}")
    instance.query(f"GRANT INSERT ON {TABLE_NAME} TO {user}")
    instance.query(f"GRANT WRITE ON S3('s3://{bucket}/{external_prefix}/.*') TO {user}")
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (5, 'epsilon')", user=user, settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == ALL_ROWS + "4\tdelta\n5\tepsilon\n"

    written = [
        obj.object_name
        for obj in started_cluster_iceberg_with_spark.minio_client.list_objects(bucket, prefix=f"{external_prefix}/", recursive=True)
    ]
    assert len(written) == 2 and all(name.endswith(".parquet") for name in written), written

    instance.query(f"DROP USER {user}")
    instance.query(f"DROP TABLE {TABLE_NAME}")


def test_azure_endpoint_of_other_cloud_with_same_account_is_rejected(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    account = started_cluster_iceberg_with_spark.azurite_account

    TABLE_NAME = f"test_azure_other_cloud_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    base_path = _distribute_table_components(started_cluster_iceberg_with_spark, TABLE_NAME, "azure", "azure", "azure", "azure")
    args = get_query_args("azure", started_cluster_iceberg_with_spark, base_path)
    assert instance.query(f"SELECT * FROM icebergAzure({args}) ORDER BY id") == ALL_ROWS

    # The same account name in the endpoint of another cloud names other objects than the base endpoint.
    temp_dir = tempfile.mkdtemp()
    default_download_directory(started_cluster_iceberg_with_spark, "azure", f"{base_path}/metadata/", temp_dir)
    for mf in find_files(temp_dir, ".avro"):
        if os.path.basename(mf).startswith("snap-"):
            continue
        modify_avro_file(mf, ["data_file", "file_path"],
                         lambda p: p.replace(f"@{account}/", f"@{account}.dfs.core.usgovcloudapi.net/"))
        get_uploader("azure", started_cluster_iceberg_with_spark).upload_file(
            mf, f"{base_path}/metadata/{os.path.relpath(mf, temp_dir)}")
    shutil.rmtree(temp_dir)

    error = instance.query_and_get_error(
        f"SELECT * FROM icebergAzure({args}) ORDER BY id", settings={"use_iceberg_metadata_files_cache": 0})
    assert "differs from the table's base endpoint" in error, error


def test_copied_azure_table_reads_external_path_from_its_container(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_copied_azure_table_{get_uuid_str()}"
    copy_container = f"copy-{get_uuid_str().replace('_', '-')}"
    external_prefix = f"external_{TABLE_NAME}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    metadata_dir = os.path.join(host_path, "metadata")

    # The copy in another container keeps the source `location`; the data files are outside it, in the source container.
    source_location = get_absolute_path("azure", started_cluster_iceberg_with_spark, base_path)
    external_location = get_absolute_path("azure", started_cluster_iceberg_with_spark, external_prefix)
    for avro_file in find_files(metadata_dir, ".avro"):
        if os.path.basename(avro_file).startswith("snap-"):
            modify_avro_file(avro_file, ["manifest_path"], lambda p: f"{source_location}/metadata/{os.path.basename(p)}")
        else:
            modify_avro_file(avro_file, ["data_file", "file_path"], lambda p: f"{external_location}/{os.path.basename(p)}")
    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json) as f:
            metadata = json.load(f)
        metadata["location"] = source_location
        for snapshot in metadata.get("snapshots", []):
            snapshot["manifest-list"] = f"{source_location}/metadata/{os.path.basename(snapshot['manifest-list'])}"
        with open(metadata_json, "w") as f:
            json.dump(metadata, f)

    started_cluster_iceberg_with_spark.blob_service_client.create_container(copy_container)
    copy_uploader = get_uploader(f"azure:{copy_container}", started_cluster_iceberg_with_spark)
    for metadata_file in find_files(metadata_dir, ""):
        copy_uploader.upload_file(metadata_file, f"{base_path}/{os.path.relpath(metadata_file, host_path)}")
    for data_file in find_files(os.path.join(host_path, "data"), ".parquet"):
        started_cluster_iceberg_with_spark.default_azure_uploader.upload_file(data_file, f"{external_prefix}/{os.path.basename(data_file)}")
    shutil.rmtree(temp_dir)

    args = get_query_args(f"azure:{copy_container}", started_cluster_iceberg_with_spark, base_path)
    assert instance.query(f"SELECT * FROM icebergAzure({args}) ORDER BY id") == ALL_ROWS
