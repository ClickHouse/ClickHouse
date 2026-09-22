import os
import json
import shutil
import tempfile
import avro.datafile
import avro.io

from helpers.s3_tools import AzureUploader, S3Uploader
from helpers.iceberg_utils import default_download_directory, default_upload_directory


def modify_avro_file(avro_path: str, field_path: list, modifier_func) -> None:
    with open(avro_path, 'rb') as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        metadata = dict(reader.meta)
        records = list(reader)
        reader.close()

    for record in records:
        obj = record
        for key in field_path[:-1]:
            if obj is None or key not in obj:
                break
            obj = obj[key]
        else:
            if obj and field_path[-1] in obj:
                obj[field_path[-1]] = modifier_func(obj[field_path[-1]])

    with open(avro_path, 'wb') as f:
        writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
        for key, value in metadata.items():
            if not key.startswith('avro.'):
                writer.set_meta(key, value)
        for record in records:
            writer.append(record)
        writer.close()


def get_absolute_path(storage_type: str, cluster, relative_path: str) -> str:
    relative_path = relative_path.lstrip("/")

    if storage_type == "s3":
        return f"s3a://{cluster.minio_bucket}/{relative_path}"
    elif storage_type.startswith("s3:"):
        bucket = storage_type.split(":")[1]
        return f"s3a://{bucket}/{relative_path}"
    elif storage_type.startswith("url:"):
        bucket = storage_type.split(":")[1]
        return f"http://{cluster.minio_host}:{cluster.minio_port}/{bucket}/{relative_path}"
    elif storage_type == "azure":
        return f"abfs://{cluster.azure_container_name}@{cluster.azurite_account}/{relative_path}"
    elif storage_type.startswith("azure:"):
        container = storage_type.split(":")[1]
        return f"abfs://{container}@{cluster.azurite_account}/{relative_path}"
    elif storage_type == "local":
        return f"file:///{relative_path}"
    else:
        raise ValueError(f"Unknown storage type: {storage_type}")


def get_uploader(storage_type: str, cluster):
    if storage_type == "s3":
        return cluster.default_s3_uploader
    elif storage_type.startswith("s3:") or storage_type.startswith("url:"):
        bucket = storage_type.split(":")[1]
        return S3Uploader(cluster.minio_client, bucket)
    elif storage_type == "azure":
        return cluster.default_azure_uploader
    elif storage_type.startswith("azure:"):
        container = storage_type.split(":")[1]
        return AzureUploader(cluster.blob_service_client, container)
    elif storage_type == "local":
        return cluster.default_local_uploader
    else:
        raise ValueError(f"Unknown storage type: {storage_type}")


def get_table_function(metadata_storage: str):
    if metadata_storage == "s3" or metadata_storage.startswith("s3:"):
        return "icebergS3"
    elif metadata_storage == "azure" or metadata_storage.startswith("azure:"):
        return "icebergAzure"
    elif metadata_storage == "local":
        return "icebergLocal"
    else:
        raise ValueError(f"Unknown storage type: {metadata_storage}")


def get_query_args(metadata_storage: str, cluster, table_path: str):
    minio_url = f"http://{cluster.minio_host}:{cluster.minio_port}"
    if metadata_storage == "s3":
        return f"s3, filename='{table_path}/', format=Parquet, url='{minio_url}/{cluster.minio_bucket}/'"
    elif metadata_storage.startswith("s3:"):
        bucket = metadata_storage.split(":")[1]
        return f"s3, filename='{table_path}/', format=Parquet, url='{minio_url}/{bucket}/'"
    elif metadata_storage == "azure":
        return f"azure, container='{cluster.azure_container_name}', storage_account_url='{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', blob_path='{table_path}/', format=Parquet"
    elif metadata_storage.startswith("azure:"):
        container = metadata_storage.split(":")[1]
        return f"azure, container='{container}', storage_account_url='{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', blob_path='{table_path}/', format=Parquet"
    elif metadata_storage == "local":
        return f"local, path='/{table_path}', format=Parquet"
    else:
        raise ValueError(f"Unknown storage type: {metadata_storage}")


def find_files(directory: str, suffix: str) -> list:
    result = []
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(suffix):
                result.append(os.path.join(root, f))
    return result


def external_bucket(started_cluster, index: int = 1) -> str:
    name = f"{started_cluster.minio_bucket}-storage{index}"
    if not started_cluster.minio_client.bucket_exists(name):
        started_cluster.minio_client.make_bucket(name)
    return name


def path_modifier(old_path: str, new_storage: str, cluster, base_path: str):
    if "://" in old_path:
        parts = old_path.split("/")
        for i, part in enumerate(parts):
            if base_path.split("/")[0] in part or "var" in part:
                relative = "/".join(parts[i:])
                break
        else:
            relative = parts[-1]
    else:
        relative = old_path.lstrip("/")

    return get_absolute_path(new_storage, cluster, relative)


def create_and_upload_table(cluster, table_name):
    spark = cluster.spark_session
    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    default_upload_directory(cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")


def _download_table_for_relocation(started_cluster, table_name):
    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, table_name)
    os.makedirs(host_path, exist_ok=True)
    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/", host_path)
    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    return temp_dir, host_path, base_path


def _distribute_table_components(started_cluster, table_name, metadata_storage, manifest_list_storage,
                                 manifest_storage, data_storage):
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
    for mf in manifest_files:
        modify_avro_file(mf, ["data_file", "file_path"],
                         lambda p: path_modifier(p, data_storage, started_cluster, base_path))

    manifest_list_files = [f for f in find_files(metadata_dir, ".avro") if os.path.basename(f).startswith("snap-")]
    for ml in manifest_list_files:
        modify_avro_file(ml, ["manifest_path"],
                         lambda p: path_modifier(p, manifest_storage, started_cluster, base_path))

    for mj in find_files(metadata_dir, ".metadata.json"):
        with open(mj, 'r') as f:
            data = json.load(f)
        data["location"] = get_absolute_path(metadata_storage, started_cluster, base_path)
        for snap in data.get("snapshots", []):
            if "manifest-list" in snap:
                snap["manifest-list"] = path_modifier(snap["manifest-list"], manifest_list_storage, started_cluster, base_path)
        with open(mj, 'w') as f:
            json.dump(data, f, indent=2)

    def upload(files, storage):
        uploader = get_uploader(storage, started_cluster)
        for f in files:
            uploader.upload_file(f, f"{base_path}/{os.path.relpath(f, host_path)}")

    upload(find_files(metadata_dir, ".metadata.json") + find_files(metadata_dir, "version-hint.text"), metadata_storage)
    upload(manifest_list_files, manifest_list_storage)
    upload(manifest_files, manifest_storage)
    upload(find_files(data_dir, ".parquet") if os.path.exists(data_dir) else [], data_storage)

    shutil.rmtree(temp_dir)
    return base_path


def _move_files_to_bucket(started_cluster, files, bucket, host_path, base_path):
    uploader = S3Uploader(started_cluster.minio_client, bucket)
    for f in files:
        rel = os.path.relpath(f, host_path)
        uploader.upload_file(f, f"{base_path}/{rel}")
        started_cluster.minio_client.remove_object(started_cluster.minio_bucket, f"{base_path}/{rel}")


def relocate_manifest_lists_to_bucket(started_cluster, table_name, manifest_list_bucket):
    manifest_list_storage = f"s3:{manifest_list_bucket}"

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    metadata_dir = os.path.join(host_path, "metadata")

    manifest_list_files = [f for f in find_files(metadata_dir, ".avro") if os.path.basename(f).startswith("snap-")]
    for mj in find_files(metadata_dir, ".metadata.json"):
        with open(mj, 'r') as f:
            data = json.load(f)
        for snap in data.get("snapshots", []):
            if "manifest-list" in snap:
                snap["manifest-list"] = path_modifier(snap["manifest-list"], manifest_list_storage, started_cluster, base_path)
        with open(mj, 'w') as f:
            json.dump(data, f, indent=2)
        rel = os.path.relpath(mj, host_path)
        started_cluster.default_s3_uploader.upload_file(mj, f"{base_path}/{rel}")

    _move_files_to_bucket(started_cluster, manifest_list_files, manifest_list_bucket, host_path, base_path)

    shutil.rmtree(temp_dir)
    return base_path


def _delete_file_names(metadata_dir: str) -> set:
    names = set()
    for mf in find_files(metadata_dir, ".avro"):
        if os.path.basename(mf).startswith("snap-"):
            continue
        with open(mf, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            try:
                for record in reader:
                    data_file = record.get("data_file", {})
                    if data_file.get("content", 0) != 0:
                        names.add(os.path.basename(data_file["file_path"]))
            finally:
                reader.close()
    return names


def _rewrite_manifests_and_reupload(started_cluster, host_path, base_path, file_path_modifier):
    metadata_dir = os.path.join(host_path, "metadata")

    # Relocating data also requires rewriting position-delete rows and bounds; move delete files instead.
    assert not _delete_file_names(metadata_dir), \
        f"{host_path} has delete files; relocating its data files would silently disable them"

    manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
    for mf in manifest_files:
        modify_avro_file(mf, ["data_file", "file_path"], file_path_modifier)
        rel = os.path.relpath(mf, host_path)
        started_cluster.default_s3_uploader.upload_file(mf, f"{base_path}/{rel}")


def relocate_data_files_to_bucket(started_cluster, table_name, data_bucket, prefix=None, endpoint=None):
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    prefix = prefix if prefix is not None else f"{base_path}/data"
    uri = f"{endpoint}/{data_bucket}" if endpoint else f"s3a://{data_bucket}"
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path,
        lambda p: f"{uri}/{prefix}/{os.path.basename(p)}")
    uploader = S3Uploader(started_cluster.minio_client, data_bucket)
    for f in find_files(os.path.join(host_path, "data"), ".parquet"):
        uploader.upload_file(f, f"{prefix}/{os.path.basename(f)}")
        rel = os.path.relpath(f, host_path)
        started_cluster.minio_client.remove_object(started_cluster.minio_bucket, f"{base_path}/{rel}")
    shutil.rmtree(temp_dir)
    return base_path


def relocate_delete_files_to_bucket(started_cluster, table_name, delete_bucket):
    delete_storage = f"s3:{delete_bucket}"

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    metadata_dir = os.path.join(host_path, "metadata")
    delete_names = _delete_file_names(metadata_dir)
    assert delete_names, f"{table_name} has no delete files to relocate"

    def to_delete_bucket(old_path):
        if os.path.basename(old_path) not in delete_names:
            return old_path
        return path_modifier(old_path, delete_storage, started_cluster, base_path)

    for mf in find_files(metadata_dir, ".avro"):
        if os.path.basename(mf).startswith("snap-"):
            continue
        modify_avro_file(mf, ["data_file", "file_path"], to_delete_bucket)
        rel = os.path.relpath(mf, host_path)
        started_cluster.default_s3_uploader.upload_file(mf, f"{base_path}/{rel}")

    delete_files = [f for f in find_files(os.path.join(host_path, "data"), ".parquet")
                    if os.path.basename(f) in delete_names]
    _move_files_to_bucket(started_cluster, delete_files, delete_bucket, host_path, base_path)

    shutil.rmtree(temp_dir)
    return base_path


def _external_delete_files_in_another_bucket(started_cluster, table_name):
    spark = started_cluster.spark_session
    bucket = external_bucket(started_cluster)

    spark.sql(
        f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    spark.sql(f"DELETE FROM {table_name} WHERE id = 2")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    base_path = relocate_delete_files_to_bucket(started_cluster, table_name, bucket)
    return base_path, bucket, f"{base_path}/"


def _external_manifest_lists_in_another_bucket(started_cluster, table_name):
    spark = started_cluster.spark_session
    bucket = external_bucket(started_cluster)

    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha')")
    spark.sql(f"INSERT INTO {table_name} VALUES (2, 'beta')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    base_path = relocate_manifest_lists_to_bucket(started_cluster, table_name, bucket)
    return base_path, bucket, f"{base_path}/"


def _external_data_files_in_another_bucket(started_cluster, table_name):
    bucket = external_bucket(started_cluster)

    create_and_upload_table(started_cluster, table_name)
    base_path = relocate_data_files_to_bucket(started_cluster, table_name, bucket)
    return base_path, bucket, f"{base_path}/data/"


def _external_data_files_in_the_same_bucket(started_cluster, table_name):
    external_prefix = f"external_data/{table_name}"

    create_and_upload_table(started_cluster, table_name)
    base_path = relocate_data_files_to_bucket(started_cluster, table_name, started_cluster.minio_bucket, prefix=external_prefix)
    return base_path, started_cluster.minio_bucket, f"{external_prefix}/"


REMOVE_ORPHAN_FILES = "ALTER TABLE {table} EXECUTE remove_orphan_files(older_than = '2020-01-01 00:00:00', dry_run = 1)"
REMOVE_ORPHAN_FILES_SETTINGS = {"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 1}
ALL_ROWS = "1\talpha\n2\tbeta\n3\tgamma\n"


def _manifest_lists_delete_files(avro_path: str) -> bool:
    with open(avro_path, 'rb') as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        try:
            return any(record.get("data_file", {}).get("content", 0) != 0 for record in reader)
        finally:
            reader.close()


def _rewrite_paths_to_local_uri(started_cluster, table_name, authority, deletes_only=False, target_dir=None):
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    relocated = set()

    def to_local(old_path):
        name = os.path.basename(old_path)
        relocated.add(name)
        if target_dir:
            return f"file://{authority}{target_dir}/{name}"
        return f"file://{authority}/{base_path}/data/{name}"

    for mf in find_files(metadata_dir, ".avro"):
        if os.path.basename(mf).startswith("snap-"):
            continue
        if deletes_only and not _manifest_lists_delete_files(mf):
            continue
        modify_avro_file(mf, ["data_file", "file_path"], to_local)
        rel = os.path.relpath(mf, host_path)
        started_cluster.default_s3_uploader.upload_file(mf, f"{base_path}/{rel}")

    assert relocated, f"Nothing to relocate for {table_name}"

    for f in find_files(data_dir, ".parquet"):
        if os.path.basename(f) not in relocated:
            continue
        rel = os.path.relpath(f, host_path)
        local_path = f"{target_dir}/{os.path.basename(f)}" if target_dir else f"/{base_path}/{rel}"
        started_cluster.default_local_uploader.upload_file(f, local_path)
        started_cluster.minio_client.remove_object(started_cluster.minio_bucket, f"{base_path}/{rel}")

    shutil.rmtree(temp_dir)
    return base_path


def _create_iceberg_s3_table(started_cluster, table_name, base_path):
    instance = started_cluster.instances["node1"]
    args = get_query_args("s3", started_cluster, base_path)
    instance.query(f"DROP TABLE IF EXISTS {table_name}")
    instance.query(f"CREATE TABLE {table_name} ENGINE=IcebergS3({args})")


def _check_cluster_function_rejects_table(started_cluster, table_name, base_path, expected_rows):
    instance = started_cluster.instances["node1"]
    args = get_query_args("s3", started_cluster, base_path)
    error = instance.query_and_get_error(
        f"SELECT * FROM icebergS3Cluster('cluster_simple', {args}) ORDER BY id")
    assert "cannot be read by a cluster function" in error

    _create_iceberg_s3_table(started_cluster, table_name, base_path)
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY id") == expected_rows
    instance.query(f"DROP TABLE {table_name} SYNC")


def _relocate_local_data_files_outside_table_directory(started_cluster, table_name, external_dir, scheme):
    instance = started_cluster.instances["node1"]

    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    for metadata_json in find_files(metadata_dir, ".metadata.json"):
        with open(metadata_json) as f:
            metadata = json.load(f)
        metadata["location"] = f"{scheme}{host_path}"
        with open(metadata_json, "w") as f:
            json.dump(metadata, f)

    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        modify_avro_file(manifest, ["data_file", "file_path"], lambda p: f"{scheme}{external_dir}/{os.path.basename(p)}")

    default_upload_directory(started_cluster, "local", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    for f in find_files(data_dir, ".parquet"):
        started_cluster.default_local_uploader.upload_file(f, f"{external_dir}/{os.path.basename(f)}")
    instance.exec_in_container(["bash", "-c", f"rm -f {host_path}/data/*.parquet"])

    return host_path
