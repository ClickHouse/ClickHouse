import os
import json
import shutil
import tempfile
import avro.datafile
import avro.io

from helpers.s3_tools import AzureUploader, S3Uploader
from helpers.iceberg_utils import default_download_directory, default_upload_directory


def modify_avro_file(avro_path: str, field_path: list, modifier_func) -> None:
    """
    Modify a field in an AVRO file, preserving the rest of it as is.

    field_path: list of keys to navigate to the field
    modifier_func: function that takes old value and returns new value
    """
    with open(avro_path, 'rb') as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        # Preserve all file metadata (partition-spec, format-version, etc.)
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
    """Convert relative path to absolute path for given storage type."""
    relative_path = relative_path.lstrip("/")

    if storage_type == "s3":
        return f"s3a://{cluster.minio_bucket}/{relative_path}"
    elif storage_type.startswith("s3:"):  # s3:bucket_name format
        bucket = storage_type.split(":")[1]
        return f"s3a://{bucket}/{relative_path}"
    elif storage_type.startswith("url:"):  # url:bucket_name format - explicit http://endpoint/bucket/... URL
        bucket = storage_type.split(":")[1]
        return f"http://{cluster.minio_host}:{cluster.minio_port}/{bucket}/{relative_path}"
    elif storage_type == "azure":
        return f"abfs://{cluster.azure_container_name}@{cluster.azurite_account}/{relative_path}"
    elif storage_type.startswith("azure:"):  # azure:container_name format
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
    """Get query arguments for the iceberg table function."""
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
    """Find files ending with given suffix."""
    result = []
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(suffix):
                result.append(os.path.join(root, f))
    return result


def external_bucket(started_cluster, index: int = 1) -> str:
    """A bucket next to the one the table itself lives in, for files its metadata places elsewhere.
    The cluster fixture only creates the base bucket, so it is made here on first use."""
    name = f"{started_cluster.minio_bucket}-storage{index}"
    if not started_cluster.minio_client.bucket_exists(name):
        started_cluster.minio_client.make_bucket(name)
    return name


def path_modifier(old_path: str, new_storage: str, cluster, base_path: str):
    """Create a new absolute path for a different storage location."""
    # Extract just the filename/relative portion
    if "://" in old_path:
        # Parse out the path part after protocol://bucket/
        parts = old_path.split("/")
        # Find where the actual path starts (after bucket)
        for i, part in enumerate(parts):
            if base_path.split("/")[0] in part or "var" in part:
                relative = "/".join(parts[i:])
                break
        else:
            relative = parts[-1]
    else:
        relative = old_path.lstrip("/")

    return get_absolute_path(new_storage, cluster, relative)


def _download_table_for_relocation(started_cluster, table_name):
    """Download a table's on-disk files to a fresh temp dir for rewriting/relocation. Returns
    (temp_dir, host_path, base_path); the caller is responsible for `shutil.rmtree(temp_dir)`."""
    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, table_name)
    os.makedirs(host_path, exist_ok=True)
    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/", host_path)
    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    return temp_dir, host_path, base_path


def _distribute_table_components(started_cluster, table_name, metadata_storage, manifest_list_storage,
                                 manifest_storage, data_storage):
    """Rewrite every path a table's metadata spells into the absolute path of the storage its component
    is assigned to, and upload each component there. Returns `base_path`."""
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


STORAGE_TYPES = ["s3", "azure", "local"]


def _get_type_family(t):
    if t.startswith("s3"):
        return "s3"
    elif t.startswith("azure"):
        return "azure"
    return t


def _generate_valid_combinations():
    """
    Generate valid storage combinations.
    Rule: all components must be same type family as metadata, OR local.
    Local doesn't need credentials, so S3+local and Azure+local work.
    But S3+Azure doesn't work (credentials aren't interchangeable).
    Each component is resolved on its own, so moving several of them at once adds nothing over
    moving one at a time -- except moving all of them, the shape a table written elsewhere has.
    """
    combinations = []
    for metadata in STORAGE_TYPES:
        main_family = _get_type_family(metadata)
        for manifest_list in STORAGE_TYPES:
            if _get_type_family(manifest_list) not in (main_family, "local"):
                continue
            for manifest in STORAGE_TYPES:
                if _get_type_family(manifest) not in (main_family, "local"):
                    continue
                for data in STORAGE_TYPES:
                    if _get_type_family(data) not in (main_family, "local"):
                        continue
                    components = (manifest_list, manifest, data)
                    external = sum(1 for c in components if _get_type_family(c) != main_family)
                    if external not in (0, 1, len(components)):
                        continue
                    combinations.append((metadata, *components))
    return combinations


VALID_COMBINATIONS = _generate_valid_combinations()


def _move_files_to_bucket(started_cluster, files, bucket, host_path, base_path):
    """Upload each file to `bucket` under its table-relative path and delete the stale base-bucket copy,
    so the file ends up living only on the secondary storage."""
    uploader = S3Uploader(started_cluster.minio_client, bucket)
    for f in files:
        rel = os.path.relpath(f, host_path)
        uploader.upload_file(f, f"{base_path}/{rel}")
        started_cluster.minio_client.remove_object(started_cluster.minio_bucket, f"{base_path}/{rel}")


def relocate_manifest_lists_to_bucket(started_cluster, table_name, manifest_list_bucket):
    """Move the table's manifest lists to `manifest_list_bucket`; the stale base-bucket copies are
    deleted so a read that wrongly resolves the external path against the base storage cannot succeed."""
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
    """Base names of the delete files (`data_file.content` != 0) the manifests reference."""
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
    """Rewrite every manifest's `data_file.file_path` via `file_path_modifier` and re-upload the
    manifests to the base bucket. Manifest lists and metadata.json are left untouched."""
    metadata_dir = os.path.join(host_path, "metadata")

    # A position delete file names its data files with the exact spelling the manifest uses, both in its
    # own rows and in the `file_path` bounds its manifest entry carries. Rewriting the data-file paths
    # here would leave both behind, and the table would read as if nothing had been deleted. Relocate the
    # delete files instead (`relocate_delete_files_to_bucket`), which leaves those references intact.
    assert not _delete_file_names(metadata_dir), \
        f"{host_path} has delete files; relocating its data files would silently disable them"

    manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
    for mf in manifest_files:
        modify_avro_file(mf, ["data_file", "file_path"], file_path_modifier)
        rel = os.path.relpath(mf, host_path)
        started_cluster.default_s3_uploader.upload_file(mf, f"{base_path}/{rel}")


def relocate_data_files_to_bucket(started_cluster, table_name, data_bucket):
    """Move the table's data files to `data_bucket`; manifests are rewritten to point there and the
    stale base-bucket copies are deleted so the data lives only on the secondary storage."""
    data_storage = f"s3:{data_bucket}"

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    data_dir = os.path.join(host_path, "data")

    _rewrite_manifests_and_reupload(started_cluster, host_path, base_path,
                                    lambda p: path_modifier(p, data_storage, started_cluster, base_path))

    _move_files_to_bucket(started_cluster, find_files(data_dir, ".parquet"), data_bucket, host_path, base_path)

    shutil.rmtree(temp_dir)
    return base_path


def relocate_delete_files_to_bucket(started_cluster, table_name, delete_bucket):
    """Move the table's delete files to `delete_bucket`; their manifest entries are rewritten to point
    there and the stale base-bucket copies are deleted. The data files stay where they are, so the
    data-file paths a position delete file names -- in its rows and in its manifest bounds -- keep
    matching."""
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


def relocate_data_files_within_base_bucket(started_cluster, table_name, external_prefix):
    """Rewrite the table's data-file references to absolute URIs in the SAME base bucket but under
    `external_prefix` (outside the table directory), and move the parquet files there. Returns `base_path`."""
    base_bucket = started_cluster.minio_bucket
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    data_dir = os.path.join(host_path, "data")

    def to_external(old_path):
        filename = old_path.rstrip("/").rsplit("/", 1)[-1]
        return f"s3a://{base_bucket}/{external_prefix}/{filename}"

    _rewrite_manifests_and_reupload(started_cluster, host_path, base_path, to_external)

    for f in find_files(data_dir, ".parquet"):
        filename = os.path.basename(f)
        started_cluster.default_s3_uploader.upload_file(f, f"{external_prefix}/{filename}")
        rel = os.path.relpath(f, host_path)
        started_cluster.minio_client.remove_object(base_bucket, f"{base_path}/{rel}")

    shutil.rmtree(temp_dir)
    return base_path


# Each of these puts a table's files of one kind outside the table's base directory. They return the
# table's `base_path` and the bucket and prefix the relocated files now live under.

def _external_delete_files_in_another_bucket(started_cluster, table_name):
    """The positional delete file is what makes the table worth compacting, so an `OPTIMIZE` run gets as
    far as the cleanup that would delete it, and it is also the file put outside the table directory."""
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
    """Two appends, so the older snapshot -- and with it its manifest list, the file relocated here --
    is the one `retain_last = 1` expires."""
    spark = started_cluster.spark_session
    bucket = external_bucket(started_cluster)

    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha')")
    spark.sql(f"INSERT INTO {table_name} VALUES (2, 'beta')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    base_path = relocate_manifest_lists_to_bucket(started_cluster, table_name, bucket)
    return base_path, bucket, f"{base_path}/"


def _external_data_files_in_another_bucket(started_cluster, table_name):
    """The data files live in another bucket; metadata and manifests stay in the base bucket."""
    spark = started_cluster.spark_session
    bucket = external_bucket(started_cluster)

    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    base_path = relocate_data_files_to_bucket(started_cluster, table_name, bucket)
    return base_path, bucket, f"{base_path}/data/"


def _external_data_files_in_the_same_bucket(started_cluster, table_name):
    """The data files are referenced by an absolute URI elsewhere in the SAME bucket, so they resolve to
    the base storage but land outside `table_path`."""
    spark = started_cluster.spark_session
    external_prefix = f"external_data/{table_name}"

    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    base_path = relocate_data_files_within_base_bucket(started_cluster, table_name, external_prefix)
    return base_path, started_cluster.minio_bucket, f"{external_prefix}/"


REMOVE_ORPHAN_FILES = "ALTER TABLE {table} EXECUTE remove_orphan_files(older_than = '2020-01-01 00:00:00', dry_run = 1)"
REMOVE_ORPHAN_FILES_SETTINGS = {"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 1}
ALL_ROWS = "1\talpha\n2\tbeta\n3\tgamma\n"


def _manifest_lists_delete_files(avro_path: str) -> bool:
    """Whether a manifest describes delete files (`data_file.content` != 0) rather than data files."""
    with open(avro_path, 'rb') as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        try:
            return any(record.get("data_file", {}).get("content", 0) != 0 for record in reader)
        finally:
            reader.close()


def _rewrite_paths_to_local_uri(started_cluster, table_name, authority, deletes_only=False, target_dir=None):
    """Rewrite the table's file references -- with `deletes_only`, just those of its delete files -- to
    `file://{authority}/<original absolute path>`, and put the rewritten parquet files at that path on
    node1's filesystem, deleting the base-bucket copies so a read that wrongly resolves the path against
    the base storage cannot succeed. With `target_dir`, an absolute path, the files go there instead of
    to the table's own directory. Returns `base_path`."""
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
    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{started_cluster.minio_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {table_name}")
    instance.query(f"CREATE TABLE {table_name} ENGINE=IcebergS3({args})")


def _check_cluster_function_rejects_table(started_cluster, table_name, base_path, expected_rows):
    """The cluster function must fail closed on the table, while a plain read on node1, where the local
    files really are, still returns `expected_rows`."""
    instance = started_cluster.instances["node1"]
    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"

    error = instance.query_and_get_error(
        f"SELECT * FROM icebergS3Cluster('cluster_simple', s3, filename='{base_path}/', format=Parquet, "
        f"url='{minio_url}/{started_cluster.minio_bucket}/') ORDER BY id")
    assert "cannot be read by a cluster function" in error

    _create_iceberg_s3_table(started_cluster, table_name, base_path)
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY id") == expected_rows
    instance.query(f"DROP TABLE {table_name} SYNC")


def _relocate_local_data_files_outside_table_directory(started_cluster, table_name, external_dir):
    """Point the data files of a Spark-written local table at `external_dir` using bare absolute paths
    (no `file://` scheme), put them there on node1 and drop the in-table copies. Returns the table dir.
    The `location` written by the hadoop catalog is already a bare absolute path, so the data paths
    below only differ from it in escaping the table directory."""
    instance = started_cluster.instances["node1"]

    host_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    for manifest in [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]:
        modify_avro_file(manifest, ["data_file", "file_path"], lambda p: f"{external_dir}/{os.path.basename(p)}")

    default_upload_directory(started_cluster, "local", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")
    for f in find_files(data_dir, ".parquet"):
        started_cluster.default_local_uploader.upload_file(f, f"{external_dir}/{os.path.basename(f)}")
    instance.exec_in_container(["bash", "-c", f"rm -f {host_path}/data/*.parquet"])

    return host_path


def _relocate_data_files_to_bucket_by_ip(started_cluster, table_name, data_bucket):
    """Move the table's data files to `data_bucket` and reference them by an explicit
    `http://<minio ip>:<port>/<bucket>/...` URL, i.e. a different authority spelling than the base
    storage's `http://minio1:<port>`. Returns `base_path`."""
    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster, table_name)
    data_dir = os.path.join(host_path, "data")

    endpoint_by_ip = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"
    _rewrite_manifests_and_reupload(
        started_cluster, host_path, base_path,
        lambda p: f"{endpoint_by_ip}/{data_bucket}/{base_path}/data/{os.path.basename(p)}")

    _move_files_to_bucket(started_cluster, find_files(data_dir, ".parquet"), data_bucket, host_path, base_path)

    shutil.rmtree(temp_dir)
    return base_path
