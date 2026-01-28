import pytest
import os
import shutil
import tempfile
import json
import avro.datafile
import avro.io

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    LocalUploader,
    S3Uploader,
    AzureUploader,
    LocalDownloader,
    S3Downloader,
    prepare_s3_bucket,
)
from helpers.iceberg_utils import (
    get_uuid_str,
    get_spark,
    default_upload_directory,
    default_download_directory,
)


@pytest.fixture(scope="package")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )

        cluster.start()

        prepare_s3_bucket(cluster)

        cluster.spark_session = get_spark()

        cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
        cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)

        cluster.azure_container_name = "mycontainer"
        cluster.blob_service_client.create_container(cluster.azure_container_name)
        cluster.default_azure_uploader = AzureUploader(cluster.blob_service_client, cluster.azure_container_name)

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])

        # Create extra S3 buckets for test_four_different_locations
        for i in range(1, 4):
            bucket_name = f"{cluster.minio_bucket}-storage{i}"
            if not cluster.minio_client.bucket_exists(bucket_name):
                cluster.minio_client.make_bucket(bucket_name)

        yield cluster

    finally:
        cluster.shutdown()


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
    elif storage_type.startswith("s3:"):
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


# =============================================================================
# Tests
# =============================================================================

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
                    combinations.append((metadata, manifest_list, manifest, data))
    return combinations

VALID_COMBINATIONS = _generate_valid_combinations()

@pytest.mark.parametrize("metadata_storage,manifest_list_storage,manifest_storage,data_storage", VALID_COMBINATIONS)
def test_multi_storage_combinations(started_cluster, metadata_storage, manifest_list_storage, manifest_storage, data_storage):
    """
    Test Iceberg table with all components in different storage locations.
    """
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_combo_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    # Upload to default S3 first
    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    # Download all files
    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, TABLE_NAME)
    os.makedirs(host_path, exist_ok=True)
    
    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/", host_path)

    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    # Step 1: Modify manifest files to point to data_storage
    manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
    for mf in manifest_files:
        modify_avro_file(mf, ["data_file", "file_path"], 
                        lambda p: path_modifier(p, data_storage, started_cluster, base_path))

    # Step 2: Modify manifest-list files to point to manifest_storage
    manifest_list_files = [f for f in find_files(metadata_dir, ".avro") if os.path.basename(f).startswith("snap-")]
    for ml in manifest_list_files:
        modify_avro_file(ml, ["manifest_path"],
                        lambda p: path_modifier(p, manifest_storage, started_cluster, base_path))

    # Step 3: Modify metadata.json to point to manifest_list_storage
    for mj in find_files(metadata_dir, ".metadata.json"):
        with open(mj, 'r') as f:
            data = json.load(f)
        
        data["location"] = get_absolute_path(metadata_storage, started_cluster, base_path)
        
        # Update snapshot manifest-list paths
        if "snapshots" in data:
            for snap in data["snapshots"]:
                if "manifest-list" in snap:
                    snap["manifest-list"] = path_modifier(snap["manifest-list"], manifest_list_storage, started_cluster, base_path)
        
        with open(mj, 'w') as f:
            json.dump(data, f, indent=2)

    # Step 4: Upload to respective storages
    # Metadata files (*.metadata.json, version-hint.text)
    meta_uploader = get_uploader(metadata_storage, started_cluster)
    for f in find_files(metadata_dir, ".metadata.json") + find_files(metadata_dir, "version-hint.text"):
        rel = os.path.relpath(f, host_path)
        meta_uploader.upload_file(f, f"{base_path}/{rel}")
    
    # Manifest-list files
    ml_uploader = get_uploader(manifest_list_storage, started_cluster)
    for f in manifest_list_files:
        rel = os.path.relpath(f, host_path)
        ml_uploader.upload_file(f, f"{base_path}/{rel}")

    # Manifest files
    m_uploader = get_uploader(manifest_storage, started_cluster)
    for f in manifest_files:
        rel = os.path.relpath(f, host_path)
        m_uploader.upload_file(f, f"{base_path}/{rel}")

    # Data files
    d_uploader = get_uploader(data_storage, started_cluster)
    if os.path.exists(data_dir):
        for f in find_files(data_dir, ".parquet"):
            rel = os.path.relpath(f, host_path)
            d_uploader.upload_file(f, f"{base_path}/{rel}")

    shutil.rmtree(temp_dir)

    func = get_table_function(metadata_storage)
    args = get_query_args(metadata_storage, started_cluster, base_path)

    assert instance.query(f"SELECT * FROM {func}({args}) ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"


# S3 is the primary use case for cross-bucket access.
# Azure cross-container: not supported (account_key not extractable from credential object).
def test_four_different_s3_buckets(started_cluster):
    """S3: each component in a different bucket (metadata, manifest-list, manifest, data)."""
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_four_buckets_{get_uuid_str()}"
    buckets = [
        started_cluster.minio_bucket,
        f"{started_cluster.minio_bucket}-storage1",
        f"{started_cluster.minio_bucket}-storage2",
        f"{started_cluster.minio_bucket}-storage3",
    ]

    metadata_storage = f"s3:{buckets[0]}"
    manifest_list_storage = f"s3:{buckets[1]}"
    manifest_storage = f"s3:{buckets[2]}"
    data_storage = f"s3:{buckets[3]}"

    uploaders = {f"s3:{b}": S3Uploader(started_cluster.minio_client, b) for b in buckets}

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, name STRING, score INT) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Carol', 92)")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, TABLE_NAME)
    os.makedirs(host_path, exist_ok=True)

    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/", host_path)

    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
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
        if "snapshots" in data:
            for snap in data["snapshots"]:
                if "manifest-list" in snap:
                    snap["manifest-list"] = path_modifier(snap["manifest-list"], manifest_list_storage, started_cluster, base_path)
        with open(mj, 'w') as f:
            json.dump(data, f, indent=2)

    for f in find_files(metadata_dir, ".metadata.json") + find_files(metadata_dir, "version-hint.text"):
        rel = os.path.relpath(f, host_path)
        uploaders[metadata_storage].upload_file(f, f"{base_path}/{rel}")

    for f in manifest_list_files:
        rel = os.path.relpath(f, host_path)
        uploaders[manifest_list_storage].upload_file(f, f"{base_path}/{rel}")

    for f in manifest_files:
        rel = os.path.relpath(f, host_path)
        uploaders[manifest_storage].upload_file(f, f"{base_path}/{rel}")

    if os.path.exists(data_dir):
        for f in find_files(data_dir, ".parquet"):
            rel = os.path.relpath(f, host_path)
            uploaders[data_storage].upload_file(f, f"{base_path}/{rel}")

    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    result = instance.query(f"SELECT * FROM icebergS3(s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{buckets[0]}/') ORDER BY id")

    assert result == "1\tAlice\t100\n2\tBob\t85\n3\tCarol\t92\n"