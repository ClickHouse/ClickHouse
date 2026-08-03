import pytest
import pyspark
import os
import shutil
import tempfile
import time
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
    default_upload_directory,
    default_download_directory,
)

def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_storage_iceberg_multistorage")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/var/lib/clickhouse/user_files/iceberg_data")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )
    return builder.getOrCreate()


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


# Regression test: the bucket from an explicit path-style URL must be preserved when creating
# the secondary storage; otherwise reads are issued against the wrong bucket.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3348134710
def test_explicit_http_urls_different_buckets(started_cluster):
    """S3: components referenced via explicit `http://endpoint/bucket/...` URLs in different buckets."""
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_explicit_urls_{get_uuid_str()}"
    buckets = [
        started_cluster.minio_bucket,
        f"{started_cluster.minio_bucket}-storage1",
        f"{started_cluster.minio_bucket}-storage2",
        f"{started_cluster.minio_bucket}-storage3",
    ]

    metadata_storage = f"s3:{buckets[0]}"
    manifest_list_storage = f"url:{buckets[1]}"
    manifest_storage = f"url:{buckets[2]}"
    data_storage = f"url:{buckets[3]}"

    uploaders = {
        metadata_storage: S3Uploader(started_cluster.minio_client, buckets[0]),
        manifest_list_storage: S3Uploader(started_cluster.minio_client, buckets[1]),
        manifest_storage: S3Uploader(started_cluster.minio_client, buckets[2]),
        data_storage: S3Uploader(started_cluster.minio_client, buckets[3]),
    }

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


# Regression test: external data files in different buckets under the same object key
# used to share one num-rows cache entry, returning the wrong `count()`.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3356426404
def test_num_rows_cache_no_collision_across_buckets(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    base_bucket = started_cluster.minio_bucket
    # The same object key for both tables, each in its own bucket.
    shared_key = f"shared_count_cache_{get_uuid_str()}/data/part-0.parquet"

    def prepare_table(table_name, values_sql, data_bucket):
        spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
        spark.sql(f"INSERT INTO {table_name} VALUES {values_sql}")

        default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{table_name}/", f"/iceberg_data/default/{table_name}/")

        temp_dir = tempfile.mkdtemp()
        host_path = os.path.join(temp_dir, table_name)
        os.makedirs(host_path, exist_ok=True)
        default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/", host_path)

        base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
        metadata_dir = os.path.join(host_path, "metadata")
        data_dir = os.path.join(host_path, "data")

        data_files = find_files(data_dir, ".parquet")
        assert len(data_files) == 1, f"Expected a single data file, got: {data_files}"

        # Point the data file to the same object key in a different bucket.
        manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
        for mf in manifest_files:
            modify_avro_file(mf, ["data_file", "file_path"], lambda _: f"s3a://{data_bucket}/{shared_key}")
            # Drop the statistics so that `count()` is not answered from metadata.
            modify_avro_file(mf, ["data_file", "value_counts"], lambda _: None)

        for mj in find_files(metadata_dir, ".metadata.json"):
            with open(mj, 'r') as f:
                data = json.load(f)
            for snap in data.get("snapshots", []):
                snap.get("summary", {}).pop("total-records", None)
            with open(mj, 'w') as f:
                json.dump(data, f, indent=2)

        for f in manifest_files + find_files(metadata_dir, ".metadata.json"):
            rel = os.path.relpath(f, host_path)
            started_cluster.default_s3_uploader.upload_file(f, f"{base_path}/{rel}")

        S3Uploader(started_cluster.minio_client, data_bucket).upload_file(data_files[0], shared_key)

        shutil.rmtree(temp_dir)
        return base_path

    # An entry is reused only for files older than it, so upload everything before querying.
    base_path_a = prepare_table(
        f"test_count_cache_a_{get_uuid_str()}", "(1, 'a'), (2, 'b'), (3, 'c')", f"{base_bucket}-storage1"
    )
    base_path_b = prepare_table(
        f"test_count_cache_b_{get_uuid_str()}", "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", f"{base_bucket}-storage2"
    )
    # Margin for the second-resolution `last_modified` comparison.
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

    # The first query populates the num-rows cache; the second one must not reuse its entry.
    count_a, cache_lookups_a = count(base_path_a, "count_cache_marker_a")
    count_b, cache_lookups_b = count(base_path_b, "count_cache_marker_b")
    assert count_a == "3"
    assert count_b == "5"
    # Both queries must actually consult the num-rows cache.
    assert cache_lookups_a >= 1
    assert cache_lookups_b >= 1


def _download_table_for_relocation(started_cluster, table_name):
    """Download a table's on-disk files to a fresh temp dir for rewriting/relocation. Returns
    (temp_dir, host_path, base_path); the caller is responsible for `shutil.rmtree(temp_dir)`."""
    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, table_name)
    os.makedirs(host_path, exist_ok=True)
    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/", host_path)
    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    return temp_dir, host_path, base_path


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


def _rewrite_manifests_and_reupload(started_cluster, host_path, base_path, file_path_modifier):
    """Rewrite every manifest's `data_file.file_path` via `file_path_modifier` and re-upload the
    manifests to the base bucket. Manifest lists and metadata.json are left untouched."""
    metadata_dir = os.path.join(host_path, "metadata")
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


# Regression test: the `OPTIMIZE TABLE ... MANIFEST` threshold pre-check used to read the
# current manifest list from the base storage only and failed when it lived in another bucket.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3613986714
@pytest.mark.skip(
    reason="Manifest-only compaction (`OPTIMIZE TABLE ... MANIFEST` and the "
    "`iceberg_manifest_min_count_to_compact` setting) is not part of antalya-26.6, so the "
    "external-manifest-list support this test covers has nothing to exercise here. "
    "Re-enable together with the manifest compaction feature."
)
def test_optimize_manifest_with_external_manifest_list(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_optimize_external_ml_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    # Three appends so the manifest list is above the compaction threshold below.
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2, 'beta')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = relocate_manifest_lists_to_bucket(started_cluster, TABLE_NAME, f"{base_bucket}-storage1")

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"

    def count_metadata_files():
        return sum(
            1 for obj in started_cluster.minio_client.list_objects(base_bucket, prefix=f"{base_path}/metadata/", recursive=True)
            if obj.object_name.endswith(".json")
        )

    metadata_files_before = count_metadata_files()

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # The compaction must actually commit new metadata, not early-return "below threshold".
    assert count_metadata_files() > metadata_files_before

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME}")


# Regression test: `generateManifestList` used to reread the parent snapshot's manifest list from
# the base storage only, so INSERT failed when the current manifest list lived in another bucket.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3613986717
def test_insert_with_external_manifest_list(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_insert_external_ml_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = relocate_manifest_lists_to_bucket(started_cluster, TABLE_NAME, f"{base_bucket}-storage1")

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'gamma')", settings={"allow_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME}")


# Same as `test_insert_with_external_manifest_list`, but through `ALTER TABLE ... DELETE`.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3613986717
def test_mutation_with_external_manifest_list(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_mutation_external_ml_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = relocate_manifest_lists_to_bucket(started_cluster, TABLE_NAME, f"{base_bucket}-storage1")

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 2", settings={"allow_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME}")


# Regression test: `_path` predicate pushdown and bucket splitting must operate on the same
# absolute path that Iceberg rows expose for external files. Before the fix, the iterator-side
# filter evaluated `namespace/key` while rows exposed the raw metadata URI, so a `_path`
# predicate silently discarded external files, and `cluster_table_function_split_granularity =
# 'bucket'` sliced the object info to a plain one, losing the resolved storage.
def test_external_path_virtual_column_filter(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_path_filter_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    data_bucket = f"{base_bucket}-storage1"
    data_storage = f"s3:{data_bucket}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    temp_dir = tempfile.mkdtemp()
    host_path = os.path.join(temp_dir, TABLE_NAME)
    os.makedirs(host_path, exist_ok=True)
    default_download_directory(started_cluster, "s3", f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/", host_path)

    base_path = f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    metadata_dir = os.path.join(host_path, "metadata")
    data_dir = os.path.join(host_path, "data")

    # Point the data files at another bucket; metadata stays in the base bucket.
    manifest_files = [f for f in find_files(metadata_dir, ".avro") if not os.path.basename(f).startswith("snap-")]
    for mf in manifest_files:
        modify_avro_file(mf, ["data_file", "file_path"],
                        lambda p: path_modifier(p, data_storage, started_cluster, base_path))

    for f in manifest_files:
        rel = os.path.relpath(f, host_path)
        started_cluster.default_s3_uploader.upload_file(f, f"{base_path}/{rel}")

    data_uploader = S3Uploader(started_cluster.minio_client, data_bucket)
    for f in find_files(data_dir, ".parquet"):
        rel = os.path.relpath(f, host_path)
        data_uploader.upload_file(f, f"{base_path}/{rel}")

    shutil.rmtree(temp_dir)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"

    paths = instance.query(f"SELECT DISTINCT _path FROM icebergS3({args})").strip().splitlines()
    assert len(paths) == 1
    external_path = paths[0]
    # `_path` must expose the external location, not a key inside the base bucket.
    assert data_bucket in external_path

    # Filtering by the very value the rows expose must select the file, not discard it.
    assert instance.query(
        f"SELECT count() FROM icebergS3({args}) WHERE _path = '{external_path}'"
    ).strip() == "3"

    # The same through the cluster function; bucket splitting must keep the resolved storage.
    assert instance.query(
        f"SELECT count() FROM icebergS3Cluster(cluster_simple, {args}) WHERE _path = '{external_path}' "
        "SETTINGS skip_unavailable_shards = 1, cluster_table_function_split_granularity = 'bucket'"
    ).strip() == "3"


# Regression test: `DROP TABLE` with `iceberg_delete_data_on_drop = 1` used to delete only the base
# storage subtree, leaving data files that live in another bucket behind. `IcebergMetadata::drop` now
# also walks the current metadata graph and deletes files that resolve to a secondary storage.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3621619550
def test_delete_data_on_drop_removes_external_files(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_drop_external_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    data_bucket = f"{base_bucket}-storage1"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    # Data files live in another bucket; metadata / manifests stay in the base bucket.
    base_path = relocate_data_files_to_bucket(started_cluster, TABLE_NAME, data_bucket)

    def count_objects(bucket, prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"

    assert count_objects(data_bucket, f"{base_path}/data/") > 0
    assert count_objects(base_bucket, f"{base_path}/") > 0

    # `SYNC` waits for the background drop (which runs `IcebergMetadata::drop`) to finish.
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")

    # Both the base subtree and the external data files must be gone.
    assert count_objects(base_bucket, f"{base_path}/") == 0
    assert count_objects(data_bucket, f"{base_path}/data/") == 0


# Regression test: `remove_orphan_files` scans and deletes only within the base storage, so it cannot
# clean orphans that live in another bucket / account. Rather than silently report a partial cleanup
# as complete, it now fails closed when the metadata graph references files outside the base storage.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3621619560
def test_remove_orphan_files_rejects_external_paths(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_orphan_external_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    data_bucket = f"{base_bucket}-storage1"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    base_path = relocate_data_files_to_bucket(started_cluster, TABLE_NAME, data_bucket)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    # Sanity check: the data really is external and readable.
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"

    # `remove_orphan_files` must refuse rather than silently skip the external data files.
    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE remove_orphan_files(older_than = '2020-01-01 00:00:00', dry_run = 1)",
        settings={"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 1},
    )
    assert "outside the table's base directory" in error

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


# Regression test: a data file referenced by an absolute URI elsewhere in the SAME base bucket resolves
# to the base storage but outside `table_path`. It used to land in `reachable` instead of `external_files`,
# so `remove_orphan_files` did not fail closed on it. It now does.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3632505967
def test_remove_orphan_files_rejects_same_bucket_external_paths(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_orphan_same_bucket_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    # Data files live elsewhere in the SAME bucket; metadata / manifests stay in the table directory.
    base_path = relocate_data_files_within_base_bucket(started_cluster, TABLE_NAME, external_prefix)

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    # Sanity check: the data really is outside the table directory yet readable through the base storage.
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE remove_orphan_files(older_than = '2020-01-01 00:00:00', dry_run = 1)",
        settings={"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 1},
    )
    assert "outside the table's base directory" in error

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


# Regression test: `DROP TABLE` with `iceberg_delete_data_on_drop = 1` used to leak data files that
# resolve to the base storage but live outside `table_path` (an absolute URI elsewhere in the same
# bucket): they landed in `reachable` instead of `external_files`. They are now deleted on drop.
# https://github.com/ClickHouse/ClickHouse/pull/90740#discussion_r3632505967
def test_delete_data_on_drop_removes_same_bucket_external_files(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_drop_same_bucket_{get_uuid_str()}"
    base_bucket = started_cluster.minio_bucket
    external_prefix = f"external_data/{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")
    # Data files live elsewhere in the SAME bucket; metadata / manifests stay in the table directory.
    base_path = relocate_data_files_within_base_bucket(started_cluster, TABLE_NAME, external_prefix)

    def count_objects(bucket, prefix):
        return sum(1 for _ in started_cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True))

    minio_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"

    # The data really lives outside the table directory but in the same bucket.
    assert count_objects(base_bucket, f"{base_path}/") > 0
    assert count_objects(base_bucket, f"{external_prefix}/") > 0

    # `SYNC` waits for the background drop (which runs `IcebergMetadata::drop`) to finish.
    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")

    # Both the table directory and the same-bucket external data files must be gone.
    assert count_objects(base_bucket, f"{base_path}/") == 0
    assert count_objects(base_bucket, f"{external_prefix}/") == 0
