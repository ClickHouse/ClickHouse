import json
import os
import tempfile

import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    default_upload_directory,
)


def patch_manifest_data_path(instance, table_dir, new_data_path):
    """
    Reads Iceberg metadata from the container, finds the manifest file,
    does a binary find-and-replace of the data file path inside it,
    preserving all Avro metadata (partition-spec, schema, etc.).

    Returns the original data file path so the caller can copy it to new_data_path.
    """
    import avro.datafile
    import avro.io

    metadata_dir = os.path.join(table_dir, "metadata")

    # 1. Find the latest metadata json (version-hint.text -> vN.metadata.json)
    version_hint = instance.exec_in_container(
        ["cat", os.path.join(metadata_dir, "version-hint.text")]
    ).strip()
    metadata_json_path = os.path.join(metadata_dir, f"v{version_hint}.metadata.json")
    metadata_json_str = instance.exec_in_container(["cat", metadata_json_path])
    metadata = json.loads(metadata_json_str)

    # 2. Find current snapshot's manifest-list path
    current_snapshot_id = metadata["current-snapshot-id"]
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == current_snapshot_id)
    manifest_list_path = snapshot["manifest-list"]

    # 3. Download manifest-list (Avro) from container to read manifest path
    manifest_list_local = tempfile.mktemp(suffix=".avro")
    instance.copy_file_from_container(manifest_list_path, manifest_list_local)

    with open(manifest_list_local, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        manifest_entries = list(reader)
        reader.close()

    assert len(manifest_entries) > 0, "Expected at least one manifest entry"
    manifest_path = manifest_entries[0]["manifest_path"]

    # 4. Download the manifest file (Avro) — read records to get original path
    manifest_local = tempfile.mktemp(suffix=".avro")
    instance.copy_file_from_container(manifest_path, manifest_local)

    with open(manifest_local, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        records = list(reader)
        reader.close()

    assert len(records) > 0, "Expected at least one record in manifest"
    original_data_path = records[0]["data_file"]["file_path"]

    # 5. Patch the manifest using the avro library: read with DataFileReader,
    #    then write back with DataFileWriter, copying all file-level metadata
    #    (partition-spec, schema, etc.) from the original file.
    with open(manifest_local, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        writer_schema = reader.datum_reader.writers_schema
        file_meta = dict(reader.meta)  # preserves partition-spec, schema, etc.
        codec = reader.codec
        all_records = list(reader)
        reader.close()

    for record in all_records:
        record["data_file"]["file_path"] = new_data_path

    patched_manifest_local = tempfile.mktemp(suffix=".avro")
    with open(patched_manifest_local, "wb") as f:
        writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), writer_schema, codec=codec)
        # Copy all non-standard metadata from original (partition-spec, schema, etc.)
        for key, value in file_meta.items():
            if key not in ("avro.schema", "avro.codec"):
                writer.set_meta(key, value)
        for record in all_records:
            writer.append(record)
        writer.flush()
        writer.close()

    # 6. Upload patched manifest to the container (overwrite)
    instance.copy_file_to_container(patched_manifest_local, manifest_path)

    # Cleanup temp files
    os.unlink(manifest_list_local)
    os.unlink(manifest_local)
    os.unlink(patched_manifest_local)

    return original_data_path


def test_manifest_data_path_outside_user_files(started_cluster_iceberg_with_spark):
    """
    Test that ClickHouse rejects reading an Iceberg table when the manifest
    entry points to a data file outside user_files directory via path traversal.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_manifest_path_security_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello')")

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    evil_dir = "/tmp/evil_data"
    evil_data_path = "/var/lib/clickhouse/user_files/../../../../tmp/evil_data/data.parquet"

    original_data_path = patch_manifest_data_path(instance, table_dir, evil_data_path)
    assert original_data_path is not None, "Could not find original data file path in manifest"

    instance.exec_in_container(["bash", "-c", f"mkdir -p {evil_dir}"])
    instance.exec_in_container(["bash", "-c", f"cp {original_data_path} {evil_data_path}"])

    create_sql = (
        f"DROP TABLE IF EXISTS {TABLE_NAME};\n"
        f"CREATE TABLE {TABLE_NAME} "
        f"ENGINE=IcebergLocal(local, path = '{table_dir}', format='Parquet')"
    )
    instance.query(create_sql)

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME}")
    assert "PATH_ACCESS_DENIED" in error, (
        f"Expected PATH_ACCESS_DENIED but got: {error}"
    )


def test_manifest_data_path_symlink_escape(started_cluster_iceberg_with_spark):
    """
    Test that ClickHouse rejects reading an Iceberg table when the manifest
    entry uses a symlink inside user_files that points outside.

    A path like <user_files>/allowed_link/file where allowed_link -> /tmp
    passes a purely lexical check (lexically_normal / lexically_relative)
    but accesses data outside user_files via symlink resolution.
    The check must use weakly_canonical (or equivalent) to catch this.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_symlink_escape_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello')")

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    evil_dir = "/tmp/symlink_evil_data"
    evil_data_path = f"{evil_dir}/data.parquet"

    original_data_path = patch_manifest_data_path(
        instance, table_dir,
        # This path looks valid lexically — it's inside user_files —
        # but allowed_link is a symlink to /tmp/symlink_evil_data
        "/var/lib/clickhouse/user_files/allowed_link/data.parquet"
    )
    assert original_data_path is not None, "Could not find original data file path in manifest"

    instance.exec_in_container(["bash", "-c", f"mkdir -p {evil_dir}"])
    instance.exec_in_container(["bash", "-c", f"cp {original_data_path} {evil_data_path}"])
    instance.exec_in_container([
        "bash", "-c",
        f"ln -sfn {evil_dir} /var/lib/clickhouse/user_files/allowed_link"
    ])

    create_sql = (
        f"DROP TABLE IF EXISTS {TABLE_NAME};\n"
        f"CREATE TABLE {TABLE_NAME} "
        f"ENGINE=IcebergLocal(local, path = '{table_dir}', format='Parquet')"
    )
    instance.query(create_sql)

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME}")
    assert "PATH_ACCESS_DENIED" in error, (
        f"Expected PATH_ACCESS_DENIED but got: {error}"
    )
