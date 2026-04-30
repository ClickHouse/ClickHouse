"""
IcebergLocal path traversal: a manifest entry that uses ../ to escape user_files
must be blocked with PATH_ACCESS_DENIED, not silently followed.
"""
import json
import os
import re
import shutil
import tempfile

import avro.datafile
import avro.io

from helpers.iceberg_utils import get_uuid_str

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}


def _prepare_table_then_corrupt_manifest(
    instance, table_name, table_path, malicious_path_func, file_format_override=None
):
    """Create a valid IcebergLocal table, then rewrite the manifest so that
    `data_file.file_path` points outside `user_files`."""
    instance.query(
        f"CREATE TABLE {table_name} (c0 Int) ENGINE = IcebergLocal('{table_path}/', 'Parquet');",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (42)",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    result = instance.query(f"SELECT c0 FROM {table_name}", settings=NOCACHE)
    assert result.strip() == "42"
    instance.query(f"DROP TABLE {table_name}")

    temp_dir = tempfile.mkdtemp()
    try:
        host_path = os.path.join(temp_dir, table_name)
        os.makedirs(host_path, exist_ok=True)
        _download_table_from_container(instance, table_path, host_path)

        _, manifest_paths = _get_manifest_paths_from_chain(host_path, table_path)
        assert manifest_paths, "No manifest_path in manifest-list"

        for rel_path in manifest_paths:
            local_manifest = os.path.join(host_path, rel_path)
            assert os.path.isfile(local_manifest), f"Manifest not found: {local_manifest}"

            _modify_avro_file(local_manifest, ["data_file", "file_path"], malicious_path_func)
            if file_format_override is not None:
                _modify_avro_file(
                    local_manifest,
                    ["data_file", "file_format"],
                    lambda _: file_format_override,
                )

            instance.copy_file_to_container(local_manifest, f"{table_path}/{rel_path}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _download_table_from_container(instance, table_path, host_path):
    for remote_file in instance.get_files_list_in_container(table_path):
        rel = os.path.relpath(remote_file, table_path)
        local_path = os.path.join(host_path, rel)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if instance.file_exists_in_container(remote_file):
            instance.copy_file_from_container(remote_file, local_path)


def _to_relative_path(path, table_path):
    """Strip `table_path` prefix from an absolute path, returning a relative path."""
    if not path or not path.startswith("/"):
        return path
    base = table_path.rstrip("/")
    if path == base or path.startswith(base + "/"):
        return path[len(base):].lstrip("/")
    return path


def _get_manifest_paths_from_chain(host_path, table_path, content_filter=None):
    """Follow metadata.json -> manifest-list -> manifest_path(s).
    Returns (manifest_list_rel, [manifest_rel, ...]).

    In Iceberg v2 each manifest-list entry carries a ``content`` field
    (0 = data, 1 = deletes).  Pass *content_filter* to keep only entries
    with that value; ``None`` (default) returns all manifests."""
    metadata_dir = os.path.join(host_path, "metadata")
    version_hint_path = os.path.join(metadata_dir, "version-hint.text")
    if os.path.isfile(version_hint_path):
        with open(version_hint_path) as f:
            version = f.read().strip()
        metadata_file = os.path.join(metadata_dir, f"v{version}.metadata.json")
    else:
        meta_files = _find_files(metadata_dir, ".metadata.json")
        if not meta_files:
            raise RuntimeError("No metadata.json found")

        def version_key(p):
            m = re.match(r"v(\d+)", os.path.basename(p), re.IGNORECASE)
            return int(m.group(1)) if m else 0

        metadata_file = max(meta_files, key=version_key)

    with open(metadata_file) as f:
        data = json.load(f)
    snap_id = data.get("current-snapshot-id")
    if not snap_id:
        raise RuntimeError("No current-snapshot-id in metadata")
    snapshot = next((s for s in data.get("snapshots", []) if s.get("snapshot-id") == snap_id), None)
    if not snapshot:
        raise RuntimeError(f"Snapshot {snap_id} not found")
    ml = snapshot.get("manifest-list")
    if not ml:
        raise RuntimeError("Snapshot has no manifest-list")

    ml_rel = _to_relative_path(ml, table_path)
    ml_local = os.path.join(host_path, ml_rel)
    if not os.path.isfile(ml_local):
        raise RuntimeError(f"Manifest list file not found: {ml_local}")

    manifest_paths = []
    with open(ml_local, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        for record in reader:
            if "manifest_path" not in record:
                continue
            if content_filter is not None and record.get("content") != content_filter:
                continue
            manifest_paths.append(_to_relative_path(record["manifest_path"], table_path))
        reader.close()

    return ml_rel, manifest_paths


def _modify_avro_file(avro_path, field_path, modifier_func):
    """Read an Avro file, apply `modifier_func` to the field at `field_path`
    in every record, and write the file back."""
    with open(avro_path, "rb") as f:
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

    with open(avro_path, "wb") as f:
        writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
        for key, value in metadata.items():
            if not key.startswith("avro."):
                writer.set_meta(key, value)
        for record in records:
            writer.append(record)
        writer.close()


def _find_files(directory, suffix):
    result = []
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(suffix):
                result.append(os.path.join(root, f))
    return result


def _assert_delete_path_access_denied(error):
    assert "PATH_ACCESS_DENIED" in error, f"Expected PATH_ACCESS_DENIED, got: {error}"



def test_local_iceberg_path_traversal(started_cluster_iceberg_no_spark):
    """Manifest entry with ../../../../../../../etc/passwd as data_file.file_path
    (format RAWBLOB) must be rejected with PATH_ACCESS_DENIED.

    If the error message instead shows 'No such file or directory'
    or any other error, the security check is broken: the engine attempted
    to open and read the file instead of blocking the path early."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_local_iceberg_path_traversal" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    _prepare_table_then_corrupt_manifest(
        instance,
        table_name,
        table_path,
        lambda _: f"{table_path}/../../../../../../../etc/passwd",
        file_format_override="RAWBLOB",
    )

    path_for_query = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    error = instance.query_and_get_error(
        f"SELECT * FROM icebergLocal(local, path = '{path_for_query}', format='RawBLOB')",
        settings=NOCACHE,
    )
    _assert_delete_path_access_denied(error)


def _corrupt_delete_manifest_and_query(instance, test_name, modify_manifest_fn):
    """Create an IcebergLocal table with position deletes, apply
    *modify_manifest_fn(local_manifest_path, table_path)* to every
    delete manifest, and return the error string from a SELECT."""
    table_name = test_name + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    instance.query(
        f"CREATE TABLE {table_name} (c0 Int) ENGINE = IcebergLocal('{table_path}/', 'Parquet');",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3)",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    instance.query(
        f"DELETE FROM {table_name} WHERE c0 = 1",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    result = instance.query(
        f"SELECT c0 FROM {table_name} ORDER BY c0", settings=NOCACHE
    )
    assert result.strip() == "2\n3"
    instance.query(f"DROP TABLE {table_name}")

    temp_dir = tempfile.mkdtemp()
    try:
        host_path = os.path.join(temp_dir, table_name)
        os.makedirs(host_path, exist_ok=True)
        _download_table_from_container(instance, table_path, host_path)

        _, delete_manifest_paths = _get_manifest_paths_from_chain(
            host_path, table_path, content_filter=1
        )
        assert delete_manifest_paths, "No delete manifest found after DELETE FROM"

        for rel_path in delete_manifest_paths:
            local_manifest = os.path.join(host_path, rel_path)
            assert os.path.isfile(local_manifest), f"Not found: {local_manifest}"
            modify_manifest_fn(local_manifest, table_path)
            instance.copy_file_to_container(
                local_manifest, f"{table_path}/{rel_path}"
            )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return instance.query_and_get_error(
        f"SELECT * FROM icebergLocal(local, path = '{table_path}/')",
        settings=NOCACHE,
    )

def test_local_iceberg_delete_file_path_traversal(started_cluster_iceberg_no_spark):
    """Position-delete file_path with ../ must be rejected via
    validateDeleteFilePaths (not LocalPathValidatingIterator)."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    def corrupt(manifest, table_path):
        _modify_avro_file(
            manifest,
            ["data_file", "file_path"],
            lambda _: f"{table_path}/../../../../../../../etc/passwd",
        )

    _assert_delete_path_access_denied(
        _corrupt_delete_manifest_and_query(instance, "test_local_iceberg_path_traversal", corrupt)
    )


def test_local_iceberg_equality_delete_file_path_traversal(
    started_cluster_iceberg_no_spark,
):
    """Equality-delete file_path with ../ must be rejected via
    validateDeleteFilePaths.  Patches the delete manifest to look like
    an equality delete (content=2, equality_ids=[1])."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    def corrupt(manifest, table_path):
        _modify_avro_file(manifest, ["data_file", "content"], lambda _: 2)
        _modify_avro_file(manifest, ["data_file", "equality_ids"], lambda _: [1])
        _modify_avro_file(
            manifest,
            ["data_file", "file_path"],
            lambda _: f"{table_path}/../../../../../../../etc/passwd",
        )

    _assert_delete_path_access_denied(
        _corrupt_delete_manifest_and_query(instance, "test_local_iceberg_path_traversal", corrupt)
    )