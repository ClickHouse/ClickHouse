"""
An Iceberg manifest-list entry may declare any `manifest_length`. Reading such a table must not
depend on that number: it used to become the metadata cache cell weight and terminate the server.
"""
import os
import shutil
import tempfile

from helpers.iceberg_utils import get_uuid_str
from test_storage_iceberg_no_spark.test_local_path_traversal import (
    _download_table_from_container,
    _get_manifest_paths_from_chain,
    _modify_avro_file,
)

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}
HUGE_MANIFEST_LENGTH = 2**62


def _make_table_with_manifest_length(instance, table_name, table_path, new_length):
    """Build a well-formed IcebergLocal table with the cache off, then rewrite every
    manifest-list entry's `manifest_length`.  new_length=None rewrites the file without
    changing the value."""
    instance.query(
        f"CREATE TABLE {table_name} (c0 Int) ENGINE = IcebergLocal('{table_path}/', 'Parquet');",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3)",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    assert instance.query(f"SELECT count() FROM {table_name}", settings=NOCACHE).strip() == "3"
    instance.query(f"DROP TABLE {table_name}")

    temp_dir = tempfile.mkdtemp()
    try:
        host_path = os.path.join(temp_dir, table_name)
        os.makedirs(host_path, exist_ok=True)
        _download_table_from_container(instance, table_path, host_path)
        manifest_list_rel, _ = _get_manifest_paths_from_chain(host_path, table_path)
        local_manifest_list = os.path.join(host_path, manifest_list_rel)
        _modify_avro_file(
            local_manifest_list,
            ["manifest_length"],
            (lambda v: v) if new_length is None else (lambda _: new_length),
        )
        instance.copy_file_to_container(local_manifest_list, f"{table_path}/{manifest_list_rel}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _read_back(instance, table_path):
    """Read with the metadata cache ON, from a cold cache: the cache-miss path is the only
    one that builds the cell whose weight the declared length used to set."""
    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")
    return (
        instance.query(f"SELECT count() FROM icebergLocal('{table_path}/')").strip(),
        instance.query(f"SELECT sum(c0) FROM icebergLocal('{table_path}/')").strip(),
    )


def test_iceberg_rewritten_manifest_list_is_readable(started_cluster_iceberg_no_spark):
    """Control for the test below: the same Avro round-trip with the declared length
    untouched.  It runs first so that a regression of the fix, which aborts the server and
    makes every later query in the package fail to connect, cannot mask this result."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_manifest_list_roundtrip" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    _make_table_with_manifest_length(instance, table_name, table_path, None)
    assert _read_back(instance, table_path) == ("3", "6")


def test_iceberg_huge_manifest_length_is_readable(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_huge_manifest_length" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    _make_table_with_manifest_length(instance, table_name, table_path, HUGE_MANIFEST_LENGTH)
    assert _read_back(instance, table_path) == ("3", "6")
