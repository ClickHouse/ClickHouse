"""
The full `OPTIMIZE TABLE` of an Iceberg table rewrites the data files applying only position deletes and
writes no replacement for an equality delete manifest, so it must reject a table with a live equality
delete file before writing anything: otherwise the rows it removes would reappear.
"""
import os
import shutil
import tempfile

import avro.datafile
import avro.io

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
WRITE = {"allow_insert_into_iceberg": 1, "iceberg_manifest_min_count_to_compact": 2, **NOCACHE}


def _delete_files(instance, table_path):
    return {
        os.path.basename(path)
        for path in instance.get_files_list_in_container(table_path)
        if path.endswith("-deletes.parquet")
    }


def _manifest_lists_file(manifest_path, file_name):
    with open(manifest_path, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        result = any(os.path.basename(record["data_file"]["file_path"]) == file_name for record in reader)
        reader.close()
    return result


def test_optimize_rejects_equality_deletes(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_optimize_rejects_equality_deletes_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    instance.query(f"CREATE TABLE {table_name} (id Int64) ENGINE = IcebergLocal('{table_path}/', 'Parquet')", settings=WRITE)
    instance.query(f"INSERT INTO {table_name} VALUES (1), (2)", settings=WRITE)
    instance.query(f"INSERT INTO {table_name} VALUES (3)", settings=WRITE)
    instance.query(f"DELETE FROM {table_name} WHERE id = 1", settings=WRITE)
    (first_delete_file,) = _delete_files(instance, table_path)

    # The insert merges the manifests of the first two inserts; then the snapshots preceding it expire,
    # so no retained snapshot summary tells about the first delete.
    instance.query(f"INSERT INTO {table_name} VALUES (4)", settings=WRITE)
    instance.query(
        f"ALTER TABLE {table_name} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)",
        settings={"allow_experimental_expire_snapshots": 1, **WRITE},
    )
    instance.query(f"DELETE FROM {table_name} WHERE id = 3", settings=WRITE)
    assert instance.query(f"SELECT id FROM {table_name} ORDER BY id", settings=NOCACHE) == "2\n4\n"

    # ClickHouse only writes position deletes, so relabel the first one as an equality delete.
    temp_dir = tempfile.mkdtemp()
    try:
        host_path = os.path.join(temp_dir, table_name)
        os.makedirs(host_path, exist_ok=True)
        _download_table_from_container(instance, table_path, host_path)
        _, delete_manifests = _get_manifest_paths_from_chain(host_path, table_path, content_filter=1)
        assert len(delete_manifests) == 2
        (manifest,) = [
            path for path in delete_manifests if _manifest_lists_file(os.path.join(host_path, path), first_delete_file)
        ]
        _modify_avro_file(os.path.join(host_path, manifest), ["data_file", "content"], lambda _: 2)
        _modify_avro_file(os.path.join(host_path, manifest), ["data_file", "equality_ids"], lambda _: [1])
        instance.copy_file_to_container(os.path.join(host_path, manifest), f"{table_path}/{manifest}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    files_before = sorted(instance.get_files_list_in_container(table_path))
    error = instance.query_and_get_error(
        f"OPTIMIZE TABLE {table_name}", settings={"allow_experimental_iceberg_compaction": 1, **NOCACHE}
    )
    assert "NOT_IMPLEMENTED" in error and "live equality delete file" in error, error
    assert sorted(instance.get_files_list_in_container(table_path)) == files_before
