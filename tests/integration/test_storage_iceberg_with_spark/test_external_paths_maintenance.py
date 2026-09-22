import json
import os
import shutil

import pytest

from helpers.s3_tools import S3Uploader
from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .external_paths_utils import (
    create_and_upload_table,
    _download_table_for_relocation,
    find_files,
    ALL_ROWS,
    REMOVE_ORPHAN_FILES,
    REMOVE_ORPHAN_FILES_SETTINGS,
    _create_iceberg_s3_table,
    _external_data_files_in_another_bucket,
    _external_data_files_in_the_same_bucket,
    _external_delete_files_in_another_bucket,
    _external_manifest_lists_in_another_bucket,
    external_bucket,
    relocate_manifest_lists_to_bucket,
)


def test_optimize_manifest_with_external_manifest_list(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_optimize_external_ml_{get_uuid_str()}"
    base_bucket = started_cluster_iceberg_with_spark.minio_bucket

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2, 'beta')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = relocate_manifest_lists_to_bucket(started_cluster_iceberg_with_spark, TABLE_NAME, external_bucket(started_cluster_iceberg_with_spark))

    minio_url = f"http://{started_cluster_iceberg_with_spark.minio_host}:{started_cluster_iceberg_with_spark.minio_port}"
    args = f"s3, filename='{base_path}/', format=Parquet, url='{minio_url}/{base_bucket}/'"
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    instance.query(f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergS3({args})")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"

    def count_metadata_files():
        return sum(
            1 for obj in started_cluster_iceberg_with_spark.minio_client.list_objects(base_bucket, prefix=f"{base_path}/metadata/", recursive=True)
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

    assert count_metadata_files() > metadata_files_before

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
    instance.query(f"DROP TABLE {TABLE_NAME}")


@pytest.mark.parametrize(
    "make_external,command,settings,rows",
    [
        pytest.param(
            _external_delete_files_in_another_bucket,
            "OPTIMIZE TABLE {table}",
            {"allow_experimental_iceberg_compaction": 1},
            "1\talpha\n3\tgamma\n",
            id="optimize",
        ),
        pytest.param(
            _external_manifest_lists_in_another_bucket,
            "ALTER TABLE {table} EXECUTE expire_snapshots("
            "expire_before = '2099-12-31 23:59:59', retain_last = 1, retention_period = '1ms')",
            {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 1},
            "1\talpha\n2\tbeta\n",
            id="expire_snapshots",
        ),
        pytest.param(
            _external_data_files_in_another_bucket,
            REMOVE_ORPHAN_FILES,
            REMOVE_ORPHAN_FILES_SETTINGS,
            ALL_ROWS,
            id="remove_orphan_files",
        ),
        pytest.param(
            _external_data_files_in_the_same_bucket,
            REMOVE_ORPHAN_FILES,
            REMOVE_ORPHAN_FILES_SETTINGS,
            ALL_ROWS,
            id="remove_orphan_files_same_bucket",
        ),
    ],
)


def test_maintenance_commands_reject_external_paths(started_cluster_iceberg_with_spark, make_external, command, settings, rows):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_rejects_external_{get_uuid_str()}"
    base_path, external_bucket, external_prefix = make_external(started_cluster_iceberg_with_spark, TABLE_NAME)

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    def external_objects():
        return sorted(
            obj.object_name for obj in
            started_cluster_iceberg_with_spark.minio_client.list_objects(external_bucket, prefix=external_prefix, recursive=True))

    objects_before = external_objects()
    assert objects_before, "The files should have been relocated outside the table directory"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == rows

    error = instance.query_and_get_error(command.format(table=TABLE_NAME), settings=settings)
    assert "outside the table's base directory" in error

    assert external_objects() == objects_before
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == rows

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")


@pytest.mark.parametrize(
    "write_query,rows_after",
    [
        pytest.param("INSERT INTO {table} VALUES (4, 'delta')", ALL_ROWS + "4\tdelta\n", id="insert"),
        pytest.param("ALTER TABLE {table} DELETE WHERE id = 2", "1\talpha\n3\tgamma\n", id="mutation"),
    ],
)


def test_write_with_external_manifest_list(started_cluster_iceberg_with_spark, write_query, rows_after):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_write_external_ml_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = relocate_manifest_lists_to_bucket(started_cluster_iceberg_with_spark, TABLE_NAME, external_bucket(started_cluster_iceberg_with_spark))

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)

    instance.query(write_query.format(table=TABLE_NAME), settings={"allow_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == rows_after
    instance.query(f"DROP TABLE {TABLE_NAME}")


@pytest.mark.parametrize("external_history_ref", ["manifest_list", "statistics"])
def test_remove_orphan_files_ignores_deleted_external_history(started_cluster_iceberg_with_spark, external_history_ref):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_orphan_history_{external_history_ref}_{get_uuid_str()}"
    history_bucket = external_bucket(started_cluster_iceberg_with_spark)

    # Keep an expired snapshot reachable only through historical metadata.
    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2, 'beta')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    temp_dir, host_path, base_path = _download_table_for_relocation(started_cluster_iceberg_with_spark, TABLE_NAME)
    metadata_dir = os.path.join(host_path, "metadata")
    current_metadata_file = sorted(find_files(metadata_dir, ".metadata.json"))[-1]
    with open(current_metadata_file) as f:
        current = json.load(f)

    external_key = f"{base_path}/metadata/history_external"
    external_path = f"s3a://{history_bucket}/{external_key}"

    history = json.loads(json.dumps(current))
    assert len(history["snapshots"]) >= 2, history["snapshots"]
    older_snapshot = history["snapshots"][0]
    if external_history_ref == "manifest_list":
        external_source = next(
            f for f in find_files(metadata_dir, ".avro")
            if os.path.basename(f).startswith("snap-") and str(older_snapshot["snapshot-id"]) in os.path.basename(f))
        older_snapshot["manifest-list"] = external_path
    else:
        # Statistics contents are never read during traversal.
        external_source = current_metadata_file
        history["statistics"] = [{
            "snapshot-id": older_snapshot["snapshot-id"],
            "statistics-path": external_path,
            "file-size-in-bytes": 1,
            "file-footer-size-in-bytes": 1,
            "blob-metadata": [],
        }]

    # Use a lower version in the existing naming scheme so this history cannot become the table head.
    assert current.get("metadata-log"), "expected a metadata-log entry to copy the path spelling from"
    log_entry = dict(current["metadata-log"][-1])
    history_name = "v0.metadata.json"
    history_key = f"{base_path}/metadata/{history_name}"
    log_entry["metadata-file"] = log_entry["metadata-file"].rsplit("/", 1)[0] + "/" + history_name
    current["metadata-log"] = [log_entry]
    current["snapshots"] = [s for s in current["snapshots"] if s["snapshot-id"] == current["current-snapshot-id"]]

    history_file = os.path.join(host_path, history_name)
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)
    with open(current_metadata_file, "w") as f:
        json.dump(current, f, indent=2)

    uploader = started_cluster_iceberg_with_spark.default_s3_uploader
    uploader.upload_file(history_file, history_key)
    uploader.upload_file(current_metadata_file, f"{base_path}/metadata/{os.path.basename(current_metadata_file)}")
    S3Uploader(started_cluster_iceberg_with_spark.minio_client, history_bucket).upload_file(external_source, external_key)

    shutil.rmtree(temp_dir)

    _create_iceberg_s3_table(started_cluster_iceberg_with_spark, TABLE_NAME, base_path)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n"

    error = instance.query_and_get_error(
        REMOVE_ORPHAN_FILES.format(table=TABLE_NAME), settings=REMOVE_ORPHAN_FILES_SETTINGS)
    assert "outside the table's base directory" in error

    started_cluster_iceberg_with_spark.minio_client.remove_object(history_bucket, external_key)

    instance.query(REMOVE_ORPHAN_FILES.format(table=TABLE_NAME), settings=REMOVE_ORPHAN_FILES_SETTINGS)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id") == "1\talpha\n2\tbeta\n"

    instance.query(f"DROP TABLE {TABLE_NAME} SYNC")
