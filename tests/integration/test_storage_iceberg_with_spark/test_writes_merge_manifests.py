"""`INSERT` with `iceberg_insert_merge_manifests` merges the small manifests of a partition into its new manifest.

Three inserts into one partition with `iceberg_insert_manifest_min_count_to_merge = 3`:

    INSERT 1 -> [M1]
    INSERT 2 -> [M1, M2]
    INSERT 3 -> [M3]          M3 = new file (ADDED) + files of M1 and M2 (EXISTING)

M1 and M2 stay in storage, the older snapshots still point to them.
"""

import glob
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import avro.datafile
import avro.io

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"
FAILPOINT = "iceberg_writes_pause_before_commit"
INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}
MERGE_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "iceberg_insert_merge_manifests": 1,
    "iceberg_insert_manifest_min_count_to_merge": 3,
}


def read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def latest_metadata(table_path):
    def version(path):
        return int(re.search(r"v(\d+)", os.path.basename(path)).group(1))

    newest = max(glob.glob(f"{table_path}/metadata/*.metadata.json"), key=version)
    with open(newest) as f:
        return json.load(f), version(newest)


def current_manifest_list(cluster, table_path):
    """Downloads the table and returns the current snapshot id and the entries of its manifest list."""
    default_download_directory(cluster, "local", table_path, table_path)
    metadata, _ = latest_metadata(table_path)
    snapshot_id = metadata["current-snapshot-id"]
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == snapshot_id)
    return snapshot_id, read_avro(f"{table_path}/metadata/{os.path.basename(snapshot['manifest-list'])}")


def manifests_in_storage(cluster, table_path):
    """Names of the manifest files currently in storage. The host copy only grows, so it cannot be listed instead."""
    files = default_download_directory(cluster, "local", table_path, table_path)
    return {os.path.basename(f) for f in files if f.endswith(".avro") and "/snap-" not in f}


def read_with_spark(spark, table_path, snapshot_id=None):
    """ClickHouse writes a file name into version-hint.text, Spark's Hadoop catalog expects a number."""
    _, version = latest_metadata(table_path)
    with open(f"{table_path}/metadata/version-hint.text", "w") as f:
        f.write(str(version))
    reader = spark.read.format("iceberg")
    if snapshot_id is not None:
        reader = reader.option("snapshot-id", snapshot_id)
    return sorted(row.v for row in reader.load(table_path).collect())


def test_spark_reads_merged_manifests(started_cluster_iceberg_with_spark):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    table_name = f"test_merge_manifests_{get_uuid_str()}"
    table_path = f"{TABLE_ROOT}/{table_name}/"
    create_iceberg_table("local", instance, table_name, cluster, schema="(p Int64, v Int64)", partition_by="(identity(p))")

    # Two inserts: two manifests, below the threshold of three.
    instance.query(f"INSERT INTO {table_name} VALUES (1, 1)", settings=MERGE_SETTINGS)
    instance.query(f"INSERT INTO {table_name} VALUES (1, 2)", settings=MERGE_SETTINGS)
    snapshot_before_merge, manifest_list = current_manifest_list(cluster, table_path)
    assert [e["content"] for e in manifest_list] == [0, 0]

    # A position delete written before the merge.
    instance.query(f"ALTER TABLE {table_name} DELETE WHERE v = 2", settings=INSERT_SETTINGS)

    # The third insert merges both data manifests into its own one. The delete manifest is kept.
    instance.query(f"INSERT INTO {table_name} VALUES (1, 3)", settings=MERGE_SETTINGS)
    _, manifest_list = current_manifest_list(cluster, table_path)
    assert sorted(e["content"] for e in manifest_list) == [0, 1]

    merged = next(e for e in manifest_list if e["content"] == 0)
    assert merged["added_files_count"] == 1
    assert merged["existing_files_count"] == 2
    assert merged["min_sequence_number"] == 1

    # The carried-over files are EXISTING (0) with the sequence numbers of the inserts that added them.
    # The new file is ADDED (1) and inherits the sequence number of its snapshot.
    entries = read_avro(f"{table_path}/metadata/{os.path.basename(unescape_path(merged['manifest_path']))}")
    assert sorted((e["status"], e["sequence_number"] or 0) for e in entries) == [(0, 1), (0, 2), (1, 0)]

    # The delete still applies to the file it was written for, so `v = 2` stays deleted for both engines.
    assert instance.query(f"SELECT v FROM {table_name} ORDER BY v") == "1\n3\n"
    assert read_with_spark(cluster.spark_session, table_path) == [1, 3]

    # The snapshot before the merge still reads its own manifests.
    assert read_with_spark(cluster.spark_session, table_path, snapshot_before_merge) == [1, 2]


def test_merge_is_planned_again_after_commit_conflict(started_cluster_iceberg_with_spark):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    table_name = f"test_merge_manifests_conflict_{get_uuid_str()}"
    table_path = f"{TABLE_ROOT}/{table_name}/"
    create_iceberg_table("local", instance, table_name, cluster, schema="(p Int64, v Int64)", partition_by="(identity(p))")

    instance.query(f"INSERT INTO {table_name} VALUES (1, 1)", settings=MERGE_SETTINGS)
    instance.query(f"INSERT INTO {table_name} VALUES (1, 2)", settings=MERGE_SETTINGS)

    # The third insert merges M1 and M2, then pauses right before the commit.
    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        paused_insert = executor.submit(
            instance.query, f"INSERT INTO {table_name} VALUES (1, 3)", settings=MERGE_SETTINGS, timeout=120
        )
        instance.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)
        manifests_of_failed_attempt = manifests_in_storage(cluster, table_path)

        # Another insert commits first, with a plain manifest M4.
        instance.query(f"INSERT INTO {table_name} VALUES (1, 4)", settings=INSERT_SETTINGS)

        # The paused insert loses the race and retries against the new parent [M1, M2, M4].
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        paused_insert.result(timeout=120)
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        executor.shutdown(wait=False)

    # The retry merged the manifest of the winning insert too.
    _, manifest_list = current_manifest_list(cluster, table_path)
    assert len(manifest_list) == 1
    assert manifest_list[0]["added_files_count"] == 1
    assert manifest_list[0]["existing_files_count"] == 3

    # The merged manifest of the failed attempt was removed, the one of the retry is new.
    merged_by_retry = os.path.basename(unescape_path(manifest_list[0]["manifest_path"]))
    in_storage = manifests_in_storage(cluster, table_path)
    assert merged_by_retry not in manifests_of_failed_attempt
    assert len(in_storage) == 4, f"expected M1, M2, M4 and {merged_by_retry}, got {in_storage}"
    assert merged_by_retry in in_storage

    assert instance.query(f"SELECT v FROM {table_name} ORDER BY v") == "1\n2\n3\n4\n"
    assert read_with_spark(cluster.spark_session, table_path) == [1, 2, 3, 4]
