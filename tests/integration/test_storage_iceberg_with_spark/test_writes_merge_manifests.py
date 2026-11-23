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
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"
FAILPOINT = "iceberg_writes_pause_before_commit"

MERGE_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "iceberg_insert_merge_manifests": 1,
    "iceberg_insert_manifest_min_count_to_merge": 3,
}

DATA = 0
DELETES = 1
ADDED = 1
EXISTING = 0


class IcebergTable:
    """A ClickHouse-written Iceberg table in local storage, with helpers to look at its metadata from the host."""

    def __init__(self, cluster, prefix):
        self.cluster = cluster
        self.node = cluster.instances["node1"]
        self.name = f"{prefix}_{get_uuid_str()}"
        self.path = f"{TABLE_ROOT}/{self.name}"
        create_iceberg_table("local", self.node, self.name, cluster, schema="(p Int64, v Int64)", partition_by="(identity(p))")

    def insert(self, values, settings=MERGE_SETTINGS):
        self.node.query(f"INSERT INTO {self.name} VALUES {values}", settings=settings)

    def delete(self, condition):
        self.node.query(f"ALTER TABLE {self.name} DELETE WHERE {condition}", settings={"allow_insert_into_iceberg": 1})

    def values(self):
        return [int(v) for v in self.node.query(f"SELECT v FROM {self.name} ORDER BY v").split()]

    def download(self):
        """The names of the files currently in storage. The host copy only grows, so it cannot be listed instead."""
        files = default_download_directory(self.cluster, "local", self.path + "/", self.path + "/")
        return {os.path.basename(f) for f in files}

    def manifests_in_storage(self):
        return {f for f in self.download() if f.endswith(".avro") and not f.startswith("snap-")}

    def metadata(self):
        newest = max(glob.glob(f"{self.path}/metadata/*.metadata.json"), key=self._version)
        with open(newest) as f:
            return json.load(f)

    def current_snapshot_id(self):
        self.download()
        return self.metadata()["current-snapshot-id"]

    def manifest_list(self):
        """The manifest-list entries of the current snapshot."""
        self.download()
        metadata = self.metadata()
        snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
        return read_avro(self._local(snapshot["manifest-list"]))

    def manifest_entries(self, manifest_list_entry):
        return read_avro(self._local(manifest_list_entry["manifest_path"]))

    def read_with_spark(self, snapshot_id=None):
        """The values of `v` as Spark sees them, optionally at an older snapshot."""
        with open(f"{self.path}/metadata/version-hint.text", "w") as f:
            f.write(str(self._version(max(glob.glob(f"{self.path}/metadata/*.metadata.json"), key=self._version))))
        reader = self.cluster.spark_session.read.format("iceberg")
        if snapshot_id is not None:
            reader = reader.option("snapshot-id", snapshot_id)
        return sorted(row.v for row in reader.load(self.path).collect())

    def _local(self, path_in_metadata):
        return f"{self.path}/metadata/{os.path.basename(unescape_path(path_in_metadata))}"

    @staticmethod
    def _version(path):
        return int(re.search(r"v(\d+)", os.path.basename(path)).group(1))


def read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def test_spark_reads_merged_manifests(started_cluster_iceberg_with_spark):
    table = IcebergTable(started_cluster_iceberg_with_spark, "test_merge_manifests")

    # Two inserts: two manifests, below the threshold of three.
    table.insert("(1, 1)")
    table.insert("(1, 2)")
    snapshot_before_merge = table.current_snapshot_id()
    assert [e["content"] for e in table.manifest_list()] == [DATA, DATA]

    # A position delete written before the merge.
    table.delete("v = 2")

    # The third insert merges both data manifests into its own one. The delete manifest is kept.
    table.insert("(1, 3)")
    manifest_list = table.manifest_list()
    assert sorted(e["content"] for e in manifest_list) == [DATA, DELETES]

    merged = next(e for e in manifest_list if e["content"] == DATA)
    assert (merged["added_files_count"], merged["existing_files_count"]) == (1, 2)
    assert merged["min_sequence_number"] == 1

    # The carried-over files keep the sequence numbers of the inserts that added them, the new one inherits its own.
    entries = sorted(table.manifest_entries(merged), key=lambda e: e["sequence_number"] or float("inf"))
    assert [(e["status"], e["sequence_number"]) for e in entries] == [(EXISTING, 1), (EXISTING, 2), (ADDED, None)]

    # The delete still applies to the file it was written for, so `v = 2` stays deleted for both engines.
    assert table.values() == [1, 3]
    assert table.read_with_spark() == [1, 3]

    # The snapshot before the merge still reads its own manifests.
    assert table.read_with_spark(snapshot_before_merge) == [1, 2]


def test_merge_is_planned_again_after_commit_conflict(started_cluster_iceberg_with_spark):
    table = IcebergTable(started_cluster_iceberg_with_spark, "test_merge_manifests_conflict")
    node = table.node

    table.insert("(1, 1)")
    table.insert("(1, 2)")

    # The third insert merges M1 and M2, then pauses right before the commit.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        paused = executor.submit(table.insert, "(1, 3)")
        node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)
        manifests_of_failed_attempt = table.manifests_in_storage()

        # Another insert commits first. It is below the threshold on its own, so it adds a plain manifest M4.
        table.insert("(1, 4)", settings={"allow_insert_into_iceberg": 1})

        # The paused insert loses the race and retries against the new parent [M1, M2, M4].
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        paused.result(timeout=120)
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        executor.shutdown(wait=False)

    # The retry merged the manifest of the winning insert too.
    manifest_list = table.manifest_list()
    assert len(manifest_list) == 1
    assert (manifest_list[0]["added_files_count"], manifest_list[0]["existing_files_count"]) == (1, 3)

    # The merged manifest of the failed attempt was removed, the one of the retry is new.
    in_storage = table.manifests_in_storage()
    merged_by_retry = os.path.basename(unescape_path(manifest_list[0]["manifest_path"]))
    assert merged_by_retry in in_storage
    assert merged_by_retry not in manifests_of_failed_attempt
    assert len(in_storage) == 4, "M1, M2, M4 and the merged manifest of the retry"

    assert table.values() == [1, 2, 3, 4]
    assert table.read_with_spark() == [1, 2, 3, 4]
