"""An INSERT that loses the commit race must retry with the manifests it already wrote.

The retry regenerates only the manifest list and the metadata file. The snapshot id and the
partition spec of the first attempt are kept, even if the winning commit changed the default spec.
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
    default_upload_directory,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"
FAILPOINT = "iceberg_writes_pause_before_commit"
INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


def manifests_in_storage(cluster, storage_type, table_path):
    """Names of the manifest files currently in storage (manifest lists excluded).

    The downloaded host copy only grows. Files deleted from storage stay on the host,
    so the list returned by the download is used instead of the directory content.
    """
    files = default_download_directory(cluster, storage_type, table_path, table_path)
    return {os.path.basename(f) for f in files if f.endswith(".avro") and "/snap-" not in f}


def read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def latest_metadata(table_path):
    def version(path):
        return int(re.search(r"v(\d+)", os.path.basename(path)).group(1))

    newest = max(glob.glob(f"{table_path}/metadata/*.metadata.json"), key=version)
    with open(newest) as f:
        return json.load(f), version(newest)


def current_manifest_list(table_path):
    """The current snapshot id and its manifest-list entries, keyed by manifest file name."""
    metadata, _ = latest_metadata(table_path)
    snapshot_id = metadata["current-snapshot-id"]
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == snapshot_id)
    entries = read_avro(f"{table_path}/metadata/{os.path.basename(snapshot['manifest-list'])}")
    return snapshot_id, {os.path.basename(unescape_path(e["manifest_path"])): e for e in entries}


def make_readable_by_spark(table_path):
    """ClickHouse writes a file name into version-hint.text, Spark's Hadoop catalog expects a number."""
    _, version = latest_metadata(table_path)
    with open(f"{table_path}/metadata/version-hint.text", "w") as f:
        f.write(str(version))


@pytest.mark.parametrize("conflict", ["insert", "add_partition_field"])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_commit_retry_reuses_manifests(started_cluster_iceberg_with_spark, storage_type, conflict):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_commit_retry_{conflict}_{storage_type}_{get_uuid_str()}"
    table_path = f"{TABLE_ROOT}/{table_name}/"

    # 1. A partitioned table with one committed row.
    #    Spark cannot open a table with no snapshots, because ClickHouse points its `main` ref at snapshot -1.
    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        cluster,
        schema="(a Int64, b String)",
        format_version=2,
        partition_by="(identity(a))",
    )
    instance.query(f"INSERT INTO {table_name} VALUES (0, 'base')", settings=INSERT_SETTINGS)
    expected_rows = ["0\tbase"]

    # 2. Start an insert into three partitions in the background. The failpoint stops it after it
    #    wrote its three manifests, right before the commit.
    paused_insert = f"INSERT INTO {table_name} VALUES (1, 'paused'), (2, 'paused'), (3, 'paused')"
    expected_rows += ["1\tpaused", "2\tpaused", "3\tpaused"]

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        paused_insert_future = executor.submit(instance.query, paused_insert, settings=INSERT_SETTINGS, timeout=120)
        instance.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)

        manifests_before_conflict = manifests_in_storage(cluster, storage_type, table_path)

        # 3. Commit something else while the insert is paused, so that its commit fails.
        if conflict == "insert":
            # The failpoint pauses only the first query, so this insert goes through and commits first.
            instance.query(f"INSERT INTO {table_name} VALUES (1, 'winner'), (4, 'winner')", settings=INSERT_SETTINGS)
            expected_rows += ["1\twinner", "4\twinner"]
        else:
            # Spark works on the host copy of the table, so upload its changes back to storage.
            make_readable_by_spark(table_path)
            spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD b")
            default_upload_directory(cluster, storage_type, table_path, table_path)

        # 4. Release the paused insert. Its commit fails, it retries and succeeds.
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        paused_insert_future.result(timeout=120)
    finally:
        # A failpoint left enabled would pause the next test. Disabling it twice is harmless.
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        executor.shutdown(wait=False)

    # 5. Both writers' rows are visible.
    rows = instance.query(f"SELECT a, b FROM {table_name} ORDER BY a, b").strip()
    assert rows == "\n".join(sorted(expected_rows))

    # 6. The retry reused the manifests. The final snapshot was committed by the paused insert,
    #    so the manifests it added are the ones with its snapshot id.
    manifests_after = manifests_in_storage(cluster, storage_type, table_path)
    snapshot_id, manifest_list = current_manifest_list(table_path)
    manifests_of_paused_insert = {name for name, entry in manifest_list.items() if entry["added_snapshot_id"] == snapshot_id}

    assert len(manifests_of_paused_insert) == 3
    assert manifests_of_paused_insert <= manifests_before_conflict, "the retry wrote new manifests instead of reusing them"
    assert manifests_after == manifest_list.keys(), "a manifest from a failed attempt was left in storage"

    # 7. The reused manifests are consistent with the committed snapshot.
    for name in manifests_of_paused_insert:
        # Written with the spec of the first attempt, even if the default spec changed since.
        assert manifest_list[name]["partition_spec_id"] == 0
        for entry in read_avro(f"{table_path}/metadata/{name}"):
            # The snapshot id was generated once, so it is still the committed one.
            assert entry["snapshot_id"] == snapshot_id
            # Sequence numbers are inherited from the manifest list, which is why the manifest could be reused.
            assert entry["sequence_number"] is None
            assert entry["file_sequence_number"] is None

    # 8. After a spec change, Spark reads the table with manifests of the old spec under the new default spec.
    if conflict == "add_partition_field":
        metadata, _ = latest_metadata(table_path)
        assert metadata["default-spec-id"] == 1
        make_readable_by_spark(table_path)
        assert spark.read.format("iceberg").load(table_path).count() == len(expected_rows)
