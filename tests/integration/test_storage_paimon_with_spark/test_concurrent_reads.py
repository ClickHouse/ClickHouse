"""Customer journey: readers keep serving while the writer commits.

A reader thread hammers the table function (fresh metadata every query) while
the writer commits snapshots one by one; uploads happen in commit order
(paimon_utils.upload_paimon_table), matching how real Paimon writers commit.

What this verifies: given a commit-ordered writer (like real Paimon writers),
readers never error and never observe an inconsistent count during continuous
commits. Deterministic torn-commit injection would require an out-of-order
writer and is out of scope here. The exact interleavings are stochastic, but
overlap is enforced (at least one read completes between consecutive
commits) and the assertions cannot false-positive.

test_immutable_objects_uploaded_once pins the writer property the reader
depends on, without needing the concurrency to interleave.
"""

import threading
import time

import pytest

from helpers.paimon_utils import (
    create_spark_paimon_table,
    get_creation_expression,
    get_uuid_str,
    paimon_table_dir,
    spark_table,
    upload_paimon_table,
)

COMMITS = 8
ROWS_PER_COMMIT = 5


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_reads_during_commits(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_concurrent_reads_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "id BIGINT, val STRING")
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} "
        f"SELECT id, 'x' FROM range({ROWS_PER_COMMIT})"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    count_query = f"SELECT count() FROM {table_expr}"
    valid_counts = {ROWS_PER_COMMIT * k for k in range(1, COMMITS + 2)}

    errors = []
    observed = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                observed.append(int(instance.query(count_query, timeout=120).strip()))
            except Exception as e:
                errors.append(str(e))
                return

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    try:
        for i in range(1, COMMITS + 1):
            spark.sql(
                f"INSERT INTO {spark_table(table_name)} "
                f"SELECT id + {i * ROWS_PER_COMMIT}, 'x' FROM range({ROWS_PER_COMMIT})"
            )
            upload_paimon_table(started_cluster, storage_type, table_name)
            # At least one read must complete between consecutive commits:
            # without this, scheduling may let every commit finish before the
            # reader ever runs, and the test degenerates into reading a
            # fully-written table without any read/commit overlap.
            reads_before = len(observed)
            deadline = time.monotonic() + 120
            while len(observed) <= reads_before and not errors:
                assert (
                    time.monotonic() < deadline
                ), "the reader completed no query after a commit"
                time.sleep(0.05)
    finally:
        stop.set()
        reader_thread.join(timeout=120)

    assert not reader_thread.is_alive(), "the reader thread never exited"
    assert errors == []
    assert observed, "the reader thread never completed a query"
    torn = [count for count in observed if count not in valid_counts]
    assert torn == [], f"reader observed torn commits: {torn}"
    assert (
        instance.query(count_query).strip()
        == str(ROWS_PER_COMMIT * (COMMITS + 1))
    )


def _remote_generations(started_cluster, storage_type, table_name):
    """Map remote key -> storage-side generation token for the table's objects.

    The token must advance on any re-upload, including a byte-identical one.
    On azure that is the ETag, which is per-generation. On s3 the ETag is a
    content MD5 and an identical re-upload keeps it, so `last_modified` is used
    there instead (microsecond precision in MinIO).

    Keys come from the client's own listing because the two backends normalize
    them differently: MinIO strips the leading '/' that Azurite keeps.
    """
    remote_dir = "var/lib/clickhouse/user_files" + paimon_table_dir(table_name)
    if storage_type == "s3":
        return {
            obj.object_name: obj.last_modified
            for obj in started_cluster.minio_client.list_objects(
                started_cluster.minio_bucket, prefix=remote_dir, recursive=True
            )
        }
    return {
        blob.name: blob.etag
        for blob in started_cluster.container_client.list_blobs(
            name_starts_with="/" + remote_dir
        )
    }


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_immutable_objects_uploaded_once(
    started_cluster_paimon_with_spark, storage_type
):
    """A committed Paimon object is never re-uploaded, so its storage-side
    generation stays put while a reader has it pinned.

    Re-uploading it is not remotely idempotent: it mints a new generation and a
    reader validating the one it saw fails with FILE_CHANGED_DURING_READ. The
    snapshot hint is the positive control: it is the one file a Paimon writer
    does rewrite, so its generation must advance.
    """
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_upload_once_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "id BIGINT, val STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id, 'x' FROM range(3)")
    upload_paimon_table(started_cluster, storage_type, table_name)
    before = _remote_generations(started_cluster, storage_type, table_name)

    # Spelled out rather than reusing the helper's own predicate, so that the
    # two cannot agree on a wrong definition of "hint".
    hints = {
        key
        for key in before
        if key.endswith(("snapshot/LATEST", "snapshot/EARLIEST"))
    }
    assert any(key.endswith("snapshot/LATEST") for key in hints), sorted(before)
    committed = {key: gen for key, gen in before.items() if key not in hints}
    assert any(key.endswith(".parquet") for key in committed), sorted(committed)

    spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id + 3, 'x' FROM range(3)")
    upload_paimon_table(started_cluster, storage_type, table_name)
    after = _remote_generations(started_cluster, storage_type, table_name)

    changed = [key for key, gen in committed.items() if after.get(key) != gen]
    assert changed == [], f"committed objects were re-uploaded: {changed}"
    rewritten = [key for key in hints if after.get(key) != before[key]]
    assert rewritten, f"no snapshot hint was rewritten: {sorted(hints)}"
