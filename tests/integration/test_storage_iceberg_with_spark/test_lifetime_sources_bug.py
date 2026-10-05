import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    create_iceberg_table,
    get_uuid_str,
)

import threading
import time


FAILPOINT = "object_storage_source_pause_before_virtual_columns"


def uptime(instance):
    # Uptime only ever grows while the server keeps running and drops back to nearly zero when it
    # restarts, so comparing two samples tells a survived run from a crashed one. Deriving the start
    # time as `now() - uptime()` instead would be off by a second whenever the two functions, both of
    # them second-granular, are evaluated on different sides of a second boundary. `system.crash_log`
    # is no good either: the table only appears once something has already crashed.
    return int(instance.query("SELECT uptime()"))


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_source_must_not_outlive_storage(started_cluster_iceberg_with_spark, format_version, storage_type):
    """Dropping a table must not pull the storage out from under a reading pipeline.

    `StorageObjectStorageSource::generate` resolves `storage_snapshot->storage.getStorageID()` for every
    chunk, and `StorageSnapshot::storage` is a bare `const IStorage &`: the source keeps the snapshot
    alive, not the storage behind it. If `DROP TABLE` destroys the storage while a pipeline thread sits
    in `generate`, that call locks the mutex of a destroyed `IStorage` and glibc aborts the whole server.
    Staging shows 597 crashes sharing exactly this stack, all of them signal 6 through
    `IStorage::getStorageID` from `StorageObjectStorageSource::generate`, reported for single node
    execution with parallel replicas.

    So the test parks a source inside `generate`, drops the table underneath it, and then lets it wake
    up. Whether `DROP` waits for the reading pipeline is the crux: if it goes through while the source
    is parked, the table lock is not being held for that path, and the wake-up touches freed memory.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_source_must_not_outlive_storage_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    NUM_FILES = 8
    ROWS_PER_FILE = 100

    write_iceberg_from_df(
        spark, generate_data(spark, 0, ROWS_PER_FILE), TABLE_NAME, mode="overwrite", format_version=format_version
    )
    for i in range(1, NUM_FILES):
        write_iceberg_from_df(
            spark,
            generate_data(spark, i * 1000, i * 1000 + ROWS_PER_FILE),
            TABLE_NAME,
            mode="append",
            format_version=format_version,
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    uptime_before = uptime(instance)

    # `_path` is what drags the storage into `generate`, and parallel replicas keep the reading pipeline
    # on the initiator, which is the shape the crashes were reported for.
    select_id = get_uuid_str()
    select = (
        f"SELECT _path, count() FROM {TABLE_NAME} GROUP BY _path "
        "SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, "
        "parallel_replicas_local_plan = 1, cluster_for_parallel_replicas = 'cluster_simple'"
    )

    select_error = []
    drop_error = []
    drop_finished = threading.Event()

    def run_select():
        try:
            instance.query(select, query_id=select_id)
        except Exception as e:  # the query may legitimately die with the table, the server may not
            select_error.append(str(e))

    def run_drop():
        try:
            instance.query(f"DROP TABLE {TABLE_NAME} SYNC")
        except Exception as e:
            drop_error.append(str(e))
        finally:
            drop_finished.set()

    select_thread = threading.Thread(target=run_select)
    drop_thread = threading.Thread(target=run_drop)

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        select_thread.start()

        deadline = time.time() + 60
        running = 0
        while time.time() < deadline:
            running = int(instance.query(
                f"SELECT count() FROM system.processes WHERE query_id = '{select_id}'"
            ))
            if running:
                break
            time.sleep(0.2)

        assert running, "the query never started, so no source is parked in generate()"

        # `system.processes` reports the query from its start, before the pipeline has reached the
        # failpoint, so give the source a moment to actually park there.
        time.sleep(3)

        drop_thread.start()
        # A DROP that returns while the source is parked means nothing held the table for the reading
        # pipeline. Recorded rather than asserted on: the outcome that matters is whether the server
        # survives the wake-up below.
        dropped_under_reader = drop_finished.wait(timeout=10)
    finally:
        # Releases the parked source, which then reaches `storage_snapshot->storage.getStorageID()`.
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    select_thread.join(timeout=180)
    drop_thread.join(timeout=180)
    assert not select_thread.is_alive(), "the source never woke up after the failpoint was released"
    assert not drop_thread.is_alive(), "DROP TABLE never returned"

    assert instance.query("SELECT 1").strip() == "1"
    assert uptime(instance) >= uptime_before, (
        "the server restarted during the test: DROP TABLE destroyed the storage while a source was "
        "parked in StorageObjectStorageSource::generate, and the wake-up locked the mutex of a "
        "destroyed IStorage"
        + (
            " (DROP TABLE returned while the source was still parked, so the reading pipeline was not"
            " holding the table)"
            if dropped_under_reader
            else ""
        )
    )
