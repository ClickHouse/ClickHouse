import io
import time

import pytest
from minio.error import S3Error

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

writer = cluster.add_instance(
    "writer",
    main_configs=["configs/writer.xml"],
    with_minio=True,
    stay_alive=True,
)

reader = cluster.add_instance(
    "reader",
    main_configs=["configs/reader.xml"],
    with_minio=True,
    stay_alive=True,
)

SNAPSHOT_KEY = "data/snapshot/__meta/snapshot.bin"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def snapshot_etag(key=SNAPSHOT_KEY):
    try:
        return cluster.minio_client.stat_object(cluster.minio_bucket, key).etag
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise


def get_snapshot(key=SNAPSHOT_KEY):
    response = cluster.minio_client.get_object(cluster.minio_bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def put_snapshot(data, key=SNAPSHOT_KEY):
    cluster.minio_client.put_object(cluster.minio_bucket, key, io.BytesIO(data), len(data))


def event(node, name):
    return int(node.query(f"SELECT value FROM system.events WHERE event = '{name}'").strip() or 0)


def wait_for(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        if condition():
            return
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.5)


def test_snapshot_is_written_and_loaded_on_restart(start_cluster):
    writer.query("DROP TABLE IF EXISTS t_snapshot SYNC")
    writer.query("CREATE TABLE t_snapshot (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot'")

    # The snapshot is rewritten right after every commit by default.
    etag_after_create = snapshot_etag()
    assert etag_after_create is not None
    writer.query("INSERT INTO t_snapshot SELECT number FROM numbers(10)")
    etag_after_first_insert = snapshot_etag()
    assert etag_after_first_insert != etag_after_create
    assert event(writer, "DiskPlainRewritableSnapshotWritten") > 0

    writer.query("INSERT INTO t_snapshot SELECT number FROM numbers(10, 10)")
    assert snapshot_etag() != etag_after_first_insert

    # On startup the state comes from the snapshot, and the object storage listing
    # confirms that nothing changed after it was written.
    writer.restart_clickhouse()
    assert event(writer, "DiskPlainRewritableSnapshotRead") >= 1
    assert writer.query("SELECT count(), sum(id) FROM t_snapshot") == "20\t190\n"
    assert not writer.contains_in_log("had changes after the snapshot")

    # The snapshot is a copy of the state: the disk works without it and recreates it.
    writer.stop_clickhouse()
    cluster.minio_client.remove_object(cluster.minio_bucket, SNAPSHOT_KEY)
    writer.start_clickhouse()
    assert writer.query("SELECT count(), sum(id) FROM t_snapshot") == "20\t190\n"
    assert snapshot_etag() is not None

    # Everything was removed from the disk: the snapshot is removed too.
    writer.query("DROP TABLE t_snapshot SYNC")
    assert snapshot_etag() is None
    assert len(list(cluster.minio_client.list_objects(cluster.minio_bucket, "data/snapshot/", recursive=True))) == 0


def test_stale_snapshot_is_reconciled_with_listing(start_cluster):
    writer.query("DROP TABLE IF EXISTS t_stale SYNC")
    writer.query("DROP TABLE IF EXISTS t_stale_removed SYNC")
    writer.query("CREATE TABLE t_stale (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot'")
    writer.query("INSERT INTO t_stale SELECT number FROM numbers(10)")

    writer.query("CREATE TABLE t_stale_removed (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot'")
    writer.query("INSERT INTO t_stale_removed SELECT number FROM numbers(10)")
    removed_table_uuid = writer.query("SELECT uuid FROM system.tables WHERE database = 'default' AND table = 't_stale_removed'").strip()
    stale_snapshot = get_snapshot()

    # After the saved snapshot: new parts are written, and the directories of a table are removed.
    writer.query("INSERT INTO t_stale SELECT number FROM numbers(10, 10)")
    writer.query("INSERT INTO t_stale SELECT number FROM numbers(20, 10)")
    writer.query("DROP TABLE t_stale_removed SYNC")
    assert writer.query("SELECT count(), sum(id) FROM t_stale") == "30\t435\n"

    # Simulate a server that crashed before writing the latest snapshot.
    writer.stop_clickhouse()
    put_snapshot(stale_snapshot)
    stale_etag = snapshot_etag()
    writer.start_clickhouse()

    assert event(writer, "DiskPlainRewritableSnapshotRead") >= 1
    assert writer.contains_in_log("had changes after the snapshot")
    assert writer.query("SELECT count(), sum(id) FROM t_stale") == "30\t435\n"
    assert writer.query(f"SELECT count() FROM system.remote_data_paths WHERE disk_name = 'disk_snapshot' AND local_path LIKE '%{removed_table_uuid}%'") == "0\n"
    # The reconciled state is published.
    assert snapshot_etag() != stale_etag

    writer.query("DROP TABLE t_stale SYNC")


def test_readonly_replica_uses_snapshot(start_cluster):
    writer.query("DROP TABLE IF EXISTS t_reader SYNC")
    writer.query("CREATE TABLE t_reader (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot'")
    writer.query("INSERT INTO t_reader SELECT number FROM numbers(10)")
    table_uuid = writer.query("SELECT uuid FROM system.tables WHERE database = 'default' AND table = 't_reader'").strip()

    # The read-only disk loads the state from the snapshot on startup, without listing the object storage.
    reader.query("DROP TABLE IF EXISTS t_reader SYNC")
    reader.restart_clickhouse()
    assert event(reader, "DiskPlainRewritableSnapshotRead") >= 1
    assert event(reader, "DiskPlainRewritableSnapshotWritten") == 0

    reads_before = event(reader, "DiskPlainRewritableSnapshotRead")
    reader.query(f"ATTACH TABLE t_reader UUID '{table_uuid}' (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot_readonly', refresh_parts_interval = 1")
    assert reader.query("SELECT count(), sum(id) FROM t_reader") == "10\t45\n"

    # The changes of the writer become visible through the snapshot.
    writer.query("INSERT INTO t_reader SELECT number FROM numbers(10, 10)")
    wait_for(lambda: reader.query("SELECT count(), sum(id) FROM t_reader") == "20\t190\n")
    assert event(reader, "DiskPlainRewritableSnapshotRead") > reads_before

    # Without changes, the periodic refresh finds the same ETag and does nothing.
    unchanged_before = event(reader, "DiskPlainRewritableSnapshotUnchanged")
    wait_for(lambda: event(reader, "DiskPlainRewritableSnapshotUnchanged") > unchanged_before)
    assert event(reader, "DiskPlainRewritableSnapshotWritten") == 0

    reader.query("DROP TABLE t_reader SYNC")
    writer.query("DROP TABLE t_reader SYNC")


def test_delayed_snapshot_write(start_cluster):
    key = "data/snapshot_delayed/__meta/snapshot.bin"

    writer.query("DROP TABLE IF EXISTS t_delayed SYNC")
    writer.query("CREATE TABLE t_delayed (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'snapshot_delayed'")
    writer.query("INSERT INTO t_delayed SELECT number FROM numbers(10)")

    # Written in the background after the delay.
    wait_for(lambda: snapshot_etag(key) is not None)
    etag = snapshot_etag(key)

    # The latest changes are written on shutdown, and loaded on the next start.
    writer.query("INSERT INTO t_delayed SELECT number FROM numbers(10, 10)")
    writer.restart_clickhouse()
    assert snapshot_etag(key) != etag
    assert writer.query("SELECT count(), sum(id) FROM t_delayed") == "20\t190\n"
    assert event(writer, "DiskPlainRewritableSnapshotRead") >= 1

    writer.query("DROP TABLE t_delayed SYNC")
    wait_for(lambda: snapshot_etag(key) is None)


def test_snapshot_disabled(start_cluster):
    writer.query("DROP TABLE IF EXISTS t_disabled SYNC")
    writer.query("CREATE TABLE t_disabled (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'no_snapshot'")
    writer.query("INSERT INTO t_disabled SELECT number FROM numbers(10)")
    assert snapshot_etag("data/no_snapshot/__meta/snapshot.bin") is None

    writer.query("DROP TABLE t_disabled SYNC")
