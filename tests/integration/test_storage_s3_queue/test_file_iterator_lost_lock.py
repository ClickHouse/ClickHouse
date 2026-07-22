import logging
import time
import uuid

import pytest
from kazoo.exceptions import BadVersionError, NoNodeError

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    create_table,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        # The test deliberately provokes a logical error
        # about lost bucket lock ownership.
        cluster.shutdown(ignore_logical_errors=True)


def test_streaming_recovers_after_lost_bucket_lock(started_cluster):
    node = started_cluster.instances["instance"]

    if node.is_debug_build() or node.is_built_with_sanitizer():
        pytest.skip(
            "Debug and sanitizer builds abort on the deliberately provoked "
            "logical error about lost bucket lock ownership"
        )

    table_name = f"test_lost_bucket_lock_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 300

    # With TTL = 2 sec bucket locks are refreshed once they were not
    # refreshed for 0.5 sec, so a stolen lock is detected quickly.
    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "buckets": 3,
            "processing_threads_num": 3,
            "persistent_processing_node_ttl_seconds": 2,
            # Disable the real TTL cleanup (it is active only with persistent
            # processing nodes): with TTL = 2 sec it could remove the locks
            # which stop being refreshed after the provoked ownership loss,
            # making the loss detected more than once.
            "use_persistent_processing_nodes": 0,
            "max_processed_files_before_commit": 1,
            "polling_min_timeout_ms": 100,
            # Files in flight at the moment the lock loss is detected fail
            # together with the batch and must be retried afterwards.
            "s3queue_loading_retries": 10,
        },
    )

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    node.query(
        f"""
        CREATE TABLE {dst_table_name}
        (column1 UInt32, column2 UInt32, column3 UInt32, _path String)
        ENGINE = MergeTree ORDER BY column1
        """
    )
    # Throttle the processing with sleepEachRow, so that it certainly
    # takes long enough to steal a bucket lock in the middle of it.
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {table_name}_mv TO {dst_table_name} AS
        SELECT column1, column2, column3, _path
        FROM {table_name}
        WHERE ignore(sleepEachRow(0.2)) = 0
        """
    )

    def get_processed_count():
        return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

    # Wait until streaming is in progress and holds bucket locks.
    for _ in range(150):
        if get_processed_count() >= files_to_generate // 10:
            break
        time.sleep(1)
    assert get_processed_count() >= files_to_generate // 10

    # Steal one bucket lock, as if it was removed by the TTL cleanup
    # and the bucket was acquired by another server. A single set is
    # atomic, unlike delete + create, so it cannot hit a lock which is
    # concurrently released and re-acquired by the server.
    zk = started_cluster.get_kazoo_client("zoo1")
    stolen_lock_path = None
    for _ in range(10):
        for bucket in zk.get_children(f"{keeper_path}/buckets"):
            lock_path = f"{keeper_path}/buckets/{bucket}/lock"
            try:
                stat = zk.exists(lock_path)
                # Steal a lock acquired at least a second ago, so that the
                # debug ownership check in the BucketHolder constructor
                # is certainly over.
                if stat is None or time.time() - stat.created < 1:
                    continue
                zk.set(lock_path, b"another_server")
                stolen_lock_path = lock_path
                break
            except NoNodeError:
                continue
        if stolen_lock_path:
            break
        time.sleep(1)
    assert stolen_lock_path

    # The refresh must detect the lost ownership exactly once
    # and invalidate the file iterator.
    for _ in range(150):
        if node.contains_in_log("Lost ownership of bucket lock"):
            break
        time.sleep(1)
    assert node.contains_in_log("Lost ownership of bucket lock")

    # Return the stolen lock, so that the bucket can be acquired again.
    # By this time the TTL cleanup could have removed the stolen lock as
    # abandoned and the server could have re-acquired the bucket,
    # so make sure not to remove a lock owned by the server.
    try:
        data, stat = zk.get(stolen_lock_path)
        if data == b"another_server":
            zk.delete(stolen_lock_path, version=stat.version)
    except (NoNodeError, BadVersionError):
        pass

    # Streaming must recover with a fresh file iterator
    # and process all the files.
    for _ in range(300):
        if get_processed_count() == files_to_generate:
            break
        time.sleep(1)
    assert get_processed_count() == files_to_generate

    # The ownership loss must have been detected exactly once: after the
    # iterator invalidation the fresh iterator must not fail on it again.
    # ForcedCriticalErrorsLogger mirrors every logical error into the log,
    # so filter its duplicate of the same single exception out.
    detections = [
        line
        for line in node.grep_in_log("Lost ownership of bucket lock").splitlines()
        if "(processor: " in line and "ForcedCriticalErrorsLogger" not in line
    ]
    assert 1 == len(detections)

    # The profile event must count exactly one ownership loss as well.
    assert 1 == int(
        node.query(
            "SELECT value FROM system.events "
            "WHERE event = 'ObjectStorageQueueBucketLockLostOwnership'"
        )
    )
