import json
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


def wait_for_processed_files_in_log(node, table_name, expected_count):
    """A file row reaches the destination table before the file is committed
    as processed in Keeper, so the s3queue log can lag behind the data."""
    count = 0
    for _ in range(60):
        node.query("SYSTEM FLUSH LOGS")
        count = int(
            node.query(
                f"SELECT uniqExact(file_name) FROM system.s3queue_log "
                f"WHERE table = '{table_name}' AND status = 'Processed'"
            )
        )
        if count == expected_count:
            break
        time.sleep(1)
    return count


def wait_until(predicate, timeout=30, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def get_lock_owner_id(zk, path):
    """The id of the lock owner stored in a bucket lock or in a processing node."""
    try:
        data, _ = zk.get(path)
        return json.loads(data).get("owner_id")
    except (NoNodeError, ValueError):
        return None


def find_processing_node(zk, keeper_path):
    """A processing node which has a lock owner: (path, owner id)."""
    try:
        for child in zk.get_children(f"{keeper_path}/processing"):
            path = f"{keeper_path}/processing/{child}"
            owner_id = get_lock_owner_id(zk, path)
            if owner_id:
                return path, owner_id
    except NoNodeError:
        pass
    return None


def find_bucket_lock(zk, keeper_path, buckets):
    """A bucket lock whose lock owner is alive: (path, owner id)."""
    for bucket in range(buckets):
        path = f"{keeper_path}/buckets/{bucket}/lock"
        owner_id = get_lock_owner_id(zk, path)
        if owner_id and zk.exists(f"{keeper_path}/owners/{owner_id}"):
            return path, owner_id
    return None


def create_slow_queue(
    started_cluster, node, table_name, mode, files, sleep_per_row, settings
):
    """A queue whose materialized view sleeps on every row, so that
    files stay in processing long enough to look at their locks."""
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "max_processed_files_before_commit": 1,
            "polling_min_timeout_ms": 100,
            **settings,
        },
    )
    generate_random_files(started_cluster, files_path, files, start_ind=0, row_num=1)

    node.query(
        f"""
        CREATE TABLE {dst_table_name}
        (column1 UInt32, column2 UInt32, column3 UInt32, _path String)
        ENGINE = MergeTree ORDER BY column1
        """
    )
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {table_name}_mv TO {dst_table_name} AS
        SELECT column1, column2, column3, _path
        FROM {table_name}
        WHERE ignore(sleepEachRow({sleep_per_row})) = 0
        """
    )
    return keeper_path, dst_table_name


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
            # Keep the TTL cleanup away from the test: with TTL = 2 sec it could
            # remove processing nodes of files which take longer than the TTL and
            # bucket locks which stop being refreshed after the provoked ownership loss.
            "cleanup_interval_min_ms": 600000,
            "cleanup_interval_max_ms": 600000,
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

    # uniqExact(_path) above proves at-least-once processing. Prove exactly-once
    # commits via the s3queue log: every file was set as processed exactly once.
    # A duplicates check on the destination table would be flaky by design:
    # a file which was in flight when the batch failed can legitimately
    # re-insert an already inserted row when it is retried.
    assert files_to_generate == wait_for_processed_files_in_log(
        node, table_name, files_to_generate
    )
    assert "" == node.query(
        f"SELECT file_name, count() FROM system.s3queue_log "
        f"WHERE table = '{table_name}' AND status = 'Processed' "
        f"GROUP BY file_name HAVING count() > 1"
    )


def test_lost_bucket_lock_detected_during_release(started_cluster):
    node = started_cluster.instances["instance"]

    if node.is_debug_build() or node.is_built_with_sanitizer():
        pytest.skip(
            "Debug and sanitizer builds abort on the deliberately provoked "
            "logical error about lost bucket lock ownership"
        )

    table_name = f"test_lost_lock_release_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 30

    def get_lost_ownership_events():
        return int(
            node.query(
                "SELECT sum(value) FROM system.events "
                "WHERE event = 'ObjectStorageQueueBucketLockLostOwnership'"
            )
        )

    events_before = get_lost_ownership_events()

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
            # Keep the TTL cleanup away from the test, same as above.
            "cleanup_interval_min_ms": 600000,
            "cleanup_interval_max_ms": 600000,
            "max_processed_files_before_commit": 1,
            "polling_min_timeout_ms": 100,
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
    # With TTL = 2 sec, 2.5 sec of sleep per one-row file keeps all the
    # processing threads inside the pipeline for longer than the TTL,
    # where nothing can refresh the bucket locks.
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {table_name}_mv TO {dst_table_name} AS
        SELECT column1, column2, column3, _path
        FROM {table_name}
        WHERE ignore(sleepEachRow(2.5)) = 0
        """
    )

    # Steal two bucket locks while all the threads are sleeping inside the
    # pipeline and the locks age past the TTL without a refresh. The first
    # stolen lock is detected by the refresh scan, which throws immediately
    # and invalidates the iterator, so the second one is only discovered by
    # release when the invalidated iterator holders are destroyed - the
    # release-time detection this test is about.
    zk = started_cluster.get_kazoo_client("zoo1")
    stolen_lock_paths = []
    for _ in range(100):
        for bucket in zk.get_children(f"{keeper_path}/buckets"):
            lock_path = f"{keeper_path}/buckets/{bucket}/lock"
            if lock_path in stolen_lock_paths:
                continue
            try:
                stat = zk.exists(lock_path)
                # Steal locks acquired at least a second ago, so that the
                # processing threads are certainly sleeping already.
                if stat is None or time.time() - stat.created < 1:
                    continue
                zk.set(lock_path, b"another_server")
                stolen_lock_paths.append(lock_path)
                if len(stolen_lock_paths) == 2:
                    break
            except NoNodeError:
                continue
        if len(stolen_lock_paths) == 2:
            break
        time.sleep(0.2)
    assert 2 == len(stolen_lock_paths)

    # One loss must be detected by the refresh, the other by the release.
    def get_detections():
        lines = [
            line
            for line in node.grep_in_log(
                f"Lost ownership of bucket lock {keeper_path}"
            ).splitlines()
            if "ForcedCriticalErrorsLogger" not in line
        ]
        return (
            [line for line in lines if "current owner:" in line],
            [line for line in lines if "detected during release" in line],
        )

    for _ in range(150):
        refresh_detections, release_detections = get_detections()
        if refresh_detections and release_detections:
            break
        time.sleep(1)
    refresh_detections, release_detections = get_detections()
    assert 1 == len(refresh_detections)
    assert 1 == len(release_detections)
    assert events_before + 2 == get_lost_ownership_events()

    # Return the stolen locks, so that the buckets can be acquired again.
    for lock_path in stolen_lock_paths:
        try:
            data, stat = zk.get(lock_path)
            if data == b"another_server":
                zk.delete(lock_path, version=stat.version)
        except (NoNodeError, BadVersionError):
            pass

    # Streaming must recover and process all the files exactly once.
    def get_processed_count():
        return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

    for _ in range(300):
        if get_processed_count() == files_to_generate:
            break
        time.sleep(1)
    assert get_processed_count() == files_to_generate

    assert files_to_generate == wait_for_processed_files_in_log(
        node, table_name, files_to_generate
    )
    assert "" == node.query(
        f"SELECT file_name, count() FROM system.s3queue_log "
        f"WHERE table = '{table_name}' AND status = 'Processed' "
        f"GROUP BY file_name HAVING count() > 1"
    )


def test_locks_of_killed_server_are_released_after_lock_owner_timeout(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_killed_lock_owner_{uuid.uuid4().hex[:8]}"
    files_to_generate = 10
    keeper_path, dst_table_name = create_slow_queue(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_to_generate,
        sleep_per_row=2,
        settings={
            "buckets": 2,
            "processing_threads_num": 1,
            # The long TTL must not be what releases the locks.
            "persistent_processing_node_ttl_seconds": 21600,
            "lock_owner_timeout_seconds": 5,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 100,
            "s3queue_loading_retries": 10,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")

    bucket_lock = wait_until(lambda: find_bucket_lock(zk, keeper_path, 2), timeout=60)
    assert bucket_lock is not None
    lock_path, owner_id = bucket_lock
    owner_path = f"{keeper_path}/owners/{owner_id}"

    node.stop_clickhouse(kill=True)
    clickhouse_started = False
    try:
        # A killed server does not release anything: its lock and its owner node stay.
        assert get_lock_owner_id(zk, lock_path) == owner_id
        assert zk.exists(owner_path)

        node.start_clickhouse()
        clickhouse_started = True

        # The new server process is another owner. The cleanup removes the owner which is
        # silent for longer than the timeout and after that releases its locks.
        assert wait_until(lambda: not zk.exists(owner_path), timeout=60)

        def get_processed_count():
            return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

        assert wait_until(
            lambda: get_processed_count() == files_to_generate, timeout=150, interval=1
        )
        assert node.contains_in_log(f"Removed lock owner {owner_id} as dead")
        assert node.contains_in_log(f"of dead lock owner {owner_id}")
    finally:
        if not clickhouse_started:
            node.start_clickhouse()


def test_insert_longer_than_lock_owner_timeout_keeps_locks(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_long_insert_{uuid.uuid4().hex[:8]}"
    keeper_path, dst_table_name = create_slow_queue(
        started_cluster,
        node,
        table_name,
        "unordered",
        files=1,
        sleep_per_row=12,
        settings={
            "processing_threads_num": 1,
            "lock_owner_timeout_seconds": 4,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 100,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")

    processing_node = wait_until(lambda: find_processing_node(zk, keeper_path), timeout=30)
    assert processing_node is not None
    processing_path, owner_id = processing_node
    owner_path = f"{keeper_path}/owners/{owner_id}"

    # The insert takes three times longer than the timeout. The owner node is updated by a
    # background task which does not depend on the insert, so the owner stays alive and the
    # cleanup, which runs ten times a second here, does not touch the lock.
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        assert zk.exists(owner_path)
        assert get_lock_owner_id(zk, processing_path) == owner_id
        time.sleep(0.5)

    assert wait_until(
        lambda: int(node.query(f"SELECT count() FROM {dst_table_name}")) == 1, timeout=60
    )
    assert wait_until(lambda: not zk.exists(processing_path), timeout=30)
    assert 1 == wait_for_processed_files_in_log(node, table_name, 1)
    assert not node.contains_in_log(f"Removed lock owner {owner_id} as dead")


def test_table_which_does_not_consume_releases_bucket_locks(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_release_locks_{uuid.uuid4().hex[:8]}"
    files_to_generate = 10
    buckets = 2
    keeper_path, dst_table_name = create_slow_queue(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_to_generate,
        sleep_per_row=2,
        settings={
            "buckets": buckets,
            "processing_threads_num": 1,
            # Nothing here may be released by the cleanup.
            "persistent_processing_node_ttl_seconds": 21600,
            "lock_owner_timeout_seconds": 3600,
            "s3queue_loading_retries": 10,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")

    def has_bucket_locks():
        return any(
            zk.exists(f"{keeper_path}/buckets/{bucket}/lock") for bucket in range(buckets)
        )

    bucket_lock = wait_until(
        lambda: find_bucket_lock(zk, keeper_path, buckets), timeout=60
    )
    assert bucket_lock is not None
    _, owner_id = bucket_lock
    owner_path = f"{keeper_path}/owners/{owner_id}"

    # A stopped table gives its locks back, so that other servers can take the buckets.
    # The server is alive, so its owner node stays.
    node.query(f"SYSTEM STOP {table_name}")
    assert wait_until(lambda: not has_bucket_locks(), timeout=30)
    assert zk.exists(owner_path)

    node.query(f"SYSTEM START {table_name}")
    assert wait_until(lambda: find_bucket_lock(zk, keeper_path, buckets), timeout=60)

    # A detached table releases its locks before the query returns.
    node.query(f"DETACH TABLE {table_name}")
    assert not has_bucket_locks()

    node.query(f"ATTACH TABLE {table_name}")

    def get_processed_count():
        return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

    assert wait_until(
        lambda: get_processed_count() == files_to_generate, timeout=150, interval=1
    )


def test_lock_owner_considered_dead_cannot_commit(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_dead_lock_owner_{uuid.uuid4().hex[:8]}"
    files_to_generate = 3
    keeper_path, dst_table_name = create_slow_queue(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_to_generate,
        sleep_per_row=3,
        settings={
            "processing_threads_num": 1,
            # The cleanup must not remove the owner by itself.
            "lock_owner_timeout_seconds": 3600,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 100,
            "s3queue_loading_retries": 10,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")

    processing_node = wait_until(lambda: find_processing_node(zk, keeper_path), timeout=30)
    assert processing_node is not None
    _, owner_id = processing_node
    owner_path = f"{keeper_path}/owners/{owner_id}"

    # Remove the owner while it processes a file, as the cleanup does with an owner which
    # is silent for too long (a server which is frozen or cannot reach keeper).
    zk.delete(owner_path)

    # The server finds out that it lost its locks, and takes locks again as a new owner.
    assert wait_until(
        lambda: node.contains_in_log(f"Lock owner {owner_id} was removed as dead"),
        timeout=60,
    )

    def has_new_owner():
        owners = zk.get_children(f"{keeper_path}/owners")
        return owners and owner_id not in owners

    assert wait_until(has_new_owner, timeout=60)

    def get_processed_count():
        return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

    assert wait_until(
        lambda: get_processed_count() == files_to_generate, timeout=150, interval=1
    )

    # The removed owner could not commit the file it was processing, so every file is
    # committed as processed exactly once, by an owner which is alive.
    assert files_to_generate == wait_for_processed_files_in_log(
        node, table_name, files_to_generate
    )
    assert "" == node.query(
        f"SELECT file_name, count() FROM system.s3queue_log "
        f"WHERE table = '{table_name}' AND status = 'Processed' "
        f"GROUP BY file_name HAVING count() > 1"
    )
