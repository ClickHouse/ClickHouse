"""A `plain_rewritable` disk commits a removal in the metadata first and deletes the objects afterwards.
If the server is killed in between, the objects must not stay in the bucket forever (issue #114051):
they are kept under reserved names and reclaimed when the metadata is loaded on the next start.
"""

import concurrent.futures
import io
import threading
import time

import pytest
from minio.error import S3Error

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/storage_conf.xml",
        "configs/drop_table_immediately.xml",
    ],
    with_minio=True,
    stay_alive=True,
)

# The disk endpoint is `http://minio1:9001/root/data/`.
KEY_PREFIX = "data/"
# `PlainRewritableLayout::REMOVED_NAME_PREFIX`
REMOVED_NAME_PREFIX = "__removed."
# `PlainRewritableLayout::constructTombstoneMarkerKey`
TOMBSTONE_KEY_PREFIX = KEY_PREFIX + "__meta/__tombstone/"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def list_keys():
    return sorted(
        obj.object_name
        for obj in cluster.minio_client.list_objects(
            cluster.minio_bucket, KEY_PREFIX, recursive=True
        )
    )


def key_exists(key):
    try:
        cluster.minio_client.stat_object(cluster.minio_bucket, key)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def put_key(key, data):
    cluster.minio_client.put_object(
        cluster.minio_bucket, key, io.BytesIO(data), len(data)
    )


def remove_key(key):
    cluster.minio_client.remove_object(cluster.minio_bucket, key)


def read_key(key):
    response = cluster.minio_client.get_object(cluster.minio_bucket, key)
    try:
        return response.read().decode()
    finally:
        response.close()
        response.release_conn()


def tombstone_markers(keys):
    """A removal marks the reserved name it uses, and only a marked name is reclaimed."""
    return [key for key in keys if key.startswith(TOMBSTONE_KEY_PREFIX)]


def has_removed_directory(keys):
    """`RemoveRecursive` rewrites `prefix.path` of every directory of the subtree to a path under a reserved name."""
    return any(
        read_key(key).startswith(REMOVED_NAME_PREFIX)
        for key in keys
        if key.endswith("/prefix.path")
    )


def has_removed_file_backup(keys):
    """The removal of a file keeps a backup copy under a reserved name in `__root` until it is finalized."""
    return any(f"/__root/{REMOVED_NAME_PREFIX}" in key for key in keys)


def has_removed_directory_without_data(keys):
    """`finalize` deletes the data objects of the subtree first, and the `prefix.path` objects that make it
    discoverable only once all of them are gone, so in between the subtree is still reclaimable.
    """
    removed_remote_names = {
        key.split("/")[-2]
        for key in keys
        if key.endswith("/prefix.path")
        and read_key(key).startswith(REMOVED_NAME_PREFIX)
    }
    if not removed_remote_names:
        return False

    return not any(
        key.startswith(f"{KEY_PREFIX}{remote_name}/")
        for remote_name in removed_remote_names
        for key in keys
    )


def wait_failpoint_paused(failpoint, timeout=60):
    """`SYSTEM WAIT FAILPOINT ... PAUSE` blocks until some thread parks at the failpoint,
    so it runs on a worker thread that is abandoned if the failpoint is never reached.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(node.query, f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    done, _ = concurrent.futures.wait([future], timeout=timeout)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError(f"failpoint {failpoint} was not reached within {timeout}s")
    pool.shutdown(wait=False)
    future.result()


def wait_for_keys_to_disappear(keys, timeout=60):
    """A disk is loaded when it is first used, so the reclamation can happen a bit after the start."""
    deadline = time.time() + timeout
    remaining = [key for key in keys if key_exists(key)]
    while remaining and time.time() < deadline:
        time.sleep(0.5)
        remaining = [key for key in remaining if key_exists(key)]
    assert remaining == [], f"objects remain: {remaining}"


def wait_for_empty_prefix(timeout=60):
    deadline = time.time() + timeout
    keys = list_keys()
    while keys and time.time() < deadline:
        time.sleep(0.5)
        keys = list_keys()
    assert keys == [], f"objects remain under {KEY_PREFIX}: {keys}"


@pytest.mark.parametrize(
    "failpoint, num_parts, is_removal_in_progress",
    [
        # Removing a part: its directory is renamed under a reserved name, then its objects are deleted.
        pytest.param(
            "plain_object_storage_pause_before_remove_recursive_finalize",
            3,
            has_removed_directory,
            id="remove_recursive",
        ),
        # The same, but in between the two passes of `finalize`: the data objects of the subtree are already
        # deleted, and the `prefix.path` objects that make it discoverable are about to be deleted.
        pytest.param(
            "plain_object_storage_pause_before_remove_recursive_metadata",
            3,
            has_removed_directory_without_data,
            id="remove_recursive_metadata",
        ),
        # Removing `format_version.txt`: a backup copy is kept until the removal is finalized.
        pytest.param(
            "plain_object_storage_pause_before_unlink_file_finalize",
            0,
            has_removed_file_backup,
            id="unlink_file",
        ),
    ],
)
def test_drop_table_killed_before_finalize(
    failpoint, num_parts, is_removal_in_progress
):
    node.query("DROP TABLE IF EXISTS t SYNC")
    wait_for_empty_prefix()

    node.query(
        "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_plain_rewritable'"
    )
    # A background merge would remove parts on its own and reach the failpoint instead of the DROP.
    node.query("SYSTEM STOP MERGES t")
    for i in range(num_parts):
        node.query(f"INSERT INTO t VALUES ({i})")
    assert int(node.query("SELECT count() FROM t")) == num_parts
    assert list_keys() != []

    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

    def drop_table():
        try:
            node.query("DROP TABLE t SYNC")
        except Exception:
            # The server is killed while the query is running.
            pass

    drop_thread = threading.Thread(target=drop_table)
    drop_thread.start()
    try:
        wait_failpoint_paused(failpoint)
        # The removal is committed in the metadata but the objects are still there, under a reserved name.
        keys = list_keys()
        assert is_removal_in_progress(keys)
        # The name is marked as a leftover of a removal, which is what makes it reclaimable.
        assert tombstone_markers(keys) != []
        node.stop_clickhouse(kill=True)
    finally:
        drop_thread.join()

    # The killed process left everything behind, marker included.
    keys = list_keys()
    assert is_removal_in_progress(keys)
    assert tombstone_markers(keys) != []

    node.start_clickhouse()

    # The objects under the reserved names are deleted while loading the metadata, and the table found
    # in `metadata_dropped` is dropped again, removing whatever the killed process did not get to.
    wait_for_empty_prefix()
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't'"
        )
        == "0\n"
    )
    assert node.contains_in_log("orphaned objects left by removals")


def test_names_that_only_look_reserved_are_kept():
    """A reserved name denotes an unfinished removal only while it has a marker object. Any name of that
    shape, the exact one included, could have been created as ordinary data by a version that reserved
    nothing and wrote no markers, so without a marker it is loaded as usual and never deleted.
    """
    node.query("DROP TABLE IF EXISTS t SYNC")
    wait_for_empty_prefix()

    node.query(
        "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_plain_rewritable'"
    )
    node.query("INSERT INTO t VALUES (1)")

    node.stop_clickhouse()

    look_alike_names = [
        # Exactly the generated shape, which an older server could have been asked to create as ordinary
        # data, for example `BACKUP TO Disk('s3_plain_rewritable', '__removed.abcdefghijklmnop')`.
        REMOVED_NAME_PREFIX + "abcdefghijklmnop",
        # A name that an older server could have been asked to create, for example for a backup.
        REMOVED_NAME_PREFIX + "mybackup",
        # One character short of the generated shape, and one character too long.
        REMOVED_NAME_PREFIX + "b" * 15,
        REMOVED_NAME_PREFIX + "b" * 17,
        # The right length, but not the alphabet of the generated shape.
        REMOVED_NAME_PREFIX + "B" * 16,
    ]

    # A top-level directory and a root file with each of these names, as an older server would have left them.
    keys_of_name = {}
    for index, name in enumerate(look_alike_names):
        remote_name = "zyxwvutsrqponml" + chr(ord("a") + index)
        keys = {
            f"{KEY_PREFIX}__root/{name}": b"a root file",
            f"{KEY_PREFIX}__meta/{remote_name}/prefix.path": f"{name}/".encode(),
            f"{KEY_PREFIX}{remote_name}/data.bin": b"a file of a directory",
        }
        for key, data in keys.items():
            put_key(key, data)
        keys_of_name[name] = list(keys)

    preexisting_keys = [key for keys in keys_of_name.values() for key in keys]

    node.start_clickhouse()
    assert int(node.query("SELECT count() FROM t")) == 1

    assert [key for key in preexisting_keys if not key_exists(key)] == []

    # The marker is what decides: the same name, marked, is reclaimed on the next writable start, while the
    # names that have no marker are kept.
    marked_name = look_alike_names[0]
    marked_keys = keys_of_name[marked_name]

    node.stop_clickhouse()
    put_key(TOMBSTONE_KEY_PREFIX + marked_name, marked_name.encode())
    node.start_clickhouse()

    wait_for_keys_to_disappear(marked_keys + [TOMBSTONE_KEY_PREFIX + marked_name])
    assert [
        key
        for key in preexisting_keys
        if key not in marked_keys and not key_exists(key)
    ] == []

    node.query("DROP TABLE t SYNC")
    for key in preexisting_keys:
        if key not in marked_keys:
            remove_key(key)
    wait_for_empty_prefix()
