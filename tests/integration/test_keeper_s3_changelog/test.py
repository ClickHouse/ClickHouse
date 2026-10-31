#!/usr/bin/env python3
import io
import os
import time

import pytest
from minio.deleteobjects import DeleteObject

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)
node_logs = cluster.add_instance(
    "node_logs",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    with_minio=True,
    # with_remote_database_disk=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def stop_clickhouse(cluster, node, cleanup_disks):
    node.stop_clickhouse()

    if not cleanup_disks:
        return

    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/logs"])
    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"])

    s3_objects = list_s3_objects(cluster, prefix="")
    if len(s3_objects) == 0:
        return

    assert (
        len(
            list(
                cluster.minio_client.remove_objects(
                    cluster.minio_bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )


def setup_storage(cluster, node, storage_config, cleanup_disks):
    stop_clickhouse(cluster, node, cleanup_disks)
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/enable_keeper.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
        "<!-- DISK DEFINITION PLACEHOLDER -->",
        storage_config,
    )
    node.start_clickhouse()
    # complete readiness checks that the sessions can be established,
    # but it creates sesssion for this, which will create one more record in log,
    # but this test is very strict on number of entries in the log,
    # so let's avoid this extra check and rely on retry policy
    keeper_utils.wait_until_connected(cluster, node, wait_complete_readiness=False)


def setup_local_storage(cluster, node):
    setup_storage(
        cluster,
        node,
        "<log_storage_disk>log_local<\\/log_storage_disk>"
        "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
        cleanup_disks=True,
    )


def list_s3_objects(cluster, prefix=""):
    minio = cluster.minio_client
    prefix_len = len(prefix)
    return [
        obj.object_name[prefix_len:]
        for obj in minio.list_objects(
            cluster.minio_bucket, prefix=prefix, recursive=True
        )
    ]


def get_local_files(path, node):
    files = node.exec_in_container(["ls", path]).strip().split("\n")
    files.sort()
    return files


def get_local_logs(node):
    return get_local_files("/var/lib/clickhouse/coordination/logs", node)


def get_local_snapshots(node):
    return get_local_files("/var/lib/clickhouse/coordination/snapshots", node)


def test_correctness(started_cluster):
    setup_local_storage(started_cluster, node_logs)

    node_zk = get_fake_zk("node_logs")
    try:
        node_zk.create("/test")
        # Wait for every create to be acknowledged rather than for a specific
        # `commit upto N` log line: the commit index also depends on how many
        # sessions were created, and commits are batched, so a fixed index may
        # never appear in the log.
        requests = [
            node_zk.create_async("/test/somenode", b"somedata", sequence=True)
            for _ in range(30)
        ]
        for request in requests:
            request.get(timeout=30)

        children = node_zk.get_children("/test")
        assert len(children) == 30
        for child in children:
            assert node_zk.get(f"/test/{child}")[0] == b"somedata"

        # The static config sets `s3_experimental_changelog=true` and
        # `s3_log_disk=s3_changelog`, so changelog segments must land in MinIO
        # under `snaplogs/` rather than on the local log disk.
        # The segment that is still being written is uploaded under the
        # `s3_in_progress_` prefix and published under its final name on flush.
        s3_changelog_files = [
            name
            for name in list_s3_objects(started_cluster, "snaplogs/")
            if not name.startswith("s3_in_progress_")
        ]
        assert (
            len(s3_changelog_files) > 0
        ), f"expected S3 changelog files under snaplogs/, got: {s3_changelog_files}"
        assert all(
            name.startswith("changelog_") for name in s3_changelog_files
        ), f"unexpected S3 changelog file names: {s3_changelog_files}"
    finally:
        stop_zk(node_zk)


def test_restart_reads_changelog_from_s3(started_cluster):
    # Start from a clean state (wipes local coordination data and the MinIO bucket).
    setup_local_storage(started_cluster, node_logs)

    node_zk = get_fake_zk("node_logs")
    try:
        node_zk.create("/test_restart")
        requests = [
            node_zk.create_async("/test_restart/somenode", b"somedata", sequence=True)
            for _ in range(30)
        ]
        for request in requests:
            request.get(timeout=30)

        children_before = sorted(node_zk.get_children("/test_restart"))
        assert len(children_before) == 30

        # The changelog is stored only on the S3 disk, so these objects are the sole
        # durable copy of the log.
        s3_changelog_files = list_s3_objects(started_cluster, "snaplogs/")
        assert len(s3_changelog_files) > 0
    finally:
        stop_zk(node_zk)

    # Restart Keeper after wiping the local log directory. This drops the in-memory
    # entry caches and removes any local changelog residue, so on startup Keeper must
    # rebuild its state by reading the changelog back from S3. This is the actual
    # storage contract: restart/replay, not just live reads.
    #
    # The local snapshot directory is deliberately kept: only the changelog is moved to
    # S3 by this feature, snapshots stay on `snapshot_storage_disk`. With
    # `snapshot_distance=10`, `stale_log_gap=10` and `reserved_log_items=1` the log is
    # compacted once a snapshot is taken, so the changelog alone is not expected to
    # carry the whole history.
    node_logs.stop_clickhouse()
    node_logs.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/logs"])
    node_logs.start_clickhouse()
    keeper_utils.wait_until_connected(
        started_cluster, node_logs, wait_complete_readiness=False
    )

    node_zk = get_fake_zk("node_logs")
    try:
        children_after = sorted(node_zk.get_children("/test_restart"))
        assert children_after == children_before, (
            "data lost after restart from S3 changelog: "
            f"before={children_before} after={children_after}"
        )
        for child in children_after:
            assert node_zk.get(f"/test_restart/{child}")[0] == b"somedata"
    finally:
        stop_zk(node_zk)


def get_s3_changelog_ranges(cluster):
    ranges = []
    for name in list_s3_objects(cluster, "snaplogs/"):
        if not name.startswith("changelog_"):
            continue
        from_index, to_index = name[len("changelog_") : -len(".bin")].split("_")
        ranges.append((int(from_index), int(to_index)))
    return sorted(ranges)


def split_changelog_records(data):
    # A record is `checksum` (8 bytes), `version` (1), `index` (8), `term` (8),
    # `value_type` (4) and `blob_size` (8), followed by the blob.
    records = []
    position = 0
    while position < len(data):
        index = int.from_bytes(data[position + 9 : position + 17], "little")
        blob_size = int.from_bytes(data[position + 29 : position + 37], "little")
        end = position + 37 + blob_size
        records.append((index, data[position:end]))
        position = end
    return records


def test_compaction_after_interrupted_merge(started_cluster):
    # Merges must not be capped by `rotate_log_storage_interval`, and no snapshot
    # may remove the merged object through retention.
    stop_clickhouse(started_cluster, node_logs, cleanup_disks=True)
    node_logs.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/enable_keeper.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )
    for setting, value in [
        ("snapshot_distance", "10"),
        ("stale_log_gap", "10"),
        ("rotate_log_storage_interval", "3"),
    ]:
        node_logs.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper.xml",
            f"<{setting}>{value}<",
            f"<{setting}>100000<",
        )
    node_logs.replace_in_config(
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
        "<!-- DISK DEFINITION PLACEHOLDER -->",
        "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
    )
    node_logs.start_clickhouse()
    keeper_utils.wait_until_connected(
        started_cluster, node_logs, wait_complete_readiness=False
    )

    def wait_for_merge_from(index):
        for _ in range(300):
            ranges = get_s3_changelog_ranges(started_cluster)
            if any(
                from_index == index and to_index > index
                for from_index, to_index in ranges
            ):
                return ranges
            time.sleep(0.1)
        assert False, f"no merged S3 changelog starting at index {index}: {ranges}"

    def create_nodes(count):
        node_zk = get_fake_zk("node_logs")
        try:
            node_zk.ensure_path("/test_interrupted_merge")
            # Sequential creates, so that every entry is flushed as a separate object.
            for _ in range(count):
                node_zk.create(
                    "/test_interrupted_merge/node", b"somedata", sequence=True
                )
            return len(node_zk.get_children("/test_interrupted_merge"))
        finally:
            stop_zk(node_zk)

    assert create_nodes(40) == 40
    wait_for_merge_from(1)
    node_logs.stop_clickhouse()

    # Recreate the state after a crash between publishing a merge and removing its
    # sources: startup removes the source with the same start index as the merged
    # object, but keeps the following ones.
    merged_to = max(
        to_index
        for from_index, to_index in get_s3_changelog_ranges(started_cluster)
        if from_index == 1
    )
    minio = started_cluster.minio_client
    merged_name = f"snaplogs/changelog_1_{merged_to}.bin"
    response = minio.get_object(started_cluster.minio_bucket, merged_name)
    try:
        merged_data = response.read()
    finally:
        response.close()
        response.release_conn()

    records = split_changelog_records(merged_data)
    assert [index for index, _ in records] == list(range(1, merged_to + 1))
    for index, record in records[1:]:
        minio.put_object(
            started_cluster.minio_bucket,
            f"snaplogs/changelog_{index}_{index}.bin",
            io.BytesIO(record),
            len(record),
        )

    node_logs.start_clickhouse()
    keeper_utils.wait_until_connected(
        started_cluster, node_logs, wait_complete_readiness=False
    )

    # The objects published after the restart must still be merged instead of
    # stalling the compaction on the leftover sources. The recovered object is not
    # extended: the leftover sources would replay over it on the next startup and
    # truncate it to their end.
    assert create_nodes(40) == 80
    wait_for_merge_from(merged_to + 1)
    assert (1, merged_to) in get_s3_changelog_ranges(started_cluster)

    # All acknowledged entries must survive another restart with the leftover
    # sources still listed.
    node_logs.restart_clickhouse()
    keeper_utils.wait_until_connected(
        started_cluster, node_logs, wait_complete_readiness=False
    )
    assert create_nodes(0) == 80
