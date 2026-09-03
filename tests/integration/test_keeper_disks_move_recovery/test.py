#!/usr/bin/env python3

import io
import os

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_local.xml"],
    stay_alive=True,
    with_minio=True,
    with_remote_database_disk=False,
)
gap_serial = cluster.add_instance(
    "gap_serial",
    main_configs=["configs/keeper_gap_serial.xml"],
    stay_alive=True,
)
gap_parallel = cluster.add_instance(
    "gap_parallel",
    main_configs=["configs/keeper_gap_parallel.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        mock = start_s3_mock(cluster, "broken_s3", "8087")
        yield mock
    finally:
        cluster.shutdown()


def list_objects(prefix):
    return [
        item.object_name
        for item in cluster.minio_client.list_objects(
            cluster.minio_bucket, prefix=prefix, recursive=True
        )
    ]


def install_s3_keeper_config():
    node.stop_clickhouse()
    local_logs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/coordination/logs -maxdepth 1 -type f -name 'changelog_*' -printf '%f\\n'"]
    ).splitlines()
    assert len(local_logs) > 1
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/keeper_s3.xml"),
        "/etc/clickhouse-server/config.d/keeper_local.xml",
    )


def assert_keeper_data():
    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        assert zk.get("/move_recovery")[0] == b"survives"
    finally:
        zk.stop()
        zk.close()


def restart_and_verify_marker_recovery(prefix, marker):
    node.stop_clickhouse()
    node.start_clickhouse(start_wait_sec=60)
    assert marker not in list_objects(prefix)
    assert_keeper_data()


def test_delayed_changelog_and_snapshot_marker_puts(started_cluster):
    mock = started_cluster
    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        zk.create("/move_recovery", b"survives")
        for index in range(20):
            zk.create(f"/move_recovery/log_{index}", b"value")
    finally:
        zk.stop()
        zk.close()

    install_s3_keeper_config()
    mock.setup_delayed_marker_put("tmp_changelog_")
    node.start_clickhouse(wait_start=False)
    mock.wait_delayed_marker_put()
    node.wait_start(60)

    local_logs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/coordination/logs -maxdepth 1 -type f -printf '%f\\n'"]
    ).splitlines()
    assert len([name for name in local_logs if name.startswith("changelog_")]) == 1
    assert not [name for name in list_objects("logs/") if os.path.basename(name).startswith("tmp_")]

    mock.release_delayed_marker_put()
    mock.wait_delayed_marker_put_completed()
    changelog_markers = [
        name for name in list_objects("logs/") if os.path.basename(name).startswith("tmp_changelog_")
    ]
    assert len(changelog_markers) == 1
    restart_and_verify_marker_recovery("logs/", changelog_markers[0])

    # Recreate the legacy state without a delayed request. The empty marker must be
    # removed only after the neighbouring destination validates successfully.
    changelog_files = [name for name in list_objects("logs/") if "changelog_" in os.path.basename(name)]
    assert changelog_files
    legacy_marker = os.path.join(os.path.dirname(changelog_files[0]), "tmp_" + os.path.basename(changelog_files[0]))
    cluster.minio_client.put_object(cluster.minio_bucket, legacy_marker, io.BytesIO(b""), 0)
    restart_and_verify_marker_recovery("logs/", legacy_marker)

    mock.reset()
    mock.setup_delayed_marker_put("tmp_snapshot_")
    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        first_idx = keeper_utils.send_4lw_cmd(cluster, node, "csnp")
        node.wait_for_log_line(f"Created persistent snapshot {first_idx}")
        zk.create("/move_recovery/after_snapshot", b"value")
        second_idx = keeper_utils.send_4lw_cmd(cluster, node, "csnp")
        node.wait_for_log_line(f"Created persistent snapshot {second_idx}")
    finally:
        zk.stop()
        zk.close()

    mock.wait_delayed_marker_put()
    local_snapshots = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/coordination/snapshots -maxdepth 1 -type f -printf '%f\\n'"]
    ).splitlines()
    assert len([name for name in local_snapshots if name.startswith("snapshot_")]) == 1
    assert not [
        name
        for name in list_objects("snapshots/")
        if os.path.basename(name).startswith("tmp_snapshot_")
    ]
    assert [
        name
        for name in list_objects("snapshots/")
        if os.path.basename(name).startswith("snapshot_")
    ]
    mock.release_delayed_marker_put()
    mock.wait_delayed_marker_put_completed()
    snapshot_markers = [
        name for name in list_objects("snapshots/") if os.path.basename(name).startswith("tmp_snapshot_")
    ]
    assert len(snapshot_markers) == 1
    restart_and_verify_marker_recovery("snapshots/", snapshot_markers[0])


@pytest.mark.parametrize(
    "instance_name,expected_streams",
    [("gap_serial", 1), ("gap_parallel", 4)],
)
def test_snapshot_covered_gap_accepts_next_index_boundary(
    started_cluster, instance_name, expected_streams
):
    instance = cluster.instances[instance_name]
    zk = keeper_utils.get_fake_zk(cluster, instance_name)
    try:
        zk.create("/covered_gap", str(expected_streams).encode())
        for index in range(8):
            zk.create(f"/covered_gap/before_{index}", b"before")

        snapshot_index = int(keeper_utils.send_4lw_cmd(cluster, instance, "csnp"))
        instance.wait_for_log_line(f"Created persistent snapshot {snapshot_index}")

        for index in range(4):
            zk.create(f"/covered_gap/after_{index}", b"after")
    finally:
        zk.stop()
        zk.close()

    instance.stop_clickhouse()
    log_directory = "/var/lib/clickhouse/coordination/logs"
    log_files = instance.exec_in_container(["ls", log_directory]).splitlines()

    def log_range(name):
        stem = name.removeprefix("changelog_").removesuffix(".bin")
        start, end = stem.split("_")
        return int(start), int(end)

    ranges = {
        name: log_range(name)
        for name in log_files
        if name.startswith("changelog_") and name.endswith(".bin")
    }
    next_file = next(
        name for name, (start, _) in ranges.items() if start == snapshot_index + 1
    )
    kept_before = max(
        (
            (start, name)
            for name, (start, _) in ranges.items()
            if start < snapshot_index
        ),
        default=None,
    )
    assert kept_before is not None

    for name, (start, _) in ranges.items():
        if kept_before[0] < start < snapshot_index + 1:
            instance.exec_in_container(["rm", f"{log_directory}/{name}"])

    assert next_file in instance.exec_in_container(["ls", log_directory]).splitlines()
    instance.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, instance)

    zk = keeper_utils.get_fake_zk(cluster, instance_name)
    try:
        assert zk.get("/covered_gap")[0] == str(expected_streams).encode()
        assert len(zk.get_children("/covered_gap")) == 12
        for index in range(4):
            assert zk.get(f"/covered_gap/after_{index}")[0] == b"after"
    finally:
        zk.stop()
        zk.close()
