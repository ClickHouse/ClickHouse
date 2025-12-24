import pytest

import helpers.client
import helpers.cluster
from helpers.test_tools import assert_eq_with_retry

cluster = helpers.cluster.ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/cleanup_thread.xml",
        "configs/storage.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    privileged_docker=True,
    add_loop_control_device=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.exec_in_container(["bash", "-c", "fallocate -l200M /tmp/ext4.img"])
        node1.exec_in_container(
            ["bash", "-c", "mke2fs -t ext4 -O casefold /tmp/ext4.img"]
        )
        node1.exec_in_container(["bash", "-c", "mkdir -p /mnt/ext4"])
        node1.exec_in_container(["bash", "-c", "mount /tmp/ext4.img -o loop /mnt/ext4"])
        node1.exec_in_container(["bash", "-c", "mkdir -p /mnt/ext4/case-insensitive"])
        node1.exec_in_container(["bash", "-c", "chattr +F /mnt/ext4/case-insensitive/"])

        # Verify casefold works
        node1.exec_in_container(["bash", "-c", "touch /mnt/ext4/case-insensitive/foo"])
        result = node1.exec_in_container(
            ["bash", "-c", "ls /mnt/ext4/case-insensitive/FOO"]
        )
        assert "foo" in result or "FOO" in result, "Casefold filesystem not working"

        # Create clickhouse data directory with proper permissions
        node1.exec_in_container(
            ["bash", "-c", "mkdir -p /mnt/ext4/case-insensitive/clickhouse"]
        )
        # node1.exec_in_container(["bash", "-c", "chown -R clickhouse:clickhouse /mnt/ext4/case-insensitive/clickhouse"])

        # Restart clickhouse to pick up the new disk
        node1.restart_clickhouse()
        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    node1.query(
        "CREATE TABLE test_case_sensitivity (x UInt64) "
        "ENGINE = MergeTree ORDER BY tuple()"
        "SETTINGS storage_policy='casefold_policy', min_bytes_for_wide_part=1"
    )

    node1.query("INSERT INTO test_case_sensitivity SELECT 42")
    filenames = node1.query(
        "SELECT filenames FROM system.parts_columns WHERE table='test_case_sensitivity'"
    )
    assert "x" not in filenames
    assert "ad991276876cc297cd7c8197dd3b2592" in filenames
