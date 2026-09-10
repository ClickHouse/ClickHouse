import pytest
import logging

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, "node1", timeout=timeout)


def start_clickhouse():
    node1.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)


def test_keeper_log_gap_before_committed(started_cluster):
    try:
        node1.stop_clickhouse()
        node1.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/log"])
        node1.exec_in_container(
            ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
        )
        start_clickhouse()

        node1_conn = get_fake_zk()

        node1_conn.create("/test_log_gap")
        for i in range(20):
            node1_conn.create(f"/test_log_gap/node{i}", b"somedata")

        # Verify data was created
        children = node1_conn.get_children("/test_log_gap")
        assert len(children) == 20

        node1_conn.stop()
        node1_conn.close()

        # Stop node - this will create snapshot on exit
        node1.stop_clickhouse()

        # Check what files we have
        log_files = (
            node1.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            .strip()
            .split("\n")
        )
        snapshot_files = (
            node1.exec_in_container(
                ["ls", "/var/lib/clickhouse/coordination/snapshots"]
            )
            .strip()
            .split("\n")
        )

        logging.info(f"Log files: {log_files}")
        logging.info(f"Snapshot files: {snapshot_files}")

        # We should have multiple changelog files due to max_log_file_size=1
        assert len(log_files) > 1, f"Expected multiple log files but got: {log_files}"
        assert len(snapshot_files) >= 1, f"Expected at least one snapshot but got: {snapshot_files}"

        # Get the snapshot to find its index
        # Snapshot filename format: snapshot_<index>.bin
        snapshot_file = [f for f in snapshot_files if f.startswith("snapshot_")][0]
        snapshot_index = int(snapshot_file.split("_")[1].split(".")[0])
        logging.info(f"Snapshot index: {snapshot_index}")

        def get_from_to_index(changelog):
            parts = changelog.replace("changelog_", "").replace(".bin", "").split("_")
            return (int(parts[0]), int(parts[1]))

        # Find a changelog file that is before the snapshot index
        # Changelog filename format: changelog_<from>_<to>.bin
        changelog_to_delete = None
        for log_file in log_files:
            if log_file.startswith("changelog_"):
                from_index, to_index = get_from_to_index(log_file)

                # Delete a changelog that is completely before the snapshot
                # This simulates a gap in committed logs
                if to_index < snapshot_index and (snapshot_index - to_index) > 4 and from_index > 5:
                    changelog_to_delete = log_file
                    break

        assert (
            changelog_to_delete is not None
        ), f"Could not find a changelog before snapshot index {snapshot_index}"

        logging.info(f"Deleting changelog: {changelog_to_delete}")
        node1.exec_in_container(
            ["rm", f"/var/lib/clickhouse/coordination/log/{changelog_to_delete}"]
        )

        # Verify the file was deleted
        log_files_after_delete = (
            node1.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            .strip()
            .split("\n")
        )
        assert (
            changelog_to_delete not in log_files_after_delete
        ), f"Changelog {changelog_to_delete} should have been deleted"

        logging.info(f"Log files after deletion: {log_files_after_delete}")

        start_clickhouse()
        keeper_utils.wait_until_connected(cluster, node1)

        # Reconnect and verify data is still intact
        node1_conn = get_fake_zk()

        def verify_data():
            children = node1_conn.get_children("/test_log_gap")
            assert len(children) == 20, f"Expected 20 children but got {len(children)}"

            for child in children:
                data = node1_conn.get(f"/test_log_gap/{child}")[0]
                assert data == b"somedata", f"Data mismatch for {child}: {data}"

            # Verify we can still write new data
            created_node = node1_conn.create("/test_log_gap_after_recovery", b"newdata", sequence=True)
            logging.info(f"Created node: {created_node}")
            assert (
                node1_conn.get(created_node)[0] == b"newdata"
            ), "Failed to write data after recovery"

        verify_data()

        log_files = (
            node1.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            .strip()
            .split("\n")
        )

        logging.info(f"Log files: {log_files}")

        deleted_changelog_to_index = get_from_to_index(changelog_to_delete)[1]
        for log_file in log_files:
            if log_file.startswith("changelog_"):
                from_index, to_index = get_from_to_index(log_file)
                assert from_index > deleted_changelog_to_index, f"Changelog {log_file} was expected to be deleted"

        node1.restart_clickhouse()
        keeper_utils.wait_until_connected(cluster, node1)

        # Reconnect after restart since the old connection is now stale
        node1_conn.stop()
        node1_conn.close()
        node1_conn = get_fake_zk()

        verify_data()

    finally:
        try:
            if node1_conn:
                node1_conn.stop()
                node1_conn.close()
        except:
            pass
