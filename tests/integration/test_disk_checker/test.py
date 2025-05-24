import pytest

from helpers.cluster import ClickHouseCluster

import os
import time
import logging

@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "test_disk_checker",
            main_configs=["config.xml"],
            with_minio=False,
            with_zookeeper=True,
            with_remote_database_disk=False,  # The tests work on the local disk and check local files
        )
        cluster.start()

        # local disk requires its `path` directory to exist.
        # the two paths below belong to `test1` and `test2` disks
        node = cluster.instances["test_disk_checker"]
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p /var/lib/clickhouse/path1",
            ]
        )

        yield cluster

    finally:
        cluster.shutdown()

def test_disk_checker_started_log(started_cluster):
    node = cluster.instances["test_disk_checker"]

    # This is the log string we expect (case-sensitive grep)

    expected_log = "Disk check for disk test1 started with period 10.00s"

    count = node.count_in_log(expected_log)

    # Make sure it exists in the logs
    logging.info(f"DiskChecker log count: {count}")
    assert int(count) > 0, "DiskChecker did not start or log not found"

def wait_for_file(node, filepath, timeout=30):
    for _ in range(timeout):
        res = node.exec_in_container(["bash", "-c", f"test -f {filepath} && echo exists || echo missing"]).strip()
        if res == "exists":
            return True
        time.sleep(1)
    return False

def wait_for_log(node, expected_log, timeout=30):
    for _ in range(timeout):
        count = int(node.count_in_log(expected_log))
        if count > 0:
            return True
        time.sleep(1)
    return False

def wait_for_metrics(node, metric_name, expected_value, timeout=30):
    for _ in range(timeout):
        try:
            result = node.query(f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'").strip()
            value = int(result)
            logging.info(f"wait_for_metrics for metric_name {metric_name}, "
                         f"current value {value}, expected {expected_value}")
            if value == expected_value:
                return True
        except Exception as e:
            logging.debug(f"wait_for_metrics: Exception querying metric {metric_name}: {e}")
        time.sleep(1)
    return False

def test_disk_readonly_prometheus_status(started_cluster):
    node = cluster.instances["test_disk_checker"]
    disk_name = "test1"
    # Based on your config, disk name is 'test1' and path inside container is:
    disk_path = "/var/lib/clickhouse/path1"
    disk_checker_path = os.path.join(disk_path, ".disk_checker_file")
    disk_checker_backup_path = disk_checker_path + ".bak"

    # Ensure .disk_checker_file exists initially if we have set local_disk_check_period_ms
    assert wait_for_file(node, disk_checker_path), ".disk_checker_file was not created in time"

    # Step 2: Move away .disk_checker_file to simulate readonly disk
    node.exec_in_container(["mv", disk_checker_path, disk_checker_backup_path])
    logging.info("Moved .disk_checker_file away to simulate disk readonly")

    # Wait some seconds to let ClickHouse detect the change

    # We should find the readonly log
    expected_log = f"Disk {disk_name} is readonly"
    assert wait_for_log(node, expected_log), "DiskChecker did not find expected log"

    count = node.count_in_log(expected_log)
    logging.info(f"DiskChecker found the disk {disk_name} readonly log count: {count}")
    assert int(count) > 0, f"DiskChecker did not found the log indicating disk {disk_name} is readonly"

    # Check Prometheus metrics for readonly disk
    assert wait_for_metrics(node, "ReadonlyDisks", 1), "ReadonlyDisks metric did not reach 1"

    # Step 3: Move the .disk_checker_file back to restore disk readable status
    node.exec_in_container(["mv", disk_checker_backup_path, disk_checker_path])
    logging.info("Restored .disk_checker_file to simulate disk back to normal")

    # Check Prometheus metrics again, disk should no longer be readonly
    assert wait_for_metrics(node, "ReadonlyDisks", 0), "ReadonlyDisks metric did not reach 0"

def test_disk_broken_prometheus_status(started_cluster):
    node = cluster.instances["test_disk_checker"]

    disk_path = "/var/lib/clickhouse/path1"
    disk_name = "test1"

    output = node.exec_in_container(["ls", "-ld", disk_path]).strip()
    logging.info(f"Initial permissions for {disk_path}: {output}")


    # Step 1: Remove write permission to simulate broken disk
    node.exec_in_container(["chmod", "555", disk_path])
    logging.info(f"Changed permissions to 555 on {disk_path} to simulate broken disk")

    output = node.exec_in_container(["ls", "-ld", disk_path]).strip()
    logging.info(f"After changed to Readonly,  permissions for {disk_path}: {output}")

    expected_log = f"Disk {disk_name} marked as broken"
    assert wait_for_log(node, expected_log), "DiskChecker did not find expected log for broken disk"
    assert wait_for_metrics(node, "BrokenDisks", 1), "BrokenDisks metric did not reach 1"

    # Step 2: Restore permissions
    node.exec_in_container(["chmod", "775", disk_path])
    logging.info(f"Restored permissions to 775 on {disk_path}")

    output = node.exec_in_container(["ls", "-ld", disk_path]).strip()
    logging.info(f"After changed to 775 again,  permissions for {disk_path}: {output}")

    assert wait_for_metrics(node, "BrokenDisks", 0), "BrokenDisks metric did not reach 0"


