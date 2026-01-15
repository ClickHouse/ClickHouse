import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "test_disk_checker",
            main_configs=["config.xml"],
            with_minio=False,
            with_zookeeper=False,
            with_remote_database_disk=False,
            stay_alive=True,
        )
        cluster.start()

        node = cluster.instances["test_disk_checker"]
        node.exec_in_container(["bash", "-c", "mkdir -p /var/lib/clickhouse/path1"])
        node.exec_in_container(["bash", "-c", "mkdir -p /var/lib/clickhouse/path2"])

        yield cluster
    finally:
        cluster.shutdown()


def get_metric_value(node, metric_name):
    result = node.query(
        f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
    ).strip()
    return int(result) if result else 0


def test_disk_checker_started_log(started_cluster):
    node = cluster.instances["test_disk_checker"]

    # ensure that the disk checker log line exists in server logs
    def assert_log_exists(disk_name):
        expected_log = f"Disk check for disk {disk_name} started with period 1.00 s"
        count = node.count_in_log(expected_log)
        return int(count) > 0

    wait_condition(lambda: assert_log_exists('test1'), lambda x: x, max_attempts=10, delay=1)
    wait_condition(lambda: assert_log_exists('test2'), lambda x: x, max_attempts=10, delay=1)


def test_disk_readonly_status(started_cluster):
    try:
        node = cluster.instances["test_disk_checker"]
        disk_path = "/var/lib/clickhouse/path1"

        # a hack to make disk readonly
        node.exec_in_container(["mount", "--bind", disk_path, disk_path])
        # need to retry making the dir readonly because periodic task creates temporary files there to check for write access
        mount_read_only_succeded = False
        for retry in range(10):
            try:
                node.exec_in_container(["mount", "-o", "remount,ro,bind", disk_path])
                mount_read_only_succeded = True
                break;
            except Exception as err:
                time.sleep(0.42);
        assert mount_read_only_succeded;

        # assert for metric with retries
        wait_condition(
            func=lambda: get_metric_value(node, "ReadonlyDisks"),
            condition=lambda value: value == 1,
            max_attempts=10,
            delay=1,
        )

        # restore the disk to writable state
        node.exec_in_container(["mount", "-o", "remount,rw,bind", disk_path])

        # again assert for metric with retries
        wait_condition(
            func=lambda: get_metric_value(node, "ReadonlyDisks"),
            condition=lambda value: value == 0,
            max_attempts=10,
            delay=1,
        )
    finally:
        try:
            node.exec_in_container(["umount", disk_path])
        except:
            pass


def test_disk_broken_status(started_cluster):
    try:
        node = cluster.instances["test_disk_checker"]
        disk_path = "/var/lib/clickhouse/path2"

        # move the directory to simulate a borken disk
        node.exec_in_container(["mv", disk_path, f"{disk_path}_broken"])

        # assert for metric with retries
        wait_condition(
            func=lambda: get_metric_value(node, "BrokenDisks"),
            condition=lambda value: value == 1,
            max_attempts=10,
            delay=1,
        )

        # restore the previously moved directory
        node.exec_in_container(["mv", f"{disk_path}_broken", disk_path])
        # it looks like clickhouse needs to be restarted to recover from broken disk
        node.restart_clickhouse()

        # again assert for metric with retries
        wait_condition(
            func=lambda: get_metric_value(node, "BrokenDisks"),
            condition=lambda value: value == 0,
            max_attempts=10,
            delay=1,
        )
    finally:
        try:
            node.exec_in_container(["mv", f"{disk_path}_broken", disk_path])
        except:
            pass
