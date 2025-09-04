import time
import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["config/os_thread_nice_value.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_os_thread_nice_value(started_cluster):
    for _ in range(42):
        send_thread_nice_value = node.exec_in_container([
            "bash",
            "-c",
            "ps -T -o comm,nice --no-headers | grep 'ZooKeeperSend' | awk '{print $2}' | head -n 1",
        ]).strip()
        receive_thread_nice_value = node.exec_in_container([
            "bash",
            "-c",
            "ps -T -o comm,nice --no-headers | grep 'ZooKeeperRecv' | awk '{print $2}' | head -n 1",
        ]).strip()

        if send_thread_nice_value == "-5" and receive_thread_nice_value == "-5":
            break

        time.sleep(0.2)
    else:
        pytest.fail(f"ZooKeeper threads don't have nice=-5. Send: {send_thread_nice_value}, Recv: {receive_thread_nice_value}")
