import time
import threading
import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["config/config.d/os_thread_nice_value.xml"],
    user_configs=["config/users.d/users.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_zookeeper_client_send_receive(started_cluster):
    for _ in range(42):
        send_thread_nice_ok = bool(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ps -eT -o comm,nice --no-headers | grep 'ZooKeeperSend.*-5$'",
                ],
                nothrow=True,
            ).strip()
        )

        receive_thread_nice_ok = bool(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ps -eT -o comm,nice --no-headers | grep 'ZooKeeperRecv.*-5$'",
                ],
                nothrow=True,
            ).strip()
        )

        if send_thread_nice_ok and receive_thread_nice_ok:
            return

        time.sleep(0.1)
    else:
        pytest.fail("zookeeper threads don't have nice=-5")


def test_query(started_cluster):
    query_thread = threading.Thread(
        target=lambda: node.query("SELECT sleep(3)")
    )
    query_thread.start()

    while query_thread.is_alive():
        query_pull_pipeline_executor_nice_ok = bool(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ps -eT -o comm,nice --no-headers | grep 'QueryPullPipeEx.*4$'",
                ],
                nothrow=True,
            ).strip()
        )

        tcp_handler_nice_ok = bool(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ps -eT -o comm,nice --no-headers | grep 'TCPHandler.*4$'",
                ],
                nothrow=True,
            ).strip()
        )

        if query_pull_pipeline_executor_nice_ok and tcp_handler_nice_ok:
            query_thread.join()
            return

        time.sleep(0.1)

    pytest.fail("query threads don't have nice=4")
