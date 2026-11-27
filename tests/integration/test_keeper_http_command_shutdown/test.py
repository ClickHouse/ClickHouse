import threading
import time
import urllib.request

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/keeper.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node])
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_log_contains(substring, timeout=30):
    deadline = time.monotonic() + timeout
    while not node.contains_in_log(substring):
        assert time.monotonic() < deadline, f"Log line did not appear: {substring}"
        time.sleep(0.1)


def test_http_four_letter_command_finishes_before_keeper_shutdown(started_cluster):
    keeper_utils.wait_nodes(cluster, [node])

    # The command sleeps 20 s, so it outlives the 1 s `shutdown_wait_unfinished`.
    node.query("SYSTEM ENABLE FAILPOINT keeper_http_four_letter_command_delay")

    result = {}

    def run_command():
        try:
            with urllib.request.urlopen(
                f"http://{node.ip_address}:9182/api/v1/commands?command=rqld",
                timeout=60,
            ) as response:
                result["status"] = response.status
                result["body"] = response.read().decode()
        except Exception as e:
            result["error"] = repr(e)

    thread = threading.Thread(target=run_command)
    thread.start()
    try:
        wait_for_log_contains("Received four letter command rqld")

        clickhouse_pid = node.get_process_pid("clickhouse server")
        assert clickhouse_pid is not None
        node.exec_in_container(
            ["bash", "-c", f"kill -TERM {clickhouse_pid}"], user="root"
        )

        # The request is still running when the bounded wait for HTTP connections starts.
        wait_for_log_contains(
            "Closed all non-Keeper-TCP listening sockets. Waiting for 1 outstanding connections."
        )

        thread.join(60)
        assert not thread.is_alive()
        assert result.get("status") == 200, result
        assert "Sent leadership request to leader." in result["body"]

        node.wait_start_failed(60)
        assert not node.contains_in_log("########################################")
    finally:
        thread.join(60)
        if node.get_process_pid("clickhouse server") is not None:
            node.stop_clickhouse(kill=True)
        node.start_clickhouse()
