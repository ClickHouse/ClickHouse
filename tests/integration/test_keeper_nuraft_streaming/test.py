import pytest
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node1, node2, node3])
        yield cluster
    finally:
        cluster.shutdown()


def test_streaming_mode(started_cluster):
    conf = keeper_utils.send_4lw_cmd(cluster, node1, "conf")
    assert "nuraft_streaming_mode=true" in conf, f"Streaming mode not found in conf output:\n{conf}"
    assert "nuraft_max_log_gap_in_stream=16" in conf
    assert "nuraft_max_bytes_in_flight_in_stream=4194304" in conf

    log = node1.grep_in_log("streaming mode max log gap")
    assert log, "NuRaft startup message with streaming mode parameters not found in log"
    assert "16" in log, f"Expected max_log_gap=16 in log message: {log}"
