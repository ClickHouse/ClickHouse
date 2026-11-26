import pytest
import uuid

from threading import Thread

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/users.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/users.xml"],
)

query = """
SELECT count()
FROM clusterAllReplicas('test_cluster', numbers(10))
WHERE number GLOBAL IN (
    SELECT number % 5 FROM numbers_mt(1000000000)
) OR number GLOBAL IN (
    SELECT number % 7 FROM numbers_mt(1000000000)
)
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_max_execution_time_socket_timeout(started_cluster):
    query_id = str(uuid.uuid4())

    def run_query():
        try:
            node1.query(
                query,
                settings={
                    "max_execution_time": 25,
                    "connect_timeout_with_failover_ms": 5000,
                },
                timeout=120,
                query_id=query_id,
            )
        except QueryRuntimeException as e:
            assert "TIMEOUT_EXCEEDED" in str(e)

    query_thread = Thread(target=run_query)
    with PartitionManager() as pm:
        # make sending external table data slow by adding a delay to all outgoing packets on node1
        # this gives us a chance to pause the container while it's sending external table data
        pm.add_network_delay(node1, 1000)
        query_thread.start()
        try:
            # wait for the remote query to arrive on node2
            node2.wait_for_log_line(f"initial_query_id: {query_id}", timeout=60)
            with cluster.pause_container("node1"):
                assert_eq_with_retry(
                    node2,
                    f"SELECT count() FROM system.query_log WHERE initial_query_id = '{query_id}' AND type = 'ExceptionBeforeStart'",
                    "1",
                    retry_count=60,
                    sleep_time=1,
                )
                remote_exception = node2.query(
                    f"SELECT exception FROM system.query_log WHERE initial_query_id = '{query_id}' AND type = 'ExceptionBeforeStart'"
                )
                assert "SOCKET_TIMEOUT" in remote_exception
                # max_execution_time should be used as the receive timeout.
                # The message depends on whether node1 was paused in the middle of a packet or between two packets.
                assert (
                    "25000 ms" in remote_exception
                    or "timeout is 25 seconds" in remote_exception
                )
        finally:
            query_thread.join()
