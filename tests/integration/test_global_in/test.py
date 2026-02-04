import pytest
import time
import uuid

from threading import Thread

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
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


@pytest.mark.parametrize("enable_analyzer", [0, 1])
def test_success(started_cluster, enable_analyzer):
    assert (
        int(
            node1.query(
                query, settings={"enable_analyzer": enable_analyzer}, timeout=30
            ).strip()
        )
        == 14
    )


@pytest.mark.parametrize("enable_analyzer", [0, 1])
def test_max_execution_time_timeout(started_cluster, enable_analyzer):
    query_id = str(uuid.uuid4())
    node2.query("SYSTEM ENABLE FAILPOINT sleep_on_receive_external_table_data")
    try:
        with pytest.raises(QueryRuntimeException) as e:
            node1.query(
                query,
                settings={"max_execution_time": 15, "enable_analyzer": enable_analyzer},
                timeout=30,
                query_id=query_id,
            )
        assert "TIMEOUT_EXCEEDED" in str(e.value)
        node2.query("SYSTEM FLUSH LOGS")
        remote_exception = node2.query(
            f"SELECT exception FROM system.query_log WHERE initial_query_id = '{query_id}' AND type = 'ExceptionBeforeStart'"
        )
        assert (
            "Code: 159. DB::Exception: Timeout exceeded while reading external table data."
            in remote_exception
        )
        assert "timeout is 15 seconds. (TIMEOUT_EXCEEDED)" in remote_exception
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT sleep_on_receive_external_table_data")


@pytest.mark.parametrize("enable_analyzer", [0, 1])
def test_max_execution_time_socket_timeout(started_cluster, enable_analyzer):
    query_id = str(uuid.uuid4())

    def run_query():
        try:
            node1.query(
                query,
                settings={"max_execution_time": 9, "enable_analyzer": enable_analyzer},
                timeout=30,
                query_id=query_id,
            )
        except QueryRuntimeException as e:
            assert "TIMEOUT_EXCEEDED" in str(e)

    query_thread = Thread(target=run_query)
    with PartitionManager() as pm:
        # make sending external table data slow by adding a delay to all outgoing packets on node1
        # this gives us a chance to pause the container while it's sending external table data
        pm.add_network_delay(node1, 700)
        query_thread.start()
        try:
            # wait for the remote query to arrive on node2
            node2.wait_for_log_line(f"initial_query_id: {query_id}")
            with cluster.pause_container("node1"):
                time.sleep(15)
                node2.query("SYSTEM FLUSH LOGS")
                remote_exception = node2.query(
                    f"SELECT exception FROM system.query_log WHERE initial_query_id = '{query_id}' AND type = 'ExceptionBeforeStart'"
                )
                assert "SOCKET_TIMEOUT" in remote_exception
                # max_execution_time should be used as the receive timeout
                assert "9000 ms" in remote_exception
        finally:
            query_thread.join()
