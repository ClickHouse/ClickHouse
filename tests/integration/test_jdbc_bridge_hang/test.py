import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)

# Must match <jdbc_bridge><port> in configs/jdbc_bridge.xml.
MOCK_BRIDGE_PORT = 9020

# http_receive_timeout used for the test query. The metadata request blocks for one such
# timeout per attempt. With the fix it makes a single attempt (~RECEIVE_TIMEOUT seconds);
# before the fix it retried http_max_tries (= 10) times, taking well over ten times longer.
RECEIVE_TIMEOUT = 3

node = cluster.add_instance(
    "node",
    main_configs=["configs/jdbc_bridge.xml"],
)


def start_unresponsive_bridge():
    script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mocks")
    # Run the mock inside the server's own container, reachable at 127.0.0.1:MOCK_BRIDGE_PORT.
    start_mock_servers(
        cluster,
        script_dir,
        [("unresponsive_bridge.py", "node", MOCK_BRIDGE_PORT)],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        start_unresponsive_bridge()
        yield cluster
    finally:
        cluster.shutdown()


def test_jdbc_table_function_does_not_hang_on_unresponsive_bridge(started_cluster):
    # The mock bridge answers the ping handshake but never responds to /columns_info, so
    # structure inference for the `jdbc` table function cannot complete. The query must fail
    # promptly (after a single receive timeout) instead of retrying the metadata request and
    # keeping the query alive for minutes — which is what tripped the stress-test hung check.
    start = time.time()
    error = node.query_and_get_error(
        "SELECT * FROM jdbc('dummy_datasource', 'some_table')",
        settings={"http_receive_timeout": RECEIVE_TIMEOUT},
    )
    elapsed = time.time() - start

    # The unresponsive bridge must make the query fail, not return data.
    assert error != ""
    # One attempt is ~RECEIVE_TIMEOUT seconds; ten retries would be well over 30 seconds.
    assert (
        elapsed < 15
    ), f"jdbc structure inference took {elapsed:.1f}s; the bridge metadata request is being retried instead of failing fast"
