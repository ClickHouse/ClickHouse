import http.client
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keep_alive.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown_with_idle_keep_alive_connection(started_cluster):
    """
    An idle keep-alive connection must not block server shutdown for the whole
    keep_alive_timeout.

    The interserver HTTP server belongs to the group stopped before tables; that
    shutdown phase waits on `server_pool.joinAll`, which blocks until the handler
    thread exits. A handler parked in `HTTPServerSession::hasMoreRequests` (a socket
    read bounded by keep_alive_timeout) does not observe `server.stop` until the
    read returns, so shutdown used to stall for the whole keep_alive_timeout
    (set to 50s here). Shutdown must complete quickly regardless.
    """
    # Open a connection to the interserver HTTP port, send one request and read the
    # full response, then leave the connection idle in keep-alive state. The
    # server-side handler thread now parks waiting for the next request.
    conn = http.client.HTTPConnection(node.ip_address, 9009, timeout=30)
    try:
        conn.request("GET", "/")
        response = conn.getresponse()
        response.read()
        # The connection must actually be kept alive for the test to be meaningful.
        assert response.getheader("Connection", "").lower() != "close"

        node.query("SYSTEM SHUTDOWN")

        timeout = 20
        start_time = time.time()
        while time.time() - start_time < timeout:
            if node.get_process_pid("clickhouse") is None:
                break
            time.sleep(0.5)
        else:
            raise Exception("Server did not stop within timeout")
    finally:
        # Closing the connection lets a stalled server finish shutting down so the
        # instance can be restarted for cluster teardown.
        conn.close()
        node.start_clickhouse()
