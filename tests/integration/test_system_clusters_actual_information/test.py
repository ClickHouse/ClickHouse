import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query_with_retry(
            "CREATE TABLE distributed (id UInt32, date Date) ENGINE = Distributed('test_cluster', 'default', 'replicated')"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    node.query("SYSTEM RELOAD CONFIG")
    error = node.query_and_get_error(
        "SELECT count() FROM distributed SETTINGS receive_timeout=1, handshake_timeout_ms=1"
    )

    result = node.query(
        "SELECT errors_count, estimated_recovery_time FROM system.clusters WHERE cluster='test_cluster' and host_name='node_1'"
    )
    errors_count, recovery_time = map(int, result.split())
    assert errors_count == 3

    while True:
        time.sleep(1)

        result = node.query(
            "SELECT errors_count, estimated_recovery_time FROM system.clusters WHERE cluster='test_cluster' and host_name='node_1'"
        )
        prev_time = recovery_time
        errors_count, recovery_time = map(int, result.split())

        if recovery_time == 0:
            break

        assert recovery_time < prev_time
        assert errors_count > 0

    assert recovery_time == 0
    assert errors_count == 0
