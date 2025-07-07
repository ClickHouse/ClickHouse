#!/usr/bin/env python3
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=[], stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown():
    node.query("SYSTEM ENABLE FAILPOINT lazy_pipe_fds_fail_close")
    node.stop_clickhouse()
