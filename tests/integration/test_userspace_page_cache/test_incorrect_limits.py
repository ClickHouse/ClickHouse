import os.path as p

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node_incorrect_limits",
            main_configs=["configs/conf.xml", "configs/incorrect_limits.xml"],
            with_minio=True,
        )
        # The cluster should fail to start when trying to start the node with incorrect limits
        with pytest.raises(Exception) as exc_info:
            cluster.start()

        assert "Instance `node_incorrect_limits' failed to start" in str(exc_info.value)

        yield cluster
    finally:
        cluster.shutdown()


def test_limits(started_cluster):
    """
    Node with incorrect limits should not start:
    <page_cache_min_size>10000000</page_cache_min_size>
    <page_cache_max_size>8192</page_cache_max_size>
    In the above config, page_cache_min_size is larger than page_cache_max_size,
    in this case the limits are validated and exception is thrown during server.
    startup.
    """
    logs = ""
    error_logs_file = p.join(
        cluster.instances_dir,
        "node_incorrect_limits",
        "logs",
        "clickhouse-server.err.log",
    )
    with open(error_logs_file, "r") as f:
        logs = f.read()
    assert (
        "Invalid page cache configuration: page_cache_min_size (10000000) is greater than page_cache_max_size (8192)."
        in logs
    )
