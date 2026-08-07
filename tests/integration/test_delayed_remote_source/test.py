import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/clusters.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_delayed_remote_source(start_cluster):
    err = node.query_and_get_error(
        """
        SYSTEM ENABLE FAILPOINT use_delayed_remote_source;
        SELECT count() FROM remoteSecure('localhost');
        SYSTEM DISABLE FAILPOINT use_delayed_remote_source;
        """
    )

    assert("All connection tries failed" in err)
