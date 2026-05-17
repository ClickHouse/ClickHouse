#!/usr/bin/env python3

import uuid
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
import time
import pytest
from kazoo.retry import RetryFailedError

def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_ttl_disabled():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    cluster.add_instance(
        "node1", main_configs=["configs/disable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    node1_zk = None

    cluster.start()

    node1_zk = get_fake_zk(cluster, "node1")
    with pytest.raises(RetryFailedError):
        node1_zk.create("/test_alive", b"aaaa", ttl=1000)
    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
