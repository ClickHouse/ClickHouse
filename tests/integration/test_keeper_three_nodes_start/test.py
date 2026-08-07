#!/usr/bin/env python3

import uuid
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_smoke():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None

    try:
        cluster.start()

        node1_zk = get_fake_zk(cluster, "node1")
        node1_zk.create("/test_alive", b"aaaa")

    finally:
        cluster.shutdown()

        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
