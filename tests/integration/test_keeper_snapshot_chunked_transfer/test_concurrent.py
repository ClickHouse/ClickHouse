#!/usr/bin/env python3
import concurrent.futures
import os
import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.keeper_snapshot_utils import (
    generate_keeper_configs,
    fill_test_tree,
    cleanup_test_tree,
    verify_test_tree,
    get_kill_timestamp,
    get_received_snapshot_info,
)


configs_dir = os.path.join(os.path.dirname(__file__), "configs")
# 5-node clusters: quorum=3, so 2 followers can lag simultaneously while 3 nodes maintain quorum.
generate_keeper_configs(configs_dir, [
    (["enable_keeper_conc1.xml",     "enable_keeper_conc2.xml",     "enable_keeper_conc3.xml",
      "enable_keeper_conc4.xml",     "enable_keeper_conc5.xml"],
     ["node_conc1", "node_conc2", "node_conc3", "node_conc4", "node_conc5"], 4096),
    (["enable_keeper_conc_s3_1.xml", "enable_keeper_conc_s3_2.xml", "enable_keeper_conc_s3_3.xml",
      "enable_keeper_conc_s3_4.xml", "enable_keeper_conc_s3_5.xml"],
     ["node_concs1", "node_concs2", "node_concs3", "node_concs4", "node_concs5"], 4096, True),
])

_small_buf_cfg = os.path.join(configs_dir, "small_remote_buf_user.xml")

cluster = ClickHouseCluster(__file__)

node_conc1 = cluster.add_instance("node_conc1", main_configs=["configs/enable_keeper_conc1.xml"], stay_alive=True, with_remote_database_disk=False)
node_conc2 = cluster.add_instance("node_conc2", main_configs=["configs/enable_keeper_conc2.xml"], stay_alive=True, with_remote_database_disk=False)
node_conc3 = cluster.add_instance("node_conc3", main_configs=["configs/enable_keeper_conc3.xml"], stay_alive=True, with_remote_database_disk=False)
node_conc4 = cluster.add_instance("node_conc4", main_configs=["configs/enable_keeper_conc4.xml"], stay_alive=True, with_remote_database_disk=False)
node_conc5 = cluster.add_instance("node_conc5", main_configs=["configs/enable_keeper_conc5.xml"], stay_alive=True, with_remote_database_disk=False)

node_concs1 = cluster.add_instance("node_concs1", main_configs=["configs/enable_keeper_conc_s3_1.xml"], user_configs=[_small_buf_cfg], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node_concs2 = cluster.add_instance("node_concs2", main_configs=["configs/enable_keeper_conc_s3_2.xml"], user_configs=[_small_buf_cfg], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node_concs3 = cluster.add_instance("node_concs3", main_configs=["configs/enable_keeper_conc_s3_3.xml"], user_configs=[_small_buf_cfg], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node_concs4 = cluster.add_instance("node_concs4", main_configs=["configs/enable_keeper_conc_s3_4.xml"], user_configs=[_small_buf_cfg], stay_alive=True, with_minio=True, with_remote_database_disk=False)
node_concs5 = cluster.add_instance("node_concs5", main_configs=["configs/enable_keeper_conc_s3_5.xml"], user_configs=[_small_buf_cfg], stay_alive=True, with_minio=True, with_remote_database_disk=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


CONCURRENT_FOLLOWERS_PARAMS = [
    pytest.param({"leader": node_conc1, "lagging": [node_conc4, node_conc5]}, id="local_disk"),
    pytest.param({"leader": node_concs1, "lagging": [node_concs4, node_concs5]}, id="remote_disk"),
]


@pytest.mark.parametrize("nodes", CONCURRENT_FOLLOWERS_PARAMS)
def test_concurrent_followers_fetch_snapshot(started_cluster, nodes):
    """Two followers lagging behind must both recover correctly when fetching
    the same snapshot concurrently from the leader. Tests shared loader access
    under concurrent load — especially the RemoteSnapshotLoader mutex on S3."""
    node_leader = nodes["leader"]
    lagging = nodes["lagging"]
    prefix = "/test_concurrent_followers_snapshot"

    cleanup_test_tree(cluster, node_leader, prefix)

    kill_times = {node: get_kill_timestamp(node) for node in lagging}

    for node in lagging:
        node.stop_clickhouse(kill=True)

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    fill_test_tree(leader_zk, prefix)

    def start_and_wait(node):
        node.start_clickhouse(20)
        keeper_utils.wait_until_connected(cluster, node)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(lagging)) as pool:
        futures = [pool.submit(start_and_wait, node) for node in lagging]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    leader_zk = keeper_utils.get_fake_zk(cluster, node_leader.name)
    for node in lagging:
        node_zk = keeper_utils.get_fake_zk(cluster, node.name)
        verify_test_tree(leader_zk, node_zk, prefix)
        received = get_received_snapshot_info(node, kill_times[node])
        assert received is not None, f"{node.name} did not receive a snapshot"

    cleanup_test_tree(cluster, node_leader, prefix)
