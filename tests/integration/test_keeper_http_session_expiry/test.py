#!/usr/bin/env python3

import concurrent.futures
import time
import uuid

import pytest
import requests

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# Disable `with_remote_database_disk` as the test does not use the default Keeper.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def storage_url(node, path):
    return f"http://{node.ip_address}:9182/api/v1/storage{path}"


def run_command(node, command, timeout):
    return requests.get(
        f"http://{node.ip_address}:9182/api/v1/commands",
        params={"command": command},
        timeout=timeout,
    )


def test_pending_requests_fail_on_session_expiry(started_cluster):
    keeper_utils.wait_nodes(cluster, [node1, node2])
    leader = keeper_utils.get_leader(cluster, [node1, node2])
    follower = node2 if leader == node1 else node1

    prefix = "/" + uuid.uuid4().hex
    find_command = f"find_big_family '{prefix}'"

    response = requests.post(storage_url(leader, prefix), timeout=30)
    assert response.status_code == 201, response.text

    response = run_command(leader, find_command, timeout=30)
    assert response.status_code == 200, response.text
    assert prefix in response.json()["result"]

    with PartitionManager() as pm:
        pm.partition_instances(leader, follower)
        response = requests.get(storage_url(leader, prefix), timeout=30)
        assert response.status_code == 200, response.text

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # The write cannot commit without the follower, so the 5 s session expires while it is pending.
            write = executor.submit(
                requests.post, storage_url(leader, prefix + "/child"), timeout=20
            )
            time.sleep(1)
            # The command's reads are queued behind the pending write of the same session.
            find = executor.submit(run_command, leader, find_command, 20)
            write_response = write.result()
            find_response = find.result()

    assert write_response.status_code == 500, write_response.text
    assert "Session expired" in write_response.text
    assert find_response.status_code == 200, find_response.text
    assert "Session expired" in find_response.json()["result"]

    keeper_utils.wait_nodes(cluster, [node1, node2])
    response = requests.get(storage_url(node1, prefix), timeout=30)
    assert response.status_code == 200, response.text
