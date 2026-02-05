#!/usr/bin/env python3

import os
import re
import time
import pytest
import logging
import random
import string
from multiprocessing.dummy import Pool

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_node1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper_node2.xml"], stay_alive=True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    logging.info("Starting cluster")
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_zk_connection(node_name):
    return ku.get_fake_zk(cluster, node_name)


def change_byte_limit_and_restart(node, byte_limit):
    logging.info(f"Changing {node.name} byte limit to {byte_limit}...")
    # Use sed-compatible regex: [0-9]\+ matches one or more digits in sed
    node.replace_in_config(
        f"/etc/clickhouse-server/config.d/keeper_{node.name}.xml",
        "<max_requests_append_bytes_size>[0-9]\\+</max_requests_append_bytes_size>",
        f"<max_requests_append_bytes_size>{byte_limit}</max_requests_append_bytes_size>",
    )
    node.restart_clickhouse(stop_start_wait_sec=10)
    ku.wait_until_connected(cluster, node)


def extract_entries_length_values(log_text):
    """
    Extract all EntriesLength values from append_entries logs.
    """
    # Pattern: EntriesLength=<number>
    pattern = r"EntriesLength=(\d+)"
    matches = re.findall(pattern, log_text)
    return [int(m) for m in matches]


def create_sequential_nodes_worker(args):
    """Worker function to create sequential nodes from a single connection"""
    connection_id, node_name, base_path, end_time = args
    zk = get_zk_connection(node_name)
    counter = 0
    try:
        while time.time() < end_time:
            # Use ZooKeeper's sequential node feature
            try:
                created_path = zk.create(f"{base_path}/node_", "", sequence=True)
                counter += 1
            except Exception as e:
                logging.warning(f"Connection {connection_id} failed to create sequential node: {e}")
                break
    finally:
        zk.stop()
        zk.close()
    return counter


def create_nodes_and_wait_replication(prefix, num_connections, duration_seconds):
    """Create sequential nodes using multiple connections in parallel for a duration"""
    # Create the base path once
    base_path = f"/{prefix}"
    zk = get_zk_connection("node1")
    try:
        zk.create(base_path, b"", makepath=True)
    finally:
        zk.stop()
        zk.close()

    end_time = time.time() + duration_seconds
    pool = Pool(num_connections)

    # Prepare arguments for each worker - all use same base_path
    worker_args = [(i, "node1", base_path, end_time) for i in range(num_connections)]

    # Run workers in parallel
    results = pool.map(create_sequential_nodes_worker, worker_args)
    pool.close()
    pool.join()

    total_created = sum(results)
    logging.info(f"Created {total_created} nodes total from {num_connections} connections in {duration_seconds}s")

    # Wait for all created nodes to be replicated
    last_children_num = 0
    for i in range(30):
        zk_check = get_zk_connection("node2")
        try:
            children = zk_check.get_children(base_path)
            last_children_num = len(children)
            if len(children) == total_created:
                break
        finally:
            zk_check.stop()
            zk_check.close()
        time.sleep(1)
    else:
        assert False, f"Base path {base_path} should have {total_created} children on node2 but it has {last_children_num}"

def test_keeper_max_append_byte_size():
    # Generate random suffix for unique paths
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

    change_byte_limit_and_restart(node1, byte_limit=0)
    logging.info("Creating sequential nodes from 20 connections for 10 seconds with unlimited byte size...")
    create_nodes_and_wait_replication(f"test_unlimited_{random_suffix}", num_connections=20, duration_seconds=10)

    full_log = node1.grep_in_log("append_entries for", only_latest=True)

    entries_lengths = extract_entries_length_values(full_log)

    assert any(length > 1 for length in entries_lengths), "Expected some batched append calls with unlimited byte size"

    change_byte_limit_and_restart(node1, byte_limit=1)

    logging.info("Creating sequential nodes from 20 connections for 10 seconds with 1-byte limit...")
    create_nodes_and_wait_replication(f"test_limited_{random_suffix}", num_connections=20, duration_seconds=10)

    full_log = node1.grep_in_log("append_entries for", only_latest=True)

    entries_lengths = extract_entries_length_values(full_log)
    assert all(length <= 1 for length in entries_lengths), f"Expected all append requests to have at most 1 entry, {[length for length in entries_lengths if length > 1]}"
