#!/usr/bin/env python3
import os
import time

import pytest
from kazoo.client import KazooClient

from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

cluster = ClickHouseCluster(__file__, keeper_config_dir="configs/")

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    with_remote_database_disk=False,
)


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=f"{cluster.get_instance_ip(nodename)}:2181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Verify that `max_memory_usage_soft_limit` is correctly updated
# on config reload without requiring keeper restart.
def test_soft_limit_hot_reload(started_cluster):
    started_cluster.wait_zookeeper_to_start()

    zoo1_container = started_cluster.get_container_id("zoo1")
    keeper_log = "/var/log/clickhouse-keeper/clickhouse-keeper.log"

    # zoo1 starts with max_memory_usage_soft_limit_ratio=0.5 only (no explicit value).
    startup_lines = started_cluster.exec_in_container(
        zoo1_container,
        ["bash", "-c", f'grep "max_memory_usage_soft_limit is set to" {keeper_log} || true'],
    ).strip().splitlines()
    assert len(startup_lines) >= 1, f"Expected at least 1 startup log line, got: {startup_lines}"
    startup_value = startup_lines[0].split("is set to ")[-1].strip()
    startup_count = len(startup_lines)

    # Reload with ratio=0.9 — the new limit must differ from the ratio=0.5 startup value.
    started_cluster.copy_file_to_container(
        zoo1_container,
        os.path.join(CURRENT_TEST_DIR, "configs", "keeper_config1_reload.xml"),
        "/etc/clickhouse-keeper/keeper_config1.xml",
    )

    started_cluster.exec_in_container(
        zoo1_container,
        ["bash", "-c", "pkill -HUP clickhouse-keeper 2>/dev/null || pkill -HUP clickhouse 2>/dev/null || true"],
        user="root",
    )

    # Poll for up to 30 s until a new log entry appears beyond the startup lines with a different value.
    deadline = time.time() + 30
    reloaded = False
    while time.time() < deadline:
        lines = started_cluster.exec_in_container(
            zoo1_container,
            ["bash", "-c", f'grep "max_memory_usage_soft_limit is set to" {keeper_log} || true'],
        ).strip().splitlines()
        if len(lines) > startup_count:
            reload_value = lines[-1].split("is set to ")[-1].strip()
            if reload_value != startup_value:
                reloaded = True
                break
        time.sleep(1)

    assert reloaded, (
        f"Timeout: reload value did not change from startup value '{startup_value}'. "
        f"Log lines seen: {lines}"
    )

    # Verify keeper is still accepting connections after reload.
    node_zk = get_connection_zk("zoo1")
    try:
        assert node_zk.exists("/") is not None, "Keeper root node not accessible after reload"
    finally:
        node_zk.stop()
        node_zk.close()
