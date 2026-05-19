#!/usr/bin/env python3
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

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
    with_remote_database_disk=False,  # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    main_configs=["configs/setting.xml"],
    mem_limit='14g'
)


def get_connection_zk(nodename, timeout=30.0):
    # NOTE: here we need KazooClient without implicit retries! (KazooClientWithImplicitRetries)
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


def test_soft_limit_create(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")
    started_cluster.wait_zookeeper_to_start()
    node_zk = get_connection_zk("zoo1")
    test_path = "/test_soft_limit"

    # Retry logic for initial znode creation
    # node_zk.create can occasionally hang on CI, so we add a retry loop with timeout to ensure it doesn't block indefinitely (900s).
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    # Normally, this is expected to succeed on the first try.
                    node_zk.create,
                    test_path,
                    b"abc",
                    ephemeral=False,
                    makepath=True,
                    sequence=False,
                )
                # Wait up to 30 seconds
                future.result(timeout=30)
            break  # success, exit loop
        except TimeoutError:
            logging.error(f"Attempt {attempt+1} timed out")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {e}")
    else:
        raise RuntimeError(
            f"Failed to create znode {test_path} after {max_attempts} attempts"
        )

    try:
        loop_time = 100000
        batch_size = 10000

        for i in range(0, loop_time, batch_size):
            node.query(
                f"""INSERT INTO system.zookeeper (name, path, value)
                SELECT 'node_' || number::String, '{test_path}', repeat('a', 3000)
                FROM numbers({i}, {batch_size})
            """
            )
    except Exception as e:
        # the message contains out of memory so the users will not be confused.
        assert "out of memory" in str(e).lower()

        txn = node_zk.transaction()
        for i in range(10):
            txn.delete(f"{test_path}/node_{i}")

        txn.create(f"{test_path}/node_1000001_{i}", b"abcde")
        txn.commit()

        node.query_with_retry(
            "SYSTEM FLUSH LOGS metric_log; SELECT sum(ProfileEvent_ZooKeeperHardwareExceptions) FROM system.metric_log",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=5,
            sleep_time=1,
        )

        return

    raise Exception("all records are inserted but no error occurs")


def test_soft_limit_hot_reload(started_cluster):
    started_cluster.wait_zookeeper_to_start()

    zoo1_container = started_cluster.get_container_id("zoo1")
    keeper_log = "/var/log/clickhouse-keeper/clickhouse-keeper.log"

    # At startup the limit was set to 300 000 000 bytes ≈ 286.10 MiB (explicit value).
    startup_log = started_cluster.exec_in_container(
        zoo1_container,
        ["bash", "-c", f'grep "max_memory_usage_soft_limit is set to" {keeper_log} || true'],
    )
    assert "286.10 MiB" in startup_log, (
        f"Expected startup log to contain '286.10 MiB', got:\n{startup_log}"
    )

    # Overwrite keeper_config1.xml with the reload variant that uses ratio=0.9 instead of an explicit value.
    started_cluster.copy_file_to_container(
        zoo1_container,
        os.path.join(CURRENT_TEST_DIR, "configs", "keeper_config1_reload.xml"),
        "/etc/clickhouse-keeper/keeper_config1.xml",
    )

    # Send SIGHUP to the keeper process to trigger config reload.
    started_cluster.exec_in_container(
        zoo1_container,
        ["bash", "-c", "pkill -HUP clickhouse-keeper 2>/dev/null || pkill -HUP clickhouse 2>/dev/null || true"],
        user="root",
    )

    # Poll for up to 30 s until a second log entry appears with a value different from the
    # startup "286.10 MiB". ratio=0.9 of any realistic machine memory is  differs from 286 MiB.
    deadline = time.time() + 30
    reloaded = False
    while time.time() < deadline:
        lines = started_cluster.exec_in_container(
            zoo1_container,
            ["bash", "-c", f'grep "max_memory_usage_soft_limit is set to" {keeper_log} || true'],
        ).strip().splitlines()
        reload_lines = [l for l in lines if "286.10 MiB" not in l]
        if reload_lines:
            reloaded = True
            break
        time.sleep(1)

    assert reloaded, (
        "Timeout waiting for a reload log entry with a value different from '286.10 MiB' after SIGHUP. "
        f"Log lines seen: {lines}"
    )

    # Verify keeper is still accepting connections after reload.
    node_zk = get_connection_zk("zoo1")
    try:
        assert node_zk.exists("/") is not None, "Keeper root node not accessible after reload"
    finally:
        node_zk.stop()
        node_zk.close()
