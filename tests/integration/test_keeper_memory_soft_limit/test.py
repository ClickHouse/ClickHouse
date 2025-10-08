#!/usr/bin/env python3
import pytest
from kazoo.client import KazooClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, keeper_config_dir="configs/")

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    with_remote_database_disk=False,  # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    main_configs=["configs/setting.xml"],
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
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            node_zk.create(test_path, b"abc")
            break
        except (ConnectionLoss, KazooException) as e:
            print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
            time.sleep(3)
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
        node.query("SYSTEM FLUSH LOGS metric_log")

        # Retry fetching metric to make it stable
        retries = 5
        wait_seconds = 1
        for _ in range(5):
            result = int(
                node.query(
                    "SELECT sum(ProfileEvent_ZooKeeperHardwareExceptions) FROM system.metric_log"
                ).strip()
            )
            if result > 0:
                break
            time.sleep(wait_seconds)
        else:
            raise Exception(
                "ZooKeeper hardware exceptions metric not registered after OOM"
            )

        return

    raise Exception("all records are inserted but no error occurs")
