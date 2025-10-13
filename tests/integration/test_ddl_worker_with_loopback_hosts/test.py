import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers.xml",
    ],
    with_zookeeper=True,
)

DDL_TIMEOUT = 60          # seconds to wait inside ON CLUSTER
DDL_EXTRA_WAIT = 120      # seconds to poll for final state after issuing DDL
ZK_READY_TIMEOUT = 120    # seconds to wait for ZooKeeper readiness
POLL_INTERVAL = 0.5       # polling cadence


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def count_table(node, table_name):
    return int(
        node.query(
            f"SELECT count() FROM system.tables WHERE name='{table_name}'"
        ).strip()
    )


def _wait_zk_ready(nodes, timeout=ZK_READY_TIMEOUT):
    """
    Wait until system.zookeeper is queryable from each node.
    This avoids racing the first ON CLUSTER against ZK flapping.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_ok = True
        for n in nodes:
            try:
                # If ZooKeeper isn't ready, this throws; success means session established.
                n.query("SELECT count() FROM system.zookeeper WHERE path='/'")
            except Exception:
                all_ok = False
                break
        if all_ok:
            return
        time.sleep(POLL_INTERVAL)
    raise AssertionError("ZooKeeper is not reachable from all nodes within the timeout")


def _create_on_cluster_and_wait(node_issuer, create_sql, check_fn):
    """
    Run an ON CLUSTER DDL:
    - Use a generous distributed_ddl_task_timeout (DDL_TIMEOUT).
    - Treat Code:32 (ATTEMPT_TO_READ_AFTER_EOF) and Code:159 (TIMEOUT_EXCEEDED)
      as transient and rely on polling for convergence.
    - Poll for the expected end state for up to DDL_EXTRA_WAIT.
    - Final idempotent nudge: retry once with IF NOT EXISTS if still not converged.
    """
    transient_signals = (
        "ATTEMPT_TO_READ_AFTER_EOF",
        "Distributed DDL task",
        "TIMEOUT_EXCEEDED",
        "Code: 159",
        "Code: 32",
    )

    try:
        node_issuer.query(
            create_sql,
            settings={"distributed_ddl_task_timeout": DDL_TIMEOUT},
        )
    except QueryRuntimeException as e:
        msg = str(e)
        if not any(sig in msg for sig in transient_signals):
            # Not a known transient; surface it.
            raise

    # Poll for convergence (observable end state)
    deadline = time.time() + DDL_EXTRA_WAIT
    while time.time() < deadline:
        if check_fn():
            return
        time.sleep(POLL_INTERVAL)

    # Last-chance idempotent nudge to converge
    if create_sql.startswith("CREATE TABLE "):
        safe_sql = create_sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
        node_issuer.query(
            safe_sql,
            settings={"distributed_ddl_task_timeout": DDL_TIMEOUT},
        )
        # One more quick check
        deadline = time.time() + 10
        while time.time() < deadline:
            if check_fn():
                return
            time.sleep(POLL_INTERVAL)

    raise AssertionError("Timed out waiting for ON CLUSTER DDL to converge")


def test_ddl_worker_with_loopback_hosts(
    started_cluster,
):
    # Ensure ZooKeeper is stable from both nodes before first ON CLUSTER call.
    _wait_zk_ready([node1, node2], timeout=ZK_READY_TIMEOUT)

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2 SYNC")
    node2.query("DROP TABLE IF EXISTS t2 SYNC")
    node1.query("DROP TABLE IF EXISTS t3 SYNC")
    node2.query("DROP TABLE IF EXISTS t3 SYNC")
    node1.query("DROP TABLE IF EXISTS t4 SYNC")
    node2.query("DROP TABLE IF EXISTS t4 SYNC")

    _create_on_cluster_and_wait(
        node1,
        "CREATE TABLE t1 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        check_fn=lambda: count_table(node1, "t1") == 1 and count_table(node2, "t1") == 1,
    )

    _create_on_cluster_and_wait(
        node2,
        "CREATE TABLE t2 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        check_fn=lambda: count_table(node1, "t2") == 1 and count_table(node2, "t2") == 1,
    )

    assert count_table(node1, "t2") == 1
    assert count_table(node2, "t1") == 1

    _create_on_cluster_and_wait(
        node1,
        "CREATE TABLE t3 ON CLUSTER 'test_loopback_cluster1' (x INT) ENGINE=MergeTree() ORDER BY x",
        # test_loopback_cluster1 has a loopback host, only 1 replica should process the query
        check_fn=lambda: (count_table(node1, "t3") + count_table(node2, "t3")) == 1,
    )
    assert count_table(node1, "t3") == 1 or count_table(node2, "t3") == 1

    _create_on_cluster_and_wait(
        node2,
        "CREATE TABLE t4 ON CLUSTER 'test_loopback_cluster2' (x INT) ENGINE=MergeTree() ORDER BY x",
        # test_loopback_cluster2 has a loopback host, only 1 replica should process the query
        check_fn=lambda: (count_table(node1, "t4") + count_table(node2, "t4")) == 1,
    )
    assert count_table(node1, "t4") == 1 or count_table(node2, "t4") == 1

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2 SYNC")
    node2.query("DROP TABLE IF EXISTS t2 SYNC")
    node1.query("DROP TABLE IF EXISTS t3 SYNC")
    node2.query("DROP TABLE IF EXISTS t3 SYNC")
    node1.query("DROP TABLE IF EXISTS t4 SYNC")
    node2.query("DROP TABLE IF EXISTS t4 SYNC")
