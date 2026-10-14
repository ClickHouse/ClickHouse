"""
Test for get_zookeeper_lock_acquire_timeout_ms setting.

When ZooKeeper is unavailable and one thread holds the zookeeper_mutex while trying
to reconnect, other threads should fail fast with TIMEOUT_EXCEEDED rather than
blocking indefinitely.
"""

import pytest
import time
import concurrent.futures
import contextlib
import uuid
from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.s3_queue_common import create_table

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml", "configs/disable_ddl.xml", "configs/auxiliary_zookeepers.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Background (global context) Keeper users of this node wait at most 1 s for a busy client lock.
node_background = cluster.add_instance(
    "node_background",
    with_zookeeper=True,
    with_minio=True,
    main_configs=["configs/zookeeper.xml", "configs/disable_ddl.xml", "configs/auxiliary_zookeepers.xml"],
    user_configs=["configs/users.xml", "configs/background_lock_timeout.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("zk_name", ["default", "zookeeper2"])
def test_zookeeper_lock_acquire_timeout(started_cluster, zk_name):
    """
    Test that queries fail with TIMEOUT_EXCEEDED when they can't acquire
    the zookeeper_mutex (or auxiliary_zookeepers_mutex) within the configured timeout.

    Strategy:
    1. Pause ZooKeeper so getZooKeeper()/getAuxiliaryZooKeeper() blocks on reconnection
    2. Fire one query with long timeout (holds mutex while blocked on ZK)
    3. Fire concurrent queries with short timeout - they should fail fast
    4. Verify the short-timeout queries fail with TIMEOUT_EXCEEDED quickly
    """
    zk_filter = "" if zk_name == "default" else f" AND zookeeperName = '{zk_name}'"
    base_query = f"SELECT * FROM system.zookeeper WHERE path = '/'{zk_filter} LIMIT 1"

    node.query(base_query)
    with cluster.pause_container("zoo1"):
        # once this query fails, it means the Zookeeper session is expired
        with pytest.raises(QueryRuntimeException) as e:
            node.query(base_query)
        assert "KEEPER_EXCEPTION" in str(e.value)

        long_timeout_ms = 30000
        short_timeout_ms = 200
        # The client timeout must outlast the server-side timeout plus process startup and sanitizer scheduling delays.
        client_timeout_seconds = 60
        # End-to-end duration includes client process startup and command dispatch on sanitizer builds.
        max_query_duration_seconds = 10

        short_queries = {
            f"system_zookeeper_{i}": base_query
            for i in range(3 if zk_name == "default" else 5)
        }
        if zk_name == "default":
            short_queries["session_uptime"] = "SELECT zookeeperSessionUptime()"
            short_queries["system_reconnect"] = "SYSTEM RECONNECT ZOOKEEPER"

        # `SYSTEM RELOAD CONFIG` and `SYSTEM RELOAD ASYNCHRONOUS METRICS` first wait on their own
        # serialization mutexes, so this Keeper-lock test cannot establish the same timeout contract for them.
        results = {}

        def run_query(query_id, query, lock_acquire_timeout_ms):
            start = time.monotonic()
            try:
                node.query(
                    query,
                    settings={"get_zookeeper_lock_acquire_timeout_ms": lock_acquire_timeout_ms},
                    timeout=client_timeout_seconds,
                    query_id=query_id,
                )
                return (query_id, "success", time.monotonic() - start, None)
            except Exception as e:
                return (query_id, "error", time.monotonic() - start, str(e))

        # Fire queries concurrently:
        # - One with long timeout (will hold the mutex)
        # - Several with short timeout (should fail fast)
        failpoint = (
            "context_zookeeper_lock_acquired_pause"
            if zk_name == "default"
            else "context_auxiliary_zookeeper_lock_acquired_pause"
        )
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(short_queries) + 1) as executor:
                # Start the long-timeout query first
                long_future = executor.submit(run_query, "long", base_query, long_timeout_ms)

                try:
                    # Long-timeout query should be running and acquire the mutex
                    node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=10)

                    # Now fire short-timeout queries
                    short_futures = [
                        executor.submit(run_query, query_id, query, short_timeout_ms)
                        for query_id, query in short_queries.items()
                    ]

                    for f in concurrent.futures.as_completed(short_futures, timeout=client_timeout_seconds + 10):
                        query_id, status, duration, error = f.result()
                        results[query_id] = (status, duration, error)
                finally:
                    node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

                _, long_status, _, long_error = long_future.result()
                assert long_status == "error", f"Expected error, got {long_status}"
                assert (
                    "DB::Exception: All connection tries failed while connecting to ZooKeeper."
                    in long_error
                ), f"Expected 'all connection tries failed' error, got {long_error}"
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # Analyze results from short-timeout queries
        # Look for our specific mutex acquire timeout error message
        assert len(results) == len(short_queries), (
            f"Expected {len(short_queries)} results, got {len(results)}"
        )

        def assert_lock_timeout(query_id):
            status, duration, error = results[query_id]
            assert status == "error", f"Expected {query_id} to fail, got {status}"
            assert "TIMEOUT_EXCEEDED" in error, (
                f"Expected TIMEOUT_EXCEEDED from {query_id}, got {error}"
            )
            assert "acquiring" in error and "ZooKeeper lock" in error, (
                f"Expected 'acquiring ... ZooKeeper lock' from {query_id}, got {error}"
            )
            assert f"({short_timeout_ms} ms)" in error, (
                f"Expected the {short_timeout_ms} ms Keeper-lock timeout from {query_id}, got {error}"
            )
            assert duration < max_query_duration_seconds, (
                f"Expected {query_id} timeout < {max_query_duration_seconds}s, got {duration}s"
            )

        if zk_name == "default":
            assert_lock_timeout("system_reconnect")

        for query_id in short_queries:
            if query_id != "system_reconnect":
                assert_lock_timeout(query_id)


@pytest.mark.parametrize("zk_name", ["default", "zookeeper2"])
def test_zookeeper_lock_acquire_timeout_success_when_no_contention(started_cluster, zk_name):
    """
    Verify that queries succeed normally when there's no lock contention,
    even with a short timeout configured.
    """
    zk_filter = "" if zk_name == "default" else f" AND zookeeperName = '{zk_name}'"
    result = node.query(
        f"SELECT count() FROM system.zookeeper WHERE path = '/'{zk_filter}",
        settings={"get_zookeeper_lock_acquire_timeout_ms": 100},
    )
    assert int(result.strip()) > 0


AUX_LOCK_FAILPOINT = "context_auxiliary_zookeeper_lock_acquired_pause"


@contextlib.contextmanager
def hold_auxiliary_keeper_lock(instance, attempts=5):
    """Pause a `SYSTEM DROP REPLICA ... FROM ZKPATH` query in `Context::getAuxiliaryZooKeeper` while it holds
    the auxiliary Keeper mutex; that query takes the Keeper client outside a query pipeline.

    The fail point pauses whichever thread takes the mutex first. If a background thread wins,
    the holder query times out instead: release the fail point and try again."""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    holder = None
    try:
        for _ in range(attempts):
            query_id = f"aux_keeper_lock_holder_{uuid.uuid4().hex}"
            # One client call, so the holder reaches the mutex right after the fail point is armed.
            holder = pool.submit(
                instance.query,
                f"SYSTEM ENABLE FAILPOINT {AUX_LOCK_FAILPOINT}; "
                f"SYSTEM DROP REPLICA 'lock_holder' FROM ZKPATH 'zookeeper2:/clickhouse/{query_id}'",
                settings={"get_zookeeper_lock_acquire_timeout_ms": 500},
                query_id=query_id,
            )
            # `SYSTEM WAIT FAILPOINT` returns at once while the fail point is not enabled yet,
            # so wait for the holder's DROP REPLICA, which starts after it is enabled.
            for _ in range(300):
                if holder.done() or int(instance.query(
                    f"SELECT count() FROM system.processes WHERE query_id = '{query_id}' AND query ILIKE '%DROP REPLICA%'"
                )):
                    break
                time.sleep(0.1)
            else:
                raise AssertionError("The holder query did not start")
            instance.query(f"SYSTEM WAIT FAILPOINT {AUX_LOCK_FAILPOINT} PAUSE", timeout=30)
            try:
                holder.result(timeout=3)
            except concurrent.futures.TimeoutError:
                break
            except QueryRuntimeException as e:
                if "TIMEOUT_EXCEEDED" not in str(e):
                    raise
            instance.query(f"SYSTEM DISABLE FAILPOINT {AUX_LOCK_FAILPOINT}")
            holder = None
        else:
            raise AssertionError(f"Could not hold the auxiliary Keeper lock in {attempts} attempts")
        yield
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {AUX_LOCK_FAILPOINT}")
        pool.shutdown(wait=False)
    with pytest.raises(QueryRuntimeException) as e:
        holder.result(timeout=60)
    assert "does not look like a table path" in str(e.value)


def test_s3queue_registry_survives_lock_timeout(started_cluster):
    """The S3Queue registry thread retries a Keeper client lock timeout instead of treating it as a bug."""
    table = f"s3queue_registry_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/test_{table}"
    try:
        create_table(
            started_cluster,
            node_background,
            table,
            "unordered",
            f"{table}_data",
            additional_settings={"keeper_path": f"zookeeper2:{keeper_path}"},
        )
        with hold_auxiliary_keeper_lock(node_background):
            node_background.wait_for_log_line(
                rf"StorageObjectStorageQueue\(zookeeper2:{keeper_path}\).*will try to connect again: .*Timeout exceeded while acquiring auxiliary ZooKeeper lock",
                timeout=30,
            )
        assert node_background.query("SELECT 1") == "1\n"
        assert not node_background.contains_in_log("Logical error")
    finally:
        node_background.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_replicated_table_attach_survives_lock_timeout(started_cluster):
    """A ReplicatedMergeTree table attached while the Keeper client lock is busy stops being read-only once it is free."""
    table = f"r_{uuid.uuid4().hex[:8]}"
    try:
        node_background.query(
            f"CREATE TABLE {table} (x UInt64) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{table}', '1') "
            "ORDER BY x SETTINGS initialization_retry_period = 1"
        )
        node_background.query(f"INSERT INTO {table} VALUES (1)")
        node_background.query(f"DETACH TABLE {table}")
        with hold_auxiliary_keeper_lock(node_background):
            node_background.query(f"ATTACH TABLE {table}")
            node_background.wait_for_log_line(
                rf"{table}.*Initialization failed.*Timeout exceeded while acquiring auxiliary ZooKeeper lock",
                timeout=30,
            )
        for _ in range(60):
            if node_background.query(f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'") == "0\n":
                break
            time.sleep(1)
        else:
            raise AssertionError(f"Table {table} is still read-only")
        node_background.query(f"INSERT INTO {table} VALUES (2)")
        assert node_background.query(f"SELECT count() FROM {table}") == "2\n"
    finally:
        node_background.query(f"DROP TABLE IF EXISTS {table} SYNC")
