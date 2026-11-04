import uuid

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/span_log.xml"],
    user_configs=["configs/shard_retry_config.xml"],
)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"])
nodes = (node1, node2, node3, node4)
failpoints = (
    "remote_query_executor_exception_retryable",
    "remote_query_executor_cannot_open_file_once",
    "remote_query_executor_cannot_read_from_file_descriptor",
    "remote_query_executor_exception_after_sending_announcement",
    "parallel_replicas_wait_for_unused_replicas",
    "remote_query_executor_exception_after_sending_data",
    "remote_query_executor_exception_after_sending_progress",
    "use_delayed_remote_source",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in nodes:
            node.query("CREATE TABLE local_table(i Int64) ENGINE=MergeTree ORDER BY i")
            node.query(
                "CREATE TABLE distributed_table(i Int64) "
                "ENGINE = Distributed(test_cluster, default, local_table, rand())"
            )
            node.query("INSERT INTO local_table SELECT number FROM numbers(1000)")
        node4.query(
            "CREATE TABLE hedged_distributed_table(i Int64) "
            "ENGINE = Distributed(hedged_retry_cluster, default, local_table, rand())"
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_retry_state(started_cluster):
    # Rebuild cluster pools so earlier tests' replica error counts cannot select a
    # healthy replica first and accidentally bypass the retry under test.
    for node in nodes:
        node.query("SYSTEM DROP DNS CACHE")
    try:
        yield
    finally:
        for node in nodes:
            for failpoint in failpoints:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")


def retries(query_id, node=node1):
    node.query("SYSTEM FLUSH LOGS query_log")
    rows = node.query(
        "SELECT ProfileEvents['DistributedTryCount'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND is_initial_query "
        "AND type IN ('QueryFinish', 'ExceptionWhileProcessing')"
    ).splitlines()
    assert len(rows) == 1, rows
    return int(rows[0])


# Exercise each connection-construction path, without a full settings matrix.
@pytest.mark.parametrize(
    "source,hedged,async_read,lazy",
    [
        ("distributed_table", 0, 0, False),
        ("distributed_table", 1, 1, False),
        ("remote('node1|node2,node3|node4', default, local_table)", 0, 1, False),
        ("cluster('test_cluster', default, local_table)", 0, 1, False),
        ("distributed_table", 1, 1, True),
    ],
)
def test_leaf_queries_retried(source, hedged, async_read, lazy):
    for node in (node1, node3):
        node.query("SYSTEM ENABLE FAILPOINT remote_query_executor_exception_retryable")
    if lazy:
        node1.query("SYSTEM ENABLE FAILPOINT use_delayed_remote_source")

    query_id = str(uuid.uuid4())
    assert node1.query(
        f"SELECT count(), sum(i) FROM {source}",
        query_id=query_id,
        settings={
            "load_balancing": "in_order",
            "prefer_localhost_replica": 0,
            "use_hedged_requests": hedged,
            "async_socket_for_remote": async_read,
            "async_query_sending_for_remote": async_read,
            "opentelemetry_start_trace_probability": 1,
        },
    ) == "2000\t999000\n"
    assert retries(query_id) == 2

    node1.query("SYSTEM FLUSH LOGS opentelemetry_span_log")
    assert node1.query(
        "SELECT count(), countIf(status_code = 'OK'), "
        "countIf(attribute['clickhouse.cancelled'] = '1') "
        "FROM system.opentelemetry_span_log "
        "WHERE operation_name = 'RemoteQueryExecutor::execute' "
        f"AND attribute['clickhouse.query_id'] = '{query_id}'"
    ) == "2\t2\t0\n"


@pytest.mark.parametrize("limit", [0, 1])
def test_leaf_file_read_failure(limit):
    # Keep the first leaf failing so success requires a different replica.
    node1.query(
        "SYSTEM ENABLE FAILPOINT remote_query_executor_cannot_read_from_file_descriptor"
    )
    query_id = str(uuid.uuid4())
    settings = {
        "distributed_shard_retry_count": limit,
        "load_balancing": "in_order",
        "prefer_localhost_replica": 0,
        "use_hedged_requests": 0,
    }
    query = "SELECT count(), sum(i) FROM hedged_distributed_table"
    if limit == 0:
        with pytest.raises(QueryRuntimeException, match="CANNOT_READ_FROM_FILE_DESCRIPTOR"):
            node4.query(query, query_id=query_id, settings=settings)
    else:
        assert node4.query(query, query_id=query_id, settings=settings) == "1000\t499500\n"
        assert node4.query(
            "SELECT host_name FROM system.clusters "
            "WHERE cluster = 'hedged_retry_cluster' AND errors_count > 0 ORDER BY host_name"
        ) == "node1\n"
    assert retries(query_id, node4) == limit


@pytest.mark.parametrize("packet", ["data", "progress"])
def test_no_retry_after_visible_state(packet):
    for node in nodes:
        node.query(
            f"SYSTEM ENABLE FAILPOINT remote_query_executor_exception_after_sending_{packet}"
        )
    query_id = str(uuid.uuid4())
    with pytest.raises(QueryRuntimeException, match="TOO_MANY_SIMULTANEOUS_QUERIES"):
        node1.query(
            "SELECT i FROM distributed_table",
            query_id=query_id,
            settings={
                "prefer_localhost_replica": 0,
                "max_block_size": 16,
                # Keep periodic progress from masking the independent data guard.
                "interactive_delay": 100000000,
            },
        )
    assert retries(query_id) == 0


@pytest.mark.parametrize("limit", [0, 5, 256])
def test_retry_limit(limit):
    # A single failing shard makes the exact number of retries deterministic.
    # The larger synchronous case also exercises the iterative retry loop.
    for node in (node1, node2):
        node.query("SYSTEM ENABLE FAILPOINT remote_query_executor_exception_retryable")
    query_id = str(uuid.uuid4())
    with pytest.raises(QueryRuntimeException, match="TOO_MANY_SIMULTANEOUS_QUERIES"):
        node1.query(
            "SELECT sum(i) FROM remote('node1|node2', default, local_table)",
            query_id=query_id,
            settings={
                "distributed_shard_retry_count": limit,
                "prefer_localhost_replica": 0,
                "use_hedged_requests": 0,
                "async_socket_for_remote": 0,
                "async_query_sending_for_remote": 0,
            },
        )
    assert retries(query_id) == limit


def parallel_replica_settings():
    return {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 3,
        "cluster_for_parallel_replicas": "hedged_retry_cluster",
        "parallel_replicas_for_non_replicated_merge_tree": 1,
        "parallel_replicas_local_plan": 0,
        "parallel_replicas_min_number_of_rows_per_replica": 0,
        "distributed_shard_retry_count": 1,
        "interactive_delay": 100000000,
    }


@pytest.mark.parametrize("plan_based", [0, 1])
def test_parallel_replicas_retry_before_announcement(plan_based):
    # Every assigned replica fails once. Recovery must preserve its coordinator
    # identity and read each range once, even though every error count increases.
    for node in (node1, node2, node3):
        node.query(
            "SYSTEM ENABLE FAILPOINT remote_query_executor_cannot_open_file_once"
        )
    node4.query("SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas")
    query_id = str(uuid.uuid4())
    assert node4.query(
        "SELECT count(), sum(i) FROM local_table",
        query_id=query_id,
        settings={
            **parallel_replica_settings(),
            "parallel_replicas_plan_based": plan_based,
        },
    ) == "1000\t499500\n"
    assert retries(query_id, node4) == 3
    assert node4.query(
        "SELECT count() FROM system.clusters "
        "WHERE cluster = 'hedged_retry_cluster' AND errors_count > 0"
    ) == "3\n"


def test_parallel_replicas_do_not_retry_after_announcement():
    for node in (node1, node2, node3):
        node.query(
            "SYSTEM ENABLE FAILPOINT remote_query_executor_exception_after_sending_announcement"
        )
    query_id = str(uuid.uuid4())
    with pytest.raises(QueryRuntimeException, match="TOO_MANY_SIMULTANEOUS_QUERIES"):
        node4.query(
            "SELECT sum(i) FROM local_table",
            query_id=query_id,
            settings=parallel_replica_settings(),
        )
    assert retries(query_id, node4) == 0


@pytest.mark.parametrize("async_read", [0, 1])
def test_hedged_retry_releases_pool_entries(async_read):
    node1.query("SYSTEM ENABLE FAILPOINT remote_query_executor_cannot_open_file_once")
    query_id = str(uuid.uuid4())
    # Resolve the schema locally, then execute through a one-slot remote pool.
    # The retry must release the failed attempt's entry before reconnecting.
    assert node1.query(
        "SELECT count(), sum(i) FROM remote('node1', default, local_table)",
        query_id=query_id,
        settings={
            "distributed_shard_retry_count": 1,
            "distributed_connections_pool_size": 1,
            "connection_pool_max_wait_ms": 1000,
            "use_hedged_requests": 1,
            "prefer_localhost_replica": 0,
            "async_socket_for_remote": async_read,
            "async_query_sending_for_remote": async_read,
        },
        timeout=20,
    ) == "1000\t499500\n"
    assert retries(query_id) == 1


@pytest.mark.parametrize("serialize_query_plan", [0, 1])
def test_hedged_retry_penalizes_packet_producer(serialize_query_plan):
    # Start on a slow healthy replica, then hedge onto a replica that fails. The
    # retry must reach the third, healthy replica and charge the second pool.
    slow_config = "/etc/clickhouse-server/users.d/retry_slow_replica.xml"
    node1.replace_config(
        slow_config,
        "<clickhouse><profiles><default>"
        "<sleep_after_receiving_query_ms>2000</sleep_after_receiving_query_ms>"
        "</default></profiles></clickhouse>",
    )
    node1.query("SYSTEM RELOAD CONFIG")
    node2.query("SYSTEM ENABLE FAILPOINT remote_query_executor_exception_retryable")
    query_id = str(uuid.uuid4())
    try:
        assert node4.query(
            "SELECT sum(i) FROM hedged_distributed_table",
            query_id=query_id,
            settings={
                "distributed_shard_retry_count": 1,
                "use_hedged_requests": 1,
                "prefer_localhost_replica": 0,
                "load_balancing": "in_order",
                "receive_data_timeout_ms": 100,
                "serialize_query_plan": serialize_query_plan,
            },
        ) == "499500\n"
        assert retries(query_id, node4) == 1
        assert node4.query(
            "SELECT host_name FROM system.clusters "
            "WHERE cluster = 'hedged_retry_cluster' AND errors_count > 0 ORDER BY host_name"
        ) == "node2\n"
    finally:
        node1.replace_config(slow_config, "<clickhouse/>")
        node1.query("SYSTEM RELOAD CONFIG")
