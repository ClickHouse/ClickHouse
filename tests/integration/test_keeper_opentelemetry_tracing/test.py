#!/usr/bin/env python3
import pytest
import uuid
from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import wait_nodes


@pytest.fixture(scope="module", params=["xid64", "xid32"])
def started_cluster(request):
    xid_mode = request.param
    cluster = ClickHouseCluster(__file__)

    clickhouse = cluster.add_instance(
        "clickhouse",
        main_configs=[
            "configs/server/use_keeper.xml",
            f"configs/{xid_mode}/server/use_xid_64.xml",
            "configs/server/enable_span_log.xml",
        ],
        user_configs=[
            "configs/server/users.xml",
        ],
        with_remote_database_disk=False,
    )

    # we're running keepers as standalone ClickHouse servers as we need to collect span log from them
    keeper1 = cluster.add_instance(
        "clickhouseKeeper1",
        main_configs=[
            "configs/keeper/keeper1.xml",
            f"configs/{xid_mode}/keeper/use_xid_64.xml",
        ],
    )
    keeper2 = cluster.add_instance(
        "clickhouseKeeper2",
        main_configs=[
            "configs/keeper/keeper2.xml",
            f"configs/{xid_mode}/keeper/use_xid_64.xml",
        ],
    )
    keeper3 = cluster.add_instance(
        "clickhouseKeeper3",
        main_configs=[
            "configs/keeper/keeper3.xml",
            f"configs/{xid_mode}/keeper/use_xid_64.xml",
        ],
    )

    try:
        cluster.start()
        # Wait for all keeper nodes to be ready and form a cluster with a leader
        wait_nodes(cluster, [keeper1, keeper2, keeper3])
        yield cluster
    finally:
        cluster.shutdown()


def test_keeper_opentelemetry_tracing(started_cluster):
    """
    1. Run a DDL query.
    2. Find related ZooKeeper write (i.e. create, set, or multi) operations.
    3. For each write operation, make sure that the replica that we have session with and the leader have the right spans in the right sequence.
    4. Find related ZooKeeper read (i.e. get, exists, or list) operations.
    5. For each read operation, make sure that the replica that we have session with has right spans in the right sequence and that other replicas have no spans.
    """
    clickhouse = started_cluster.instances["clickhouse"]
    keeper1 = started_cluster.instances["clickhouseKeeper1"]
    keeper2 = started_cluster.instances["clickhouseKeeper2"]
    keeper3 = started_cluster.instances["clickhouseKeeper3"]

    db = f"test_keeper_opentelemetry_tracing_{uuid.uuid4()}_database"
    t = f"test_keeper_opentelemetry_tracing_{uuid.uuid4()}_table"

    ddl_query_id = str(uuid.uuid4())

    # run DDL query
    clickhouse.query(
        f"CREATE DATABASE `{db}` ENGINE=Replicated('/test/{db}', 'shard1', 'replica1')",
    )
    clickhouse.query(
        f"CREATE TABLE `{db}.{t}` (`s` String) ENGINE = ReplicatedMergeTree('/test/{t}', 'r1') ORDER BY tuple()",
        query_id=ddl_query_id,
    )

    # flush logs
    for node in (keeper1, keeper2, keeper3, clickhouse):
        node.query("SYSTEM FLUSH LOGS system.opentelemetry_span_log")

    # find the DDL query's trace_id
    ddl_trace_id = clickhouse.query(
        f"""
        SELECT trace_id
        FROM system.opentelemetry_span_log
        WHERE 1
            AND operation_name = 'query'
            AND attribute['clickhouse.query_id'] = '{ddl_query_id}'
        """
    ).strip()
    assert ddl_trace_id != ""

    # find all span ids for the zookeeper.<write-operation> spans that pertain to the DDL query
    # each span id is a separate zookeeper query that we ran as part of the DDL query
    zookeeper_write_operations_spans_on_client = clickhouse.query(
        f"""
        SELECT span_id
        FROM system.opentelemetry_span_log
        WHERE 1
            AND trace_id = '{ddl_trace_id}'
            AND operation_name IN ('zookeeper.create', 'zookeeper.set', 'zookeeper.multi')
        FORMAT TSV
        """
    ).strip().split('\n')
    assert zookeeper_write_operations_spans_on_client

    # for each span id, verify that the replica serving it and the leader had the right sequence of spans
    for span_id in zookeeper_write_operations_spans_on_client:
        query = f"""
            SELECT operation_name
            FROM system.opentelemetry_span_log
            WHERE 1
                AND trace_id = '{ddl_trace_id}'
                AND parent_span_id = {span_id}
            ORDER BY start_time_us ASC
            FORMAT TSV
        """

        follower_result = keeper2.query(query).strip().split('\n')
        assert follower_result == [
            "keeper.receive_request",
            "keeper.dispatcher.requests_queue",
            "keeper.write.pre_commit",
            "keeper.write.commit",
            "keeper.dispatcher.responses_queue",
            "keeper.send_response",
        ], follower_result

        leader_result = keeper1.query(query).strip().split('\n')
        assert leader_result == [
            "keeper.write.pre_commit",
            "keeper.write.commit",
        ], leader_result

    # find all span ids for the zookeeper.<read-operation> spans that pertain to the DDL query
    # each span id is a separate zookeeper query that we ran as part of the DDL query
    zookeeper_read_operations_spans_on_client = clickhouse.query(
        f"""
        SELECT span_id
        FROM system.opentelemetry_span_log
        WHERE 1
            AND trace_id = '{ddl_trace_id}'
            AND operation_name IN ('zookeeper.get', 'zookeeper.list', 'zookeeper.exists')
        FORMAT TSV
        """
    ).strip().split('\n')
    assert zookeeper_read_operations_spans_on_client
    
    for span_id in zookeeper_read_operations_spans_on_client:
        query = f"""
            SELECT operation_name
            FROM system.opentelemetry_span_log
            WHERE 1
                AND trace_id = '{ddl_trace_id}'
                AND parent_span_id = {span_id}
            ORDER BY start_time_us ASC
            FORMAT TSV
        """

        # replica (follower) that we have session with
        follower_result = keeper2.query(query).strip().split('\n')
        assert follower_result in (
            # case 1: the read had to wait for the write it depends on to complete
            [
                "keeper.receive_request",
                "keeper.dispatcher.requests_queue",
                "keeper.read.wait_for_write",
                "keeper.read.process",
                "keeper.dispatcher.responses_queue",
                "keeper.send_response",
            ],
            # case 2: the read did not have to wait for any writes
            [
                "keeper.receive_request",
                "keeper.dispatcher.requests_queue",
                "keeper.read.process",
                "keeper.dispatcher.responses_queue",
                "keeper.send_response",
            ],
        ), follower_result

        # other replicas
        assert keeper1.query(query) == ""
        assert keeper3.query(query) == ""


def test_keeper_opentelemetry_tracing_without_server_trace(started_cluster):
    """
    Test that keeper request tracing works independently of server request tracing.
    With opentelemetry_start_trace_probability=0.0 and opentelemetry_start_keeper_trace_probability=1.0,
    traces for ZooKeeper requests should still be created from scratch and propagated to all keeper replicas.
    """

    clickhouse = started_cluster.instances["clickhouse"]
    keeper1 = started_cluster.instances["clickhouseKeeper1"]
    keeper2 = started_cluster.instances["clickhouseKeeper2"]
    keeper3 = started_cluster.instances["clickhouseKeeper3"]

    db = f"test_no_server_trace_{uuid.uuid4()}_database"

    # run DDL with server tracing disabled but keeper tracing enabled per-query
    clickhouse.query(
        f"CREATE DATABASE `{db}` ENGINE=Replicated('/test/{db}', 'shard1', 'replica1')",
        settings={
            "opentelemetry_start_trace_probability": "0.0",
            "opentelemetry_start_keeper_trace_probability": "1.0",
        },
    )

    # find traces pertaining to zookeeper write requests related to our DDL
    # these traces were started from scratch by create_trace_if_not_exists
    clickhouse.query("SYSTEM FLUSH LOGS system.opentelemetry_span_log")
    zookeeper_write_traces = clickhouse.query(
        f"""
        SELECT trace_id
        FROM system.opentelemetry_span_log
        WHERE 1
            AND operation_name IN ('zookeeper.create', 'zookeeper.set', 'zookeeper.multi')
            AND attribute['zk.path'] LIKE '/test/{db}%'
        FORMAT TSV
        """
    ).strip()
    assert zookeeper_write_traces != ""

    # for each trace, verify that the leader has spans for it
    keeper1.query("SYSTEM FLUSH LOGS system.opentelemetry_span_log")
    for trace_id in zookeeper_write_traces.split('\n'):
        assert int(
            keeper1.query(f"SELECT count() FROM system.opentelemetry_span_log WHERE trace_id = '{trace_id}'").strip()
        ) > 0
