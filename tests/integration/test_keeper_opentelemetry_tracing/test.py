#!/usr/bin/env python3
import pytest
import time
import uuid
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

keeper1 = cluster.add_instance(
    "keeper1",
    main_configs=[
        "configs/keeper/one.xml",
        "configs/server/use_keeper.xml",
    ],
)

keeper2 = cluster.add_instance(
    "keeper2",
    main_configs=[
        "configs/keeper/two.xml",
        "configs/server/use_keeper.xml",
    ],
)

keeper3 = cluster.add_instance(
    "keeper3",
    main_configs=[
        "configs/keeper/three.xml",
        "configs/server/use_keeper.xml",
    ],
)

clickhouse = cluster.add_instance(
    "clickhouse",
    main_configs=[
        "configs/server/use_keeper.xml",
        "configs/server/enable_span_log.xml",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
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

    Note: in this test, we're delibirately running keepers in standalone ClickHouse servers, this is needed for the span log to be there.
    """

    db = f"test_keeper_opentelemetry_tracing_{uuid.uuid4()}_database"
    t = f"test_keeper_opentelemetry_tracing_{uuid.uuid4()}_table"

    ddl_query_id = uuid.uuid4()

    # run DDL query
    clickhouse.query(
        f"CREATE DATABASE `{db}` ENGINE=Replicated('/test/{db}', 'shard1', 'replica1')",
    )
    clickhouse.query(
        f"CREATE TABLE `{db}.{t}` (`s` String) ENGINE = MergeTree('/test/{t}', 'replica1') ORDER BY tuple()",
        settings={
            "opentelemetry_start_trace_probability": 1.0,
            "opentelemetry_keeper_spans_probability": 1.0,
        },
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
            AND operation_name LIKE '%DB::DDLWorker::processTask%'
            AND attribute['clickhouse.ddl_entry.initial_query_id'] = '{ddl_query_id}'
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
        FORMAT TabSeparatedRaw
        """
    ).strip().split('\n')
    assert len(zookeeper_write_operations_spans_on_client) > 0

    # for each span id, verify that the replica serving it and the leader had the right sequence of spans
    for span_id in zookeeper_write_operations_spans_on_client:
        query = f"""
            SELECT operation_name
            FROM system.opentelemetry_span_log
            WHERE 1
                AND trace_id = '{ddl_trace_id}'
                AND parent_span_id = '{span_id}'
            ORDER BY start_time_us ASC
            FORMAT TabSeparatedRaw
        """

        # replica (follower) that we have the session with
        assert keeper2.query(query).strip().split('\n') == [
            "keeper.receive_request",
            "keeper.process_request",
            "keeper.write.pre_commit",
            "keeper.write.commit",
            "keeper.dispatcher.responses_queue",
            "keeper.send_response",
        ]

        # leader
        assert keeper1.query(query).strip().split('\n') == [
            "keeper.write.pre_commit",
            "keeper.write.commit",
        ]

    # find all span ids for the zookeeper.<read-operation> spans that pertain to the DDL query
    # each span id is a separate zookeeper query that we ran as part of the DDL query
    zookeeper_read_operations_spans_on_client = clickhouse.query(
        f"""
        SELECT span_id
        FROM system.opentelemetry_span_log
        WHERE 1
            AND trace_id = '{ddl_trace_id}'
            AND operation_name IN ('zookeeper.get', 'zookeeper.list', 'zookeeper.exists')
        FORMAT TabSeparatedRaw
        """
    ).strip().split('\n')
    assert len(zookeeper_read_operations_spans_on_client) > 0
    
    for span_id in zookeeper_write_operations_spans_on_client:
        query = f"""
            SELECT operation_name
            FROM system.opentelemetry_span_log
            WHERE 1
                AND trace_id = '{ddl_trace_id}'
                AND parent_span_id = '{span_id}'
            ORDER BY start_time_us ASC
            FORMAT TabSeparatedRaw
        """

        # replica (follower) that we have session with
        assert keeper2.query(query).strip().split('\n') in (
            # case 1: the read had to wait for the write it depends on to complete
            [
                "keeper.receive_request",
                "keeper.process_request",
                "keeper.read.wait_for_write",
                "keeper.read.process",
                "keeper.dispatcher.responses_queue",
                "keeper.send_response",
            ],
            # case 2: the read did not have to wait for any writes
            [
                "keeper.receive_request",
                "keeper.process_request",
                "keeper.read.process",
                "keeper.dispatcher.responses_queue",
                "keeper.send_response",
            ],
        )

        # other replicas
        assert keeper1.query(query) == ""
        assert keeper3.query(query) == ""
