-- Tags: no-fasttest, no-old-analyzer, distributed
-- no-old-analyzer: make_distributed_plan requires the analyzer.
--
-- A distributed plan query (make_distributed_plan = 1) dispatches its tasks to stateless workers
-- over HTTP. The trace context travels with the dispatch request as W3C `traceparent` /
-- `tracestate` headers, so the worker's task span joins the initiator's trace:
--
--   query (initiator)
--   └── StatelessWorkerClient::sendTask [CLIENT]
--       └── InterserverIOHTTPHandler [SERVER]
--           └── (worker thread pool span)
--               └── DistributedPlanTask::execute [SERVER]
--
-- The harness worker cluster (test_cluster_one_shard_two_replicas) is this same server, so every
-- span lands in the local span log. The traces are started by sampling (a .sql test has no client
-- `traceparent`) and found through the `log_comment` of their initiator query. The number of tasks
-- is an implementation detail of the planner, so the checks print distinct span descriptions, not
-- counts: a missing or malformed span shows up as a missing or changed line.

DROP TABLE IF EXISTS t_dp_otel;
CREATE TABLE t_dp_otel (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_otel SELECT number % 10 FROM numbers(10000);

SET opentelemetry_start_trace_probability = 1;
-- The test profile sets a global max_rows_to_group_by; distributed aggregation cannot enforce it
-- once the aggregation is split per bucket and refuses the plan, so pin it to 0.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0;

SET log_comment = 'otel worker spans: stateless workers', distributed_plan_execute_locally = 0;
SELECT x, count() FROM t_dp_otel GROUP BY x FORMAT Null;

-- Nothing is dispatched when the tasks run in-process: they run on the initiator's threads and their
-- spans attach to the initiator's trace directly, with no span of the local executor in between.
SET log_comment = 'otel worker spans: local execution', distributed_plan_execute_locally = 1;
SELECT x, count() FROM t_dp_otel GROUP BY x FORMAT Null;

-- A traced request that fails on the worker before any task starts leaves the request span as its
-- only worker-side trace, so the span must carry the failure. `url` forwards the trace context of
-- its query the same way the dispatch does.
SET log_comment = 'otel worker spans: failed request', make_distributed_plan = 0, http_max_tries = 1, http_make_head_request = 0;
SELECT * FROM url('http://localhost:' || toString(getServerPort('interserver_http_port')) || '/?endpoint=no_such_endpoint&compress=false', RawBLOB, 'd String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }

SET opentelemetry_start_trace_probability = 0, log_comment = '';
SYSTEM FLUSH LOGS query_log, opentelemetry_span_log;

-- The spans of the three traces, labelled by the log_comment of their initiator query. The span log
-- is large on a busy server, so it is never on the build side of a join: the few trace ids are
-- collected first and the spans are then read with an IN filter, whatever the join settings are.
CREATE TEMPORARY TABLE traced_queries ENGINE = Memory AS
SELECT DISTINCT query_id, replaceOne(log_comment, 'otel worker spans: ', '') AS label
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE 'otel worker spans: %';

CREATE TEMPORARY TABLE traces ENGINE = Memory AS
SELECT trace_id, label, query_id AS initiator_query_id
FROM
(
    SELECT trace_id, attribute['clickhouse.query_id'] AS query_id
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday() AND operation_name = 'query'
        AND attribute['clickhouse.query_id'] IN (SELECT query_id FROM traced_queries)
) AS query_spans
JOIN traced_queries USING (query_id);

CREATE TEMPORARY TABLE spans ENGINE = Memory AS
SELECT
    label, span_id, parent_span_id, operation_name, kind, attribute,
    operation_name = 'query' AND attribute['clickhouse.query_id'] = initiator_query_id AS is_initiator,
    attribute['clickhouse.initial_query_id'] = initiator_query_id AS has_initiator_query_id
FROM
(
    SELECT trace_id, span_id, parent_span_id, operation_name, kind, attribute
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday() AND trace_id IN (SELECT trace_id FROM traces)
) AS trace_spans
JOIN traces USING (trace_id);

SELECT 'traces found:', arraySort(groupUniqArray(label)) FROM spans;

SELECT 'dispatch spans:';
SELECT DISTINCT
    label, operation_name, kind,
    'task_id: ' || if(attribute['clickhouse.distributed.task_id'] != '', 'set', 'MISSING'),
    'initial_query_id: ' || if(has_initiator_query_id, 'initiator', attribute['clickhouse.initial_query_id']),
    'target_host: ' || if(attribute['clickhouse.target_host'] != '', 'set', 'MISSING'),
    'exception: ' || if(mapContains(attribute, 'clickhouse.exception'), attribute['clickhouse.exception'], 'none')
FROM spans
WHERE operation_name = 'StatelessWorkerClient::sendTask'
ORDER BY ALL;

-- The parent of a dispatch request is the dispatch span; the failed request is a child of some span
-- of the initiator query (the thread that reads the URL), which does not matter here.
SELECT 'request spans:';
SELECT DISTINCT
    s.label, s.operation_name, s.kind,
    'parent: ' || multiIf(p.operation_name LIKE 'StatelessWorker%', p.operation_name, p.span_id != 0, 'a span of the initiator query', 'MISSING'),
    'endpoint: ' || extract(decodeURLComponent(s.attribute['clickhouse.uri']), 'endpoint=([^&/]*)'),
    'http.method: ' || s.attribute['http.method'],
    'http_status: ' || s.attribute['clickhouse.http_status'],
    'exception_code: ' || if(mapContains(s.attribute, 'clickhouse.exception_code'), s.attribute['clickhouse.exception_code'], 'none'),
    -- The message without the "Code: N. DB::Exception: " prefix: the code is printed above, and the
    -- harness treats an exception text in the output as a failure of the test.
    'exception: ' || if(mapContains(s.attribute, 'clickhouse.exception'), extract(s.attribute['clickhouse.exception'], 'DB::Exception: (.*)$'), 'none')
FROM spans AS s
LEFT JOIN spans AS p ON p.span_id = s.parent_span_id
WHERE s.operation_name = 'InterserverIOHTTPHandler'
ORDER BY ALL;

SELECT 'task spans:';
SELECT DISTINCT
    label, operation_name, kind,
    'task_id: ' || if(attribute['clickhouse.distributed.task_id'] != '', 'set', 'MISSING'),
    'initial_query_id: ' || if(has_initiator_query_id, 'initiator', attribute['clickhouse.initial_query_id']),
    'plan_hash: ' || if(attribute['clickhouse.distributed.plan_hash'] != '', 'set', 'MISSING'),
    'execute_locally: ' || attribute['clickhouse.distributed.execute_locally'],
    'exchange.outputs: ' || if(attribute['clickhouse.exchange.outputs'] != '', 'set', 'MISSING'),
    'query_id: ' || if(attribute['clickhouse.query_id'] != '', 'set', 'MISSING'),
    'query_status: ' || attribute['clickhouse.query_status'],
    'exception: ' || if(mapContains(attribute, 'clickhouse.exception'), attribute['clickhouse.exception'], 'none')
FROM spans
WHERE operation_name = 'DistributedPlanTask::execute'
ORDER BY ALL;

-- Every task span must reach the initiator's `query` span through its parents. Printed are the
-- distributed-plan spans met on the way: through the dispatch request for a worker, and nothing at
-- all for in-process execution. The depth of the chain depends on the thread pools in between.
SELECT 'ancestors of the task spans:';
WITH RECURSIVE chain AS
(
    SELECT label, span_id AS task_span_id, parent_span_id AS next_span_id, [operation_name] AS path, 0 AS depth
    FROM spans
    WHERE operation_name = 'DistributedPlanTask::execute'
    UNION ALL
    SELECT
        c.label, c.task_span_id,
        if(s.is_initiator, 0, s.parent_span_id),
        if(s.is_initiator OR s.operation_name LIKE 'Distributed%' OR s.operation_name LIKE 'StatelessWorker%' OR s.operation_name = 'InterserverIOHTTPHandler',
            arrayPushBack(c.path, s.operation_name), c.path),
        c.depth + 1
    FROM chain AS c
    JOIN spans AS s ON s.span_id = c.next_span_id
    WHERE c.next_span_id != 0 AND c.depth < 32
)
SELECT DISTINCT label, arrayStringConcat(path, ' <- ') AS ancestors
FROM chain
WHERE next_span_id = 0 OR next_span_id NOT IN (SELECT span_id FROM spans)
ORDER BY ALL;

SELECT 'spans dispatched by the local execution:', countIf(operation_name = 'StatelessWorkerClient::sendTask'), countIf(operation_name = 'InterserverIOHTTPHandler')
FROM spans WHERE label = 'local execution';

DROP TABLE t_dp_otel;
