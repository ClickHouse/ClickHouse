-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_system_one_full;

CREATE TABLE t_system_one_full (x UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(200000);

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
-- Distributed aggregation cannot enforce a global max_rows_to_group_by, and the functional-test
-- profile sets it nonzero, so pin it off. Trivial-count would fold the aggregation away.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    max_rows_to_group_by = 0, optimize_trivial_count_query = 0;

-- All of the following previously failed with
-- SUPPORT_IS_DISABLED: step 'ReadFromSystemOne' is not serializable for remote execution.
SELECT 'aggregation over system.one';
SELECT count() FROM system.one GROUP BY dummy;
-- Not count(), so a trivial-count rewrite cannot be what makes this pass.
SELECT sum(dummy) FROM system.one GROUP BY dummy;
SELECT 'constant select';
SELECT count() FROM (SELECT 1 AS x) GROUP BY x;

-- The serializability check only runs once a plan splits into more than one stage, so a query that
-- stays single-stage would pass without ever serializing the step. Assert that these plans really do
-- distribute: the row is absent unless an exchange was planted, so removing `make_distributed_plan`
-- above makes the test fail instead of silently covering nothing. The oracle must not aggregate --
-- an aggregating query over EXPLAIN is itself distributed and would fail on its own source step.
SELECT 'aggregation over system.one distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM system.one GROUP BY dummy)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;
SELECT 'constant select distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM (SELECT 1 AS x) GROUP BY x)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

-- The step beside a real reader, i.e. the shape join-order estimation feeds on.
SELECT 'unioned with a populated table';
SELECT sum(x), count() FROM (SELECT dummy AS x FROM system.one UNION ALL SELECT x FROM t_system_one_full)
GROUP BY x % 4 ORDER BY 1;
SELECT 'joined with a populated table';
SELECT count() FROM t_system_one_full a INNER JOIN system.one b ON a.x = b.dummy;

-- The output header is the step's whole state and the decoder rebuilds a hardcoded single UInt8
-- 'dummy' chunk, so pin that the virtual columns are materialized above the step and do not widen
-- the header it carries. Assert the values, not merely that nothing threw.
SELECT 'virtual columns through the step';
SELECT count(), min(_table), min(_database) FROM system.one GROUP BY dummy;

DROP TABLE t_system_one_full;
