-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_read_nothing;
DROP TABLE IF EXISTS t_read_nothing_full;
DROP TABLE IF EXISTS t_read_nothing_types;

-- Left empty on purpose: an empty MergeTree table is planned as a `ReadNothing` source.
CREATE TABLE t_read_nothing (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_read_nothing_full (x UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(200000);
CREATE TABLE t_read_nothing_types
(
    lc LowCardinality(Nullable(String)),
    arr Array(UInt64),
    m Map(String, Nullable(Int32)),
    t Tuple(a UInt8, b String),
    n Nullable(Float64)
) ENGINE = MergeTree ORDER BY tuple();

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
-- Distributed aggregation cannot enforce a global max_rows_to_group_by, and the functional-test
-- profile sets it nonzero, so pin it off. Trivial-count would fold the aggregation away.
SET make_distributed_plan = 1, enable_parallel_replicas = 0,
    distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    max_rows_to_group_by = 0, optimize_trivial_count_query = 0;

-- All of the following previously failed with
-- SUPPORT_IS_DISABLED: step 'ReadNothing' is not serializable for remote execution.
SELECT 'aggregation over an empty table';
SELECT sum(x) FROM t_read_nothing;
SELECT count() FROM t_read_nothing;
SELECT 'group by over an empty table';
SELECT x % 8, sum(x) FROM t_read_nothing GROUP BY 1 ORDER BY 1;

-- The serializability check only runs once a plan splits into more than one stage, so a query that
-- stays single-stage would pass without ever reaching `ReadNothingStep::deserialize`. Assert that
-- these plans really do distribute. The row is absent unless an exchange was planted, so removing
-- `make_distributed_plan` above makes the test fail instead of silently covering nothing. The
-- oracle must not aggregate: an aggregating query over `EXPLAIN` is itself distributed.
SELECT 'aggregation over an empty table distributes'
FROM (EXPLAIN PIPELINE SELECT sum(x) FROM t_read_nothing)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;
SELECT 'group by over an empty table distributes'
FROM (EXPLAIN PIPELINE SELECT x % 8, sum(x) FROM t_read_nothing GROUP BY 1 ORDER BY 1)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

SELECT 'empty side unioned with a populated table';
SELECT sum(x) FROM (SELECT x FROM t_read_nothing UNION ALL SELECT x FROM t_read_nothing_full)
GROUP BY x % 4 ORDER BY 1;

SELECT 'join with an empty side';
SELECT count() FROM t_read_nothing_full a INNER JOIN t_read_nothing b ON a.x = b.x;

-- The output header is the step's whole state and travels through the generic per-node preamble,
-- so exercise wrapped and compound types explicitly.
SELECT 'wrapped and compound header types';
SELECT count(), min(lc), max(arr), min(m), max(t), sum(n) FROM t_read_nothing_types;
SELECT lc, groupArray(arr) FROM t_read_nothing_types GROUP BY lc ORDER BY lc;

-- Only column names and types travel in the serialized header, so a projection above the source
-- does not change what `ReadNothing` carries. Kept as a plain shape check.
SELECT 'projection above an empty source';
SELECT c, sum(x) FROM (SELECT 42 AS c, x FROM t_read_nothing) GROUP BY c ORDER BY c;

DROP TABLE t_read_nothing;
DROP TABLE t_read_nothing_full;
DROP TABLE t_read_nothing_types;
