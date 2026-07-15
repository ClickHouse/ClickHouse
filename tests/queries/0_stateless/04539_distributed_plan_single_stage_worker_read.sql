-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_lookup;
CREATE TABLE t_small (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_lookup (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_small SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_lookup SELECT number, toString(number) FROM numbers(200000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1;

-- Whole small table (fewer rows than the broadcast threshold): no exchange, single stage.
SELECT v FROM t_small WHERE k = 3;

-- Point lookups into a larger table, still selecting few rows: single stage.
SELECT v FROM t_lookup WHERE k = 42;
SELECT k, v FROM t_lookup WHERE k = 100000;

-- Outer read is a tiny point lookup; the WHERE is a single-stage scalar subquery over another
-- table, mirroring the reported scalar-subquery-in-WHERE shape.
SELECT v FROM t_lookup WHERE k = (SELECT k FROM t_small WHERE k = 7);

DROP TABLE t_small;
DROP TABLE t_lookup;
