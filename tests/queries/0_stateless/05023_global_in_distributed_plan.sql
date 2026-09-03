-- Tags: no-old-analyzer

SET enable_parallel_replicas = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;

-- Under `make_distributed_plan` no classic remote read exists, so the `GLOBAL IN` external table is
-- never attached: the set stays a plain subquery set that is built once on the initiator, and its
-- values are shipped to the workers with the tasks, which matches the `GLOBAL IN` semantics. The
-- distributed-plan set validation must keep accepting it: it rejects only sets already backed by an
-- external table.
DROP TABLE IF EXISTS t_global_in_src;
DROP TABLE IF EXISTS t_global_in_dst;
CREATE TABLE t_global_in_src (a UInt64) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO t_global_in_src SELECT number % 16 FROM numbers(100000);
CREATE TABLE t_global_in_dst (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_global_in_dst SELECT number FROM numbers(100);

SELECT count() FROM t_global_in_dst WHERE k GLOBAL IN (SELECT a FROM t_global_in_src) SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_global_in_dst WHERE k GLOBAL IN (SELECT a FROM t_global_in_src) SETTINGS make_distributed_plan = 1;

DROP TABLE t_global_in_src;
DROP TABLE t_global_in_dst;
