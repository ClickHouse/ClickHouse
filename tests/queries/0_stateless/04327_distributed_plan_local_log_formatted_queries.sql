-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_04327;
CREATE TABLE t_04327 (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04327 SELECT number, number FROM numbers(1000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject the count below).
SET max_rows_to_group_by = 0;

-- The self-join forces shuffle exchange stages, i.e. local worker tasks that reach logQueryStart.
SELECT count()
FROM t_04327 AS x, t_04327 AS y
WHERE x.id = y.a
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    log_formatted_queries = 1;

DROP TABLE t_04327;
