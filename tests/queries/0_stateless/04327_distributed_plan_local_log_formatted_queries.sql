-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A local distributed-plan task fragment is built from a serialized query plan, not parsed, so it
-- has no query AST. logQueryStart was given an empty ASTSelectQuery stub purely to carry query_kind;
-- with log_formatted_queries=1 it tried to format that stub, and ASTSelectQuery::formatImpl
-- dereferenced the absent SELECT expression list (null IAST member call).

DROP TABLE IF EXISTS t_04327;
CREATE TABLE t_04327 (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04327 SELECT number, number FROM numbers(1000);

SET max_rows_to_group_by = 0;

-- The self-join forces shuffle exchange stages, i.e. local worker tasks that reach logQueryStart.
SELECT count()
FROM t_04327 AS x, t_04327 AS y
WHERE x.id = y.a
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    query_plan_use_new_logical_join_step = 1, log_formatted_queries = 1;

DROP TABLE t_04327;
