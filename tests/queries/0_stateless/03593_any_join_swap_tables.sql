CREATE TABLE lhs(a UInt32)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE rhs(a UInt32)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO lhs VALUES (1), (2), (3);
INSERT INTO rhs SELECT * FROM numbers_mt(1e6);

SET enable_parallel_replicas = 0; -- join swap/reordering disabled with parallel replicas
SET enable_analyzer = 1, query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 10; -- cardinality estimation needed for size-based swap decision
SET query_plan_convert_any_join_to_semi_or_anti_join = 0; -- test is specifically about ANY join swap, prevent conversion changing join type

SELECT *
FROM lhs
ANY JOIN rhs
ON lhs.a = rhs.a
FORMAT Null
SETTINGS log_comment = '03593_any_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03593_any_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';

