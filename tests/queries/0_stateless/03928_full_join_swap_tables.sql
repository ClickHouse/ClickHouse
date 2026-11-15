-- Test for swapping the build and probe sides of a FULL ALL join

CREATE TABLE lhs(a UInt32)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE rhs(a UInt32)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO lhs VALUES (1), (2), (3), (1000001), (1000002), (1000003);
INSERT INTO rhs SELECT * FROM numbers_mt(1e6);

SET enable_parallel_replicas = 0; -- join swap/reordering disabled with parallel replicas
SET enable_analyzer = 1, query_plan_join_swap_table = 'auto';
SET join_algorithm='hash';

-- swap FULL ALL join sides
SELECT count(*)
FROM lhs
FULL ALL JOIN rhs
ON lhs.a = rhs.a
SETTINGS log_comment = '03928_full_all_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03928_full_all_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';

-- swap FULL ALL join sides with nulls behavior
SELECT count(*)
FROM lhs
FULL ALL JOIN rhs
ON lhs.a = rhs.a
SETTINGS join_use_nulls = 1, log_comment = '03928_full_all_join_use_nulls_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03928_full_all_join_use_nulls_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';
