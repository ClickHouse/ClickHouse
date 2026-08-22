-- Test for swapping the build and probe sides of a ANTI join

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
SET join_algorithm='hash';

-- swap LEFT ANTI join to RIGHT ANTI join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    LEFT ANTI JOIN rhs
    ON lhs.a = rhs.a
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT *
FROM lhs
LEFT ANTI JOIN rhs
ON lhs.a = rhs.a
FORMAT Null
SETTINGS log_comment = '03927_left_anti_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03927_left_anti_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';

-- swap RIGHT ANTI join to LEFT ANTI join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    RIGHT ANTI JOIN rhs
    ON lhs.a = rhs.a
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT *
FROM lhs
RIGHT ANTI JOIN rhs
ON lhs.a = rhs.a
FORMAT Null
SETTINGS log_comment = '03927_right_anti_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03927_right_anti_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';
