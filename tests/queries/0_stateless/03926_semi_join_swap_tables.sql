-- Test for swapping the build and probe sides of a SEMI join

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

-- swap LEFT SEMI join to RIGHT SEMI join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    LEFT SEMI JOIN rhs
    ON lhs.a = rhs.a
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

-- no swapping for PARTIAL_MERGE join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    LEFT SEMI JOIN rhs
    ON lhs.a = rhs.a
    SETTINGS join_algorithm='partial_merge'
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

-- no swapping for PREFER_PARTIAL_MERGE join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    LEFT SEMI JOIN rhs
    ON lhs.a = rhs.a
    SETTINGS join_algorithm='prefer_partial_merge'
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

-- no swapping for AUTO join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs 
    LEFT SEMI JOIN rhs
    ON lhs.a = rhs.a
    SETTINGS join_algorithm='auto'
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT *
FROM lhs
LEFT SEMI JOIN rhs
ON lhs.a = rhs.a
FORMAT Null
SETTINGS log_comment = '03926_left_semi_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03926_left_semi_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';

-- swap RIGHT SEMI join to LEFT SEMI join
SELECT *
FROM (
    EXPLAIN actions=1
    SELECT *
    FROM lhs
    RIGHT SEMI JOIN rhs
    ON lhs.a = rhs.a
)
WHERE (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT *
FROM lhs
RIGHT SEMI JOIN rhs
ON lhs.a = rhs.a
FORMAT Null
SETTINGS log_comment = '03926_right_semi_join_swap_tables';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_table_size
FROM system.query_log
WHERE log_comment = '03926_right_semi_join_swap_tables' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE';
