SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE test_table_2
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO test_table_1 SELECT number, number FROM numbers(10);
INSERT INTO test_table_2 SELECT number, number FROM numbers(10);

-- { echoOn }

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right';
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6
SETTINGS query_plan_join_inner_table_selection = 'right'
;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

-- { echoOff }

DROP TABLE test_table_1;
DROP TABLE test_table_2;
