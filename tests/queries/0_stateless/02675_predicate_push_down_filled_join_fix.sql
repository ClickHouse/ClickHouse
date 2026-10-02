SET enable_analyzer = 1;
SET single_join_prefer_left_table = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

DROP TABLE IF EXISTS test_table_join;
CREATE TABLE test_table_join
(
    id UInt64,
    value String
) ENGINE = Join(All, inner, id);

INSERT INTO test_table_join VALUES (0, 'JoinValue');

EXPLAIN header = 1, actions = 1 SELECT t1.id, t1.value, t2.value FROM test_table AS t1 INNER JOIN test_table_join AS t2 ON t1.id = t2.id WHERE t1.id = 0;

SELECT t1.id, t1.value, t2.value FROM test_table AS t1 INNER JOIN test_table_join AS t2 ON t1.id = t2.id WHERE t1.id = 0;

DROP TABLE test_table_join;
DROP TABLE test_table;
