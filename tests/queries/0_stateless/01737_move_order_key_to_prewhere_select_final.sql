SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;

DROP TABLE IF EXISTS prewhere_move_select_final;

CREATE TABLE prewhere_move_select_final (x Int, y Int, z Int) ENGINE = ReplacingMergeTree() ORDER BY (x, y);
INSERT INTO prewhere_move_select_final SELECT number, number * 2, number * 3 FROM numbers(1000);

select 'optimize_move_to_prewhere_if_final = 1';
SET optimize_move_to_prewhere_if_final = 1;

-- order key can be pushed down with final
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final WHERE y > 100;
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100;

-- can not be pushed down
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE z > 400;

-- only y can be pushed down
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100 and z > 400;

select '';
select 'optimize_move_to_prewhere_if_final = 0';
SET optimize_move_to_prewhere_if_final = 0;

select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final WHERE y > 100;
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100;
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE z > 400;
select '';
EXPLAIN SYNTAX SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100 and z > 400;

DROP TABLE prewhere_move_select_final;
