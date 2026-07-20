SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;

DROP TABLE IF EXISTS prewhere_move_select_final;

CREATE TABLE prewhere_move_select_final (x Int, y Int, z Int) ENGINE = ReplacingMergeTree() ORDER BY (x, y);
INSERT INTO prewhere_move_select_final SELECT number, number * 2, number * 3 FROM numbers(1000);

select 'optimize_move_to_prewhere_if_final = 1';
SET optimize_move_to_prewhere_if_final = 1;

-- order key can be pushed down with final
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final WHERE x > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE x > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final WHERE y > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final WHERE x + y > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE x + y > 100) WHERE explain LIKE '%Prewhere%';

-- can not be pushed down
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE z > 400) WHERE explain LIKE '%Prewhere filter';

-- only condition with x/y can be pushed down
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100 and z > 400) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE x > 50 and z > 400) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE x + y > 50 and z > 400) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter%';

select 'optimize_move_to_prewhere_if_final = 0';
SET optimize_move_to_prewhere_if_final = 0;

SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final WHERE y > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE z > 400) WHERE explain LIKE '%Prewhere%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move_select_final FINAL WHERE y > 100 and z > 400) WHERE explain LIKE '%Prewhere%';

DROP TABLE prewhere_move_select_final;
