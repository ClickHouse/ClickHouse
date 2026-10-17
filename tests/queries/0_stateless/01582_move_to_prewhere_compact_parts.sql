-- Tags: no-random-merge-tree-settings

SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;

DROP TABLE IF EXISTS prewhere_move;
CREATE TABLE prewhere_move (x Int, y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, toString(number) FROM numbers(1000);

SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move WHERE x > 100) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter%';

DROP TABLE prewhere_move;

CREATE TABLE prewhere_move (x1 Int, x2 Int, x3 Int, x4 String CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, number, number, repeat('a', 1024) FROM numbers(1000);

-- Not all conditions moved
SET move_all_conditions_to_prewhere = 0;
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_String', '') FROM (EXPLAIN actions=1 SELECT * FROM prewhere_move WHERE x1 > 100 AND x2 > 100 AND x3 > 100 AND x4 > '100') WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter%';

DROP TABLE prewhere_move;
