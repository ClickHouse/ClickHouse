DROP TABLE IF EXISTS prewhere_move;
CREATE TABLE prewhere_move (x Int, y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, toString(number) FROM numbers(1000);

EXPLAIN SYNTAX SELECT * FROM prewhere_move WHERE x > 100;

DROP TABLE prewhere_move;

CREATE TABLE prewhere_move (x1 Int, x2 Int, x3 Int, x4 Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, number, number, number FROM numbers(1000);

-- Not all conditions moved
EXPLAIN SYNTAX SELECT * FROM prewhere_move WHERE x1 > 100 AND x2 > 100 AND x3 > 100 AND x4 > 100;

DROP TABLE prewhere_move;
