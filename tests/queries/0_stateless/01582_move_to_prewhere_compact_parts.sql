DROP TABLE IF EXISTS prewhere_move;
CREATE TABLE prewhere_move (x Int, y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, toString(number) FROM numbers(1000);

EXPLAIN SYNTAX SELECT * FROM prewhere_move WHERE x > 100;

DROP TABLE prewhere_move;
