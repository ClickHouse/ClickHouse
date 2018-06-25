USE test;

DROP TABLE IF EXISTS test;
CREATE TABLE test (d Date DEFAULT '2000-01-01', x UInt64, y UInt64) ENGINE = MergeTree(d, x, 1);
INSERT INTO test(x,y) VALUES (6, 3);
INSERT INTO test(x,y) VALUES (0, 5);
INSERT INTO test(x,y) VALUES (7, 0);
INSERT INTO test(x,y) VALUES (0, 0);
SET enable_conditional_computation=1;
SELECT
	x,
	y,
	x and y,
	y and x,
	x and 1 and x and y,
	x and modulo(y, x),
	y and modulo(x,y)
FROM
	test
ORDER BY
	x, y
;
SET enable_conditional_computation=0;
