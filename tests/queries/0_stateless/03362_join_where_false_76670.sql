DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0(x Int) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t1(x Int) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t0 SELECT number FROM numbers(10);
INSERT INTO t1 SELECT number + 2 FROM numbers(10);

SET enable_analyzer = 1;

SELECT * FROM t1
RIGHT JOIN t0 AS t2
ON NOT t0.x = t2.x
WHERE false
;

SELECT * FROM t1
RIGHT JOIN t0 AS t2
ON NOT t0.x = t2.x
WHERE identity(false)
; -- { serverError INVALID_JOIN_ON_EXPRESSION }
