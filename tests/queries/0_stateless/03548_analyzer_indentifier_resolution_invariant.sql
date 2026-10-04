SET allow_experimental_analyzer = 1;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;

-- `a0` in the right argument of `IN` is an expression alias, not a table name,
-- so the view is created and behaves exactly like the same plain `SELECT`.
CREATE VIEW v0 AS (SELECT 1 AS a0, (1) IN a0 FROM t0 tx JOIN t0 ty ON 1 CROSS JOIN t0 tz);

INSERT INTO t0 VALUES (1);
SELECT * FROM v0;
SELECT 1 AS a0, (1) IN a0;
