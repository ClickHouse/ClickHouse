SET allow_dynamic_type_in_join_keys=0;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
CREATE TABLE t0 (c0 Dynamic, c1 Array(Dynamic), c2 Tuple(d Dynamic)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c0 Int32, c1 Array(Int32), c2 Tuple(d Int32)) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t0 VALUES (1, [1, 2, 3], tuple(1));
INSERT INTO TABLE t1 VALUES (1, [1, 2, 3], tuple(1));

SET enable_analyzer = 0;

SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c1 = t1.c1; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c2 = t1.c2; -- {serverError ILLEGAL_COLUMN}

SET enable_analyzer = 1;
SET query_plan_use_new_logical_join_step = 0;

SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c1 = t1.c1; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c2 = t1.c2; -- {serverError ILLEGAL_COLUMN}

SET query_plan_use_new_logical_join_step = 1;

SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c1 = t1.c1; -- {serverError ILLEGAL_COLUMN}
SELECT * FROM t0 JOIN t1 ON t0.c2 = t1.c2; -- {serverError ILLEGAL_COLUMN}

DROP TABLE t0;
DROP TABLE t1;

