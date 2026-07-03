SET async_insert = 0;

DROP TABLE IF EXISTS t_ret_with_scope;
CREATE TABLE t_ret_with_scope (id UInt64) ENGINE = Memory;

SELECT 'outer with visible in returning values';
WITH cte AS (SELECT toUInt64(42) AS id)
INSERT INTO t_ret_with_scope
RETURNING (SELECT id FROM cte)
VALUES (1);

SELECT count() FROM t_ret_with_scope;

SELECT 'outer with visible in returning select';
TRUNCATE TABLE t_ret_with_scope;
WITH cte AS (SELECT number + 10 AS id FROM numbers(2))
INSERT INTO t_ret_with_scope
SELECT id FROM cte
RETURNING (SELECT sum(id) FROM cte);

SELECT count() FROM t_ret_with_scope;

SELECT 'duplicate with rejected for returning';
WITH outer_cte AS (SELECT 1 AS x)
INSERT INTO t_ret_with_scope
RETURNING (WITH inner_cte AS (SELECT 2 AS x) SELECT x FROM inner_cte)
VALUES (5); -- { serverError SYNTAX_ERROR }

SELECT count() FROM t_ret_with_scope;

DROP TABLE t_ret_with_scope;
