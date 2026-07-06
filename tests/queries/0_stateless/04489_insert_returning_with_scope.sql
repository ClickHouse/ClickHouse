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

SELECT 'outer with visible in nested returning set-op';
TRUNCATE TABLE t_ret_with_scope;
WITH cte AS (SELECT toUInt64(42) AS id)
INSERT INTO t_ret_with_scope
RETURNING (((SELECT id FROM cte) UNION ALL SELECT id FROM cte) UNION ALL SELECT id FROM cte)
VALUES (1);

SELECT count() FROM t_ret_with_scope;

DROP TABLE t_ret_with_scope;
