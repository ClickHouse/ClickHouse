-- `force_materialized_cte` (default 1) throws when a CTE declared `AS MATERIALIZED` would be silently inlined.

SET enable_analyzer = 1;
SET enable_materialized_cte = 0;
-- With `force_materialized_cte = 0` the analyzer warns that `MATERIALIZED` is ignored; keep it out of stderr.
SET send_logs_level = 'fatal';

SELECT 'analyzer, materialization disabled: throws';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
-- An unreferenced materialized CTE throws too.
WITH c AS MATERIALIZED (SELECT 1) SELECT 2; -- { serverError SUPPORT_IS_DISABLED }
-- Nested in a subquery.
SELECT * FROM (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b); -- { serverError SUPPORT_IS_DISABLED }

SELECT 'analyzer, force disabled: inlined';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS force_materialized_cte = 0;
WITH c AS MATERIALIZED (SELECT 1) SELECT 2 SETTINGS force_materialized_cte = 0;

SELECT 'analyzer, materialization enabled: works';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1;
-- Enabling it for a subquery only is enough for the CTEs of that subquery.
SELECT * FROM (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1);

SELECT 'compatibility restores the old behaviour';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS compatibility = '26.8';

SELECT 'view definitions: rejected even with materialization enabled';
-- A stored view definition inlines its CTEs, so the `MATERIALIZED` contract would be lost.
SET enable_materialized_cte = 1;
SET force_materialized_cte = 1;
CREATE VIEW v_05141 AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
-- A materialized view must read from a table, not a table function.
CREATE TABLE src_05141 (x UInt64) ENGINE = Memory;
INSERT INTO src_05141 SELECT number FROM numbers(3);
CREATE TABLE dst_05141 (n UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_05141 TO dst_05141 AS WITH c AS MATERIALIZED (SELECT x FROM src_05141) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
CREATE MATERIALIZED VIEW mv_05141 TO dst_05141 AS SELECT count() AS n FROM src_05141;
ALTER TABLE mv_05141 MODIFY QUERY WITH c AS MATERIALIZED (SELECT x FROM src_05141) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'view definitions: force disabled inlines';
SET force_materialized_cte = 0;
CREATE VIEW v_05141 AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b;
SELECT * FROM v_05141;
ALTER TABLE mv_05141 MODIFY QUERY WITH c AS MATERIALIZED (SELECT x FROM src_05141) SELECT count() AS n FROM c AS a, c AS b;
DROP TABLE mv_05141;
DROP TABLE dst_05141;
DROP TABLE src_05141;

SELECT 'view read: rejected when materialization is disabled';
-- The view was created with the guard off; reading it with the guard on and materialization off throws.
SET force_materialized_cte = 1;
SET enable_materialized_cte = 0;
SELECT * FROM v_05141; -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM v_05141 SETTINGS force_materialized_cte = 0;
DROP VIEW v_05141;

SELECT 'lightweight UPDATE: rejected even with materialization enabled';
SET enable_materialized_cte = 1;
SET force_materialized_cte = 1;
SET enable_lightweight_update = 1;
CREATE TABLE t_05141 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_05141 SELECT number, 0 FROM numbers(5);
UPDATE t_05141 SET v = 1 WHERE id IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b); -- { serverError SUPPORT_IS_DISABLED }
SELECT sum(v) FROM t_05141;
UPDATE t_05141 SET v = (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b) WHERE id = 4; -- { serverError SUPPORT_IS_DISABLED }
SELECT sum(v) FROM t_05141;

SELECT 'lightweight UPDATE: force disabled inlines';
SET force_materialized_cte = 0;
UPDATE t_05141 SET v = 1 WHERE id IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT sum(v) FROM t_05141;
UPDATE t_05141 SET v = (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b) WHERE id = 4;
SELECT sum(v) FROM t_05141;
DROP TABLE t_05141;

SELECT 'old analyzer: throws, force disabled inlines';
SET force_materialized_cte = 1;
SET enable_materialized_cte = 0;
SET enable_analyzer = 0;
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1; -- { serverError SUPPORT_IS_DISABLED }
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS force_materialized_cte = 0;
