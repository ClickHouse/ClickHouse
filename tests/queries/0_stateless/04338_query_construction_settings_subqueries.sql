-- Tests that the query-construction settings (`limit` / `offset` / `page`) are applied correctly
-- when they appear on a subquery's own `SETTINGS` clause, on individual `UNION` arms, or on an
-- `INSERT ... SELECT`. These are materialized by wrapping the (sub)query as a derived table in
-- `executeQuery` rather than folded into the query tree by the analyzer.

SELECT '-- page in a subquery SETTINGS is translated to offset (page 2, limit 10 -> rows 10..19)';
SELECT min(number), max(number), count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 10, page = 2);

SELECT '-- page in a subquery SETTINGS without limit is rejected (not silently dropped)';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS page = 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- limit/offset in a subquery SETTINGS cap the subquery result';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5);
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5, offset = 3);

SELECT '-- per-UNION-arm limit is applied per arm, not collapsed onto the whole union (1 + 2 = 3)';
SELECT count() FROM ((SELECT number FROM numbers(100) SETTINGS limit = 1) UNION ALL (SELECT number FROM numbers(100) SETTINGS limit = 2));

SELECT '-- a trailing query-level SETTINGS limit still caps the whole union (3, not 5 + 3)';
SELECT count() FROM (SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) SETTINGS limit = 3);

SELECT '-- INSERT ... SETTINGS limit ... SELECT caps the inserted rows';
DROP TABLE IF EXISTS t_construction_settings;
CREATE TABLE t_construction_settings (x UInt64) ENGINE = Memory;
INSERT INTO t_construction_settings SETTINGS limit = 2 SELECT number FROM numbers(10);
SELECT count() FROM t_construction_settings;
DROP TABLE t_construction_settings;
