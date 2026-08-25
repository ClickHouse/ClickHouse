-- `SETTINGS name` without a value stands for `name = true`, so it only makes sense for a Bool
-- setting, and the check happens where the settings schema is known. The query-construction settings
-- (`select` / `filter` / `order` / `sort` / `limit` / `offset` / `page`) are read straight out of a
-- `SETTINGS` clause and materialized by wrapping the query as a derived table, without ever reaching
-- the settings schema, so that reader has to reject the shorthand itself. None of them is Bool.

SELECT '-- The own clause of a subquery is seen only by the construction-settings reader';
SELECT * FROM (SELECT 1 SETTINGS limit); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS offset); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS page); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS select); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS filter); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS order); -- { serverError TYPE_MISMATCH }
SELECT * FROM (SELECT 1 SETTINGS sort); -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- So is the clause of a non-last UNION arm, which is consumed per-arm';
SELECT * FROM ((SELECT 1 SETTINGS limit) UNION ALL (SELECT 2)); -- { serverError TYPE_MISMATCH }
SELECT * FROM ((SELECT 1 SETTINGS filter) UNION ALL (SELECT 2)); -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- `page` is rejected on a top-level query too, by the settings schema';
SELECT 1 SETTINGS page; -- { error TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- With a value the same settings still work in every position';
SELECT number FROM numbers(10) SETTINGS limit = 2, page = 2;
SELECT * FROM (SELECT number FROM numbers(10) SETTINGS filter = 'number > 7');
SELECT * FROM ((SELECT number FROM numbers(10) SETTINGS limit = 2) UNION ALL (SELECT 100)) ORDER BY 1;
SELECT 'ok';
