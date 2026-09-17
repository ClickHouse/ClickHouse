-- `SETTINGS name` without a value stands for `name = true`, so it only makes sense for a Bool
-- setting, and the check happens where the settings schema is known. The analyzer peels `limit` and
-- `offset` out of a query's `SETTINGS` clause and turns them into expression nodes, reading them as
-- numbers directly, so that path has to reject the shorthand against the schema first. For a nested
-- subquery it is the only place the inner `SETTINGS` clause is seen at all.
--
-- The old analyzer applies `limit`/`offset` from a subquery's `SETTINGS` differently, so pin the
-- analyzer whose peel path this test covers.
SET enable_analyzer = 1;

SELECT '-- The shorthand is rejected for the `limit` setting of a top-level query';
SELECT 1 SETTINGS limit; -- { error TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- And for `offset`';
SELECT 1 SETTINGS offset; -- { error TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- The same inside a subquery, where nothing has checked the clause before the analyzer';
SELECT * FROM (SELECT 1 SETTINGS limit); -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- And with `offset` inside a subquery';
SELECT * FROM (SELECT 1 SETTINGS offset); -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- A shorthand for some other non-Bool setting in a subquery is rejected as well';
SELECT * FROM (SELECT 1 SETTINGS max_threads); -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- With a value, `limit` and `offset` still work in both positions';
SELECT number FROM numbers(10) SETTINGS limit = 3, offset = 1;
SELECT * FROM (SELECT number FROM numbers(10) SETTINGS limit = 3, offset = 1);
SELECT 'ok';

SELECT '-- A Bool setting written as a shorthand in a subquery is accepted';
SELECT * FROM (SELECT 1 SETTINGS optimize_on_insert);
SELECT 'ok';
