-- Regression test: `SETTINGS limit = DEFAULT` / `offset = DEFAULT` on a SELECT inside a UNION
-- must reset the limit/offset inherited from the session, not keep injecting it into the query tree.
-- The second (empty) UNION branch keeps the query a real union while making the output deterministic.
-- The session `limit`/`offset` are set right before each union and reset right after,
-- so that they do not truncate the label SELECTs.
SET enable_analyzer = 1;

SELECT 'limit reset';
SET limit = 2;
(SELECT number FROM numbers(5) ORDER BY number SETTINGS limit = DEFAULT) UNION ALL (SELECT 1000 WHERE 0);
SET limit = 0;

SELECT 'limit kept without reset';
SET limit = 2;
(SELECT number FROM numbers(5) ORDER BY number) UNION ALL (SELECT 1000 WHERE 0);
SET limit = 0;

SELECT 'offset reset';
SET offset = 3;
(SELECT number FROM numbers(5) ORDER BY number SETTINGS offset = DEFAULT) UNION ALL (SELECT 1000 WHERE 0);
SET offset = 0;

SELECT 'offset kept without reset';
SET offset = 3;
(SELECT number FROM numbers(5) ORDER BY number) UNION ALL (SELECT 1000 WHERE 0);
SET offset = 0;

SELECT 'both reset';
SET limit = 2, offset = 3;
(SELECT number FROM numbers(5) ORDER BY number SETTINGS limit = DEFAULT, offset = DEFAULT) UNION ALL (SELECT 1000 WHERE 0);
SET limit = 0, offset = 0;
