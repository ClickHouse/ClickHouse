-- Regression test for `SETTINGS <construction setting> = DEFAULT` and UNION scoping.
--
-- A trailing query-level `SETTINGS limit = DEFAULT` / `offset = DEFAULT` on a UNION resets the
-- value inherited from the session for the whole union, exactly like on a plain SELECT.
--
-- The same reset written in a non-last arm's own (arm-local) `SETTINGS` clause is scoped to that
-- arm: an arm never inherits a construction setting (the session value shapes the whole union's
-- result, and the other arms' values are arm-local too), so the reset leaves the arm with no
-- arm-local shaping and the whole-union session `limit` / `offset` still applies.
--
-- The second (empty) UNION branch keeps the query a real union while making the output
-- deterministic. The session settings are set right before each union and reset right after,
-- so that they do not shape the label SELECTs.
SET enable_analyzer = 1;

SELECT 'query-level limit reset';
SET limit = 2;
SELECT number FROM numbers(5) ORDER BY number UNION ALL SELECT 1000 WHERE 0 SETTINGS limit = DEFAULT;
SET limit = 0;

SELECT 'limit kept without reset';
SET limit = 2;
(SELECT number FROM numbers(5) ORDER BY number) UNION ALL (SELECT 1000 WHERE 0);
SET limit = 0;

SELECT 'arm-local limit reset does not affect the union';
SET limit = 2;
(SELECT number FROM numbers(5) ORDER BY number SETTINGS limit = DEFAULT) UNION ALL (SELECT 1000 WHERE 0);
SET limit = 0;

SELECT 'query-level offset reset';
SET offset = 3;
SELECT number FROM numbers(5) ORDER BY number UNION ALL SELECT 1000 WHERE 0 SETTINGS offset = DEFAULT;
SET offset = 0;

SELECT 'offset kept without reset';
SET offset = 3;
(SELECT number FROM numbers(5) ORDER BY number) UNION ALL (SELECT 1000 WHERE 0);
SET offset = 0;

SELECT 'arm-local offset reset does not affect the union';
SET offset = 3;
(SELECT number FROM numbers(5) ORDER BY number SETTINGS offset = DEFAULT) UNION ALL (SELECT 1000 WHERE 0);
SET offset = 0;

SELECT 'query-level reset of both';
SET limit = 2, offset = 3;
SELECT number FROM numbers(5) ORDER BY number UNION ALL SELECT 1000 WHERE 0 SETTINGS limit = DEFAULT, offset = DEFAULT;
SET limit = 0, offset = 0;

SELECT 'arm-local filter reset does not affect the union';
SET filter = 'number > 2';
(SELECT number FROM numbers(5) ORDER BY number SETTINGS filter = DEFAULT) UNION ALL (SELECT 1000 AS number WHERE 0);
SET filter = '';

SELECT 'query-level filter reset';
SET filter = 'number > 2';
SELECT number FROM numbers(5) ORDER BY number UNION ALL SELECT 1000 AS number WHERE 0 SETTINGS filter = DEFAULT;
SET filter = '';

SELECT 'arm-local order reset does not affect the union';
SET order = 'number DESC';
(SELECT number FROM numbers(3) SETTINGS order = DEFAULT) UNION ALL (SELECT 1000 AS number WHERE 0);
SET order = '';
