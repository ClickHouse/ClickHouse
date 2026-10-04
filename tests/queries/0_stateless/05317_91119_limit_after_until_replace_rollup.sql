-- `SELECT * REPLACE` mappings must reach `LIMIT AFTER` / `LIMIT UNTIL` boundaries
-- also under `group_by_use_nulls = 1`, where the projection is resolved after the other clauses.
-- The boundary must bind to the replaced value `a * 10`, not to the raw grouped key. The only expected
-- difference between the modes is the ROLLUP total row: `0` with `group_by_use_nulls = 0`, `NULL` (sorted last) with `= 1`.

SET enable_analyzer = 1;

SELECT 'UNTIL';
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10))
GROUP BY a WITH ROLLUP ORDER BY a LIMIT UNTIL a >= 30
SETTINGS group_by_use_nulls = 0;
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10))
GROUP BY a WITH ROLLUP ORDER BY a LIMIT UNTIL a >= 30
SETTINGS group_by_use_nulls = 1;

SELECT 'AFTER';
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10))
GROUP BY a WITH ROLLUP ORDER BY a LIMIT 3 AFTER a > 50
SETTINGS group_by_use_nulls = 0;
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10))
GROUP BY a WITH ROLLUP ORDER BY a LIMIT 3 AFTER a > 50
SETTINGS group_by_use_nulls = 1;

SELECT 'CUBE';
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10))
GROUP BY a WITH CUBE ORDER BY a LIMIT 3 AFTER a > 50
SETTINGS group_by_use_nulls = 1;
