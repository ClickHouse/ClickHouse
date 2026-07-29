-- The result structure of `waitUntil` is static and must be reported without executing the function.
SELECT '--- DESCRIBE ---';
DESCRIBE TABLE waitUntil(true);

SELECT '--- CREATE TABLE ... AS ---';
DROP TABLE IF EXISTS wait_until_as;
CREATE TABLE wait_until_as AS waitUntil(true);
DESCRIBE TABLE wait_until_as;
DROP TABLE wait_until_as;

DROP TABLE IF EXISTS wait_until_empty;
CREATE TABLE wait_until_empty ENGINE = Memory EMPTY AS SELECT * FROM waitUntil(true);
DESCRIBE TABLE wait_until_empty;
DROP TABLE wait_until_empty;

SELECT '--- A scalar subquery without FROM is accepted ---';
SELECT * FROM waitUntil((SELECT 1), 2, 0.01);

SELECT '--- An empty result counts as "not satisfied yet" ---';
SELECT * FROM waitUntil((SELECT 1 WHERE 0), 2, 0.01);
SELECT * FROM waitUntil((SELECT count() FROM numbers(10) HAVING 0), 2, 0.01);

SELECT '--- A scalar subquery with an explicit LIMIT 1 is accepted ---';
DROP TABLE IF EXISTS wait_until_status;
CREATE TABLE wait_until_status (id UInt32, ready UInt8) ENGINE = MergeTree ORDER BY id;

-- The table is still empty, so the condition returns no rows on every attempt.
SELECT * FROM waitUntil((SELECT ready FROM wait_until_status ORDER BY id DESC LIMIT 1), 2, 0.01);

INSERT INTO wait_until_status VALUES (1, 1);
SELECT * FROM waitUntil((SELECT ready FROM wait_until_status ORDER BY id DESC LIMIT 1), 2, 0.01);

-- An offset can still make the result empty, which is not an error.
SELECT * FROM waitUntil((SELECT ready FROM wait_until_status ORDER BY id DESC LIMIT 1 OFFSET 1), 2, 0.01);

SELECT '--- More than one row is rejected ---';
INSERT INTO wait_until_status VALUES (2, 1);
SELECT * FROM waitUntil((SELECT ready FROM wait_until_status), 2, 0.01); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

SELECT '--- UNION is rejected ---';
SELECT * FROM waitUntil((SELECT 1 UNION ALL SELECT 1), 2, 0.01); -- { serverError BAD_ARGUMENTS }

DROP TABLE wait_until_status;
