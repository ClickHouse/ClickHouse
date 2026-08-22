-- Some joins return different results with and without the analyzer. Let's enforce both stay the same.
SET enable_analyzer = 0;
SELECT 'left semi true';
SELECT l.number
FROM numbers(2) AS l SEMI JOIN numbers(3) AS r ON 1
ORDER BY l.number
SETTINGS join_algorithm = 'hash';

SELECT 'left semi false';
SELECT count()
FROM numbers(2) AS l SEMI JOIN numbers(3) AS r ON 0
SETTINGS join_algorithm = 'hash';

SELECT 'left anti true';
SELECT count()
FROM numbers(2) AS l ANTI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'left anti false';
SELECT l.number
FROM numbers(2) AS l ANTI JOIN numbers(3) AS r ON 0
ORDER BY l.number
SETTINGS join_algorithm = 'hash';

SELECT 'right semi true';
SELECT r.number
FROM numbers(2) AS l RIGHT SEMI JOIN numbers(3) AS r ON 1
ORDER BY r.number
SETTINGS join_algorithm = 'hash';

SELECT 'right semi false';
SELECT count()
FROM numbers(2) AS l RIGHT SEMI JOIN numbers(3) AS r ON 0
SETTINGS join_algorithm = 'hash';

SELECT 'right semi true empty left';
SELECT count()
FROM numbers(0) AS l RIGHT SEMI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'right anti true';
SELECT count()
FROM numbers(2) AS l RIGHT ANTI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'right anti false';
SELECT r.number
FROM numbers(2) AS l RIGHT ANTI JOIN numbers(3) AS r ON 0
ORDER BY r.number
SETTINGS join_algorithm = 'hash';

SELECT 'right anti true empty left';
SELECT r.number
FROM numbers(0) AS l RIGHT ANTI JOIN numbers(3) AS r ON 1
ORDER BY r.number
SETTINGS join_algorithm = 'hash';

-- With analyzer
SET enable_analyzer = 1;
SELECT 'left semi true';
SELECT l.number
FROM numbers(2) AS l SEMI JOIN numbers(3) AS r ON 1
ORDER BY l.number
SETTINGS join_algorithm = 'hash';

SELECT 'left semi false';
SELECT count()
FROM numbers(2) AS l SEMI JOIN numbers(3) AS r ON 0
SETTINGS join_algorithm = 'hash';

SELECT 'left anti true';
SELECT count()
FROM numbers(2) AS l ANTI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'left anti false';
SELECT l.number
FROM numbers(2) AS l ANTI JOIN numbers(3) AS r ON 0
ORDER BY l.number
SETTINGS join_algorithm = 'hash';

SELECT 'right semi true';
SELECT r.number
FROM numbers(2) AS l RIGHT SEMI JOIN numbers(3) AS r ON 1
ORDER BY r.number
SETTINGS join_algorithm = 'hash';

SELECT 'right semi false';
SELECT count()
FROM numbers(2) AS l RIGHT SEMI JOIN numbers(3) AS r ON 0
SETTINGS join_algorithm = 'hash';

SELECT 'right semi true empty left';
SELECT count()
FROM numbers(0) AS l RIGHT SEMI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'right anti true';
SELECT count()
FROM numbers(2) AS l RIGHT ANTI JOIN numbers(3) AS r ON 1
SETTINGS join_algorithm = 'hash';

SELECT 'right anti false';
SELECT r.number
FROM numbers(2) AS l RIGHT ANTI JOIN numbers(3) AS r ON 0
ORDER BY r.number
SETTINGS join_algorithm = 'hash';

SELECT 'right anti true empty left';
SELECT r.number
FROM numbers(0) AS l RIGHT ANTI JOIN numbers(3) AS r ON 1
ORDER BY r.number
SETTINGS join_algorithm = 'hash';
