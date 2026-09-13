-- LIMIT BY + LIMIT AFTER/UNTIL combinations.
-- LIMIT BY runs first (per-group limiting), then LIMIT AFTER applies a range condition.

-- { echo }

-- LIMIT BY then LIMIT n AFTER: keep 2 rows per group, then take 3 starting from group >= 2.
SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 3 AFTER g >= 2;

-- LIMIT BY then LIMIT AFTER (no count): keep 1 per group, then everything from number >= 1.
SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 1 BY g
LIMIT AFTER number >= 1;

-- LIMIT BY then LIMIT n AFTER UNTIL: keep 2 per group, range from g >= 1 until g >= 2.
SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 10 AFTER g >= 1 UNTIL g >= 2;

-- LIMIT BY then LIMIT n AFTER ALL: keep 2 per group, emit 1-row windows at each matching row.
SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 1 AFTER number IN (1, 7) ALL;

-- LIMIT BY then LIMIT UNTIL: keep 2 per group, stop before g >= 2.
SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT UNTIL g >= 2;

-- Same queries with legacy interpreter.
SET enable_analyzer = 0;

SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 3 AFTER g >= 2;

SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 1 BY g
LIMIT AFTER number >= 1;

SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 10 AFTER g >= 1 UNTIL g >= 2;

SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT 1 AFTER number IN (1, 7) ALL;

SELECT number, number % 3 AS g
FROM numbers(12)
ORDER BY g, number
LIMIT 2 BY g
LIMIT UNTIL g >= 2;
