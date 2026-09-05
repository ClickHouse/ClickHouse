-- LIMIT n AFTER expr [UNTIL expr] (issue #15341)
-- Output rows starting from the first row where AFTER condition is true,
-- until UNTIL condition is true (exclusive) or limit is reached.

-- LIMIT n AFTER expr: first 3 rows starting from first row where number >= 3
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 3;

-- LIMIT n AFTER start UNTIL end: rows from first match of start until first match of end (end row excluded)
SELECT number FROM numbers(10) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;

-- LIMIT caps the range
SELECT number FROM numbers(10) ORDER BY number LIMIT 2 AFTER number >= 2 UNTIL number >= 8;

-- No match for AFTER: empty result
SELECT number FROM numbers(5) ORDER BY number LIMIT 10 AFTER number >= 10;

-- UNTIL only (no AFTER): from start until first row where condition is true
SELECT number FROM numbers(5) ORDER BY number LIMIT 10 UNTIL number >= 3;
