-- Both boundary conditions should prepare their `IN (subquery)` sets.
SELECT number
FROM numbers(8)
ORDER BY number
LIMIT AFTER number IN (SELECT 2) UNTIL number IN (SELECT 6);

SELECT number
FROM numbers(8)
ORDER BY number
LIMIT 2 AFTER number IN (SELECT 2) UNTIL number IN (SELECT 6);

SELECT count()
FROM
(
    SELECT number
    FROM numbers(8)
    ORDER BY number
    LIMIT AFTER number IN (SELECT 6) UNTIL number IN (SELECT 2)
);
