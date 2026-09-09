-- `LIMIT ... AFTER ... ALL` should emit the union of all matching windows.
SELECT number
FROM numbers(10)
ORDER BY number
LIMIT 2 AFTER number IN (2, 6) ALL;

-- Overlapping windows should not duplicate rows.
SELECT number
FROM numbers(8)
ORDER BY number
LIMIT 2 AFTER number IN (2, 3) ALL;

-- `UNTIL` before the next start only closes the current window; earlier `UNTIL` matches are ignored.
SELECT number
FROM numbers(10)
ORDER BY number
LIMIT AFTER number IN (3, 7) ALL UNTIL number IN (1, 5, 9);

-- If a row matches both `AFTER` and `UNTIL`, it should not be emitted.
SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT 2 AFTER number IN (2, 3) ALL UNTIL number IN (3, 5)
);

SELECT number
FROM numbers(8)
ORDER BY number
LIMIT 2 AFTER number IN (2, 3) ALL
SETTINGS enable_analyzer = 0;

SELECT number
FROM numbers(10)
ORDER BY number
LIMIT AFTER number IN (3, 7) ALL UNTIL number IN (1, 5, 9)
SETTINGS enable_analyzer = 0;

SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT 2 AFTER number IN (2, 3) ALL UNTIL number IN (3, 5)
)
SETTINGS enable_analyzer = 0;
