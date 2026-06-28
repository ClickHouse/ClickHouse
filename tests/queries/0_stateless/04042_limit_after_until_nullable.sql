-- `NULL` in `LIMIT AFTER`/`UNTIL` conditions must be treated as false.
-- `nullIf` keeps the nested `UInt8` payload non-zero for null rows, so this catches
-- ignoring the null map in the fast path.
SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT AFTER nullIf(toNullable(number >= 2), 1)
);

SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT UNTIL nullIf(toNullable(number >= 2), 1)
);

SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT AFTER nullIf(toNullable(number >= 2), 1)
)
SETTINGS enable_analyzer = 0;

SELECT count()
FROM
(
    SELECT number
    FROM numbers(6)
    ORDER BY number
    LIMIT UNTIL nullIf(toNullable(number >= 2), 1)
)
SETTINGS enable_analyzer = 0;
