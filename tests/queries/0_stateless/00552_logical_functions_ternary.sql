
-- Tests codepath for ternary logic
SELECT
    -- x1, x2, x3, x4,
    xor(x1, x2, x3, x4) AS xor1,
    xor(xor(x1, x2), xor(x3, x4)) AS xor2,

    or(x1, x2, x3, x4) AS or1,
    or(x1 or x2, x3 or x4) AS or2,

    and(x1, x2, x3, x4) AS and1,
    and(x1 and x2, x3 and x4) AS and2
FROM (
    SELECT
        nullIf(toUInt8(number % 3), 2) AS x1,
        nullIf(toUInt8(number / 3 % 3), 2) AS x2,
        nullIf(toUInt8(number / 9 % 3), 2) AS x3,
        nullIf(toUInt8(number / 27 % 3), 2) AS x4
    FROM numbers(81)
)
WHERE
    (xor1 != xor2 OR (xor1 is NULL) != (xor2 is NULL)) OR
    (or1 != or2 OR (or1 is NULL) != (or2 is NULL) OR (and1 != and2 OR (and1 is NULL) != (and2 is NULL)))
;


-- Test ternary logic over multiple batches of columns (currently batch spans over 10 columns)
SELECT
    -- x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
    xor(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) AS xor1,
    xor(x1, xor(xor(xor(x2, x3), xor(x4, x5)), xor(xor(x6, x7), xor(x8, xor(x9, xor(x10, x11)))))) AS xor2,

    or(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) AS or1,
    or(x1, or(or(or(x2, x3), or(x4, x5)), or(or(x6, x7), or(x8, or(x9, or(x10, x11)))))) AS or2,

    and(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) AS and1,
    and(x1, and((x2 and x3) and (x4 and x5), (x6 and x7) and (x8 and (x9 and (x10 and x11))))) AS and2
FROM (
    SELECT
        nullIf(toUInt8(number % 3), 2) AS x1,
        nullIf(toUInt8(number / 3 % 3), 2) AS x2,
        nullIf(toUInt8(number / 9 % 3), 2) AS x3,
        nullIf(toUInt8(number / 27 % 3), 2) AS x4,
        nullIf(toUInt8(number / 81 % 3), 2) AS x5,
        nullIf(toUInt8(number / 243 % 3), 2) AS x6,
        nullIf(toUInt8(number / 729 % 3), 2) AS x7,
        nullIf(toUInt8(number / 2187 % 3), 2) AS x8,
        nullIf(toUInt8(number / 6561 % 3), 2) AS x9,
        nullIf(toUInt8(number / 19683 % 3), 2) AS x10,
        nullIf(toUInt8(number / 59049 % 3), 2) AS x11
    FROM numbers(177147)
)
WHERE
    (xor1 != xor2 OR (xor1 is NULL) != (xor2 is NULL)) OR
    (or1 != or2 OR (or1 is NULL) != (or2 is NULL) OR (and1 != and2 OR (and1 is NULL) != (and2 is NULL)))
;


SELECT 'OK';
