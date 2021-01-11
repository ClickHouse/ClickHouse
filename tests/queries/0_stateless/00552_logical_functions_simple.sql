
-- Test simple logic over smaller batch of columns
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
        toUInt8(number % 2) AS x1,
        toUInt8(number / 2 % 2) AS x2,
        toUInt8(number / 4 % 2) AS x3,
        toUInt8(number / 8 % 2) AS x4
    FROM numbers(16)
)
WHERE
    xor1 != xor2 OR (and1 != and2 OR or1 != or2)
;

-- Test simple logic over multiple batches of columns (currently batch spans over 10 columns)
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
        toUInt8(number % 2) AS x1,
        toUInt8(number / 2 % 2) AS x2,
        toUInt8(number / 4 % 2) AS x3,
        toUInt8(number / 8 % 2) AS x4,
        toUInt8(number / 16 % 2) AS x5,
        toUInt8(number / 32 % 2) AS x6,
        toUInt8(number / 64 % 2) AS x7,
        toUInt8(number / 128 % 2) AS x8,
        toUInt8(number / 256 % 2) AS x9,
        toUInt8(number / 512 % 2) AS x10,
        toUInt8(number / 1024 % 2) AS x11
    FROM numbers(2048)
)
WHERE
    xor1 != xor2 OR (and1 != and2 OR or1 != or2)
;


SELECT 'OK';
