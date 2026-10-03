-- Comparison of FixedString(16) columns has a specialized kernel. Cross-check it against String comparison.

-- Bytes with the high bit set must compare as unsigned.
SELECT x < y, x > y, x <= y, x >= y, y < x, y > x
FROM
(
    SELECT
        materialize(toFixedString(unhex('7fffffffffffffffffffffffffffffff'), 16)) AS x,
        materialize(toFixedString(unhex('80000000000000000000000000000000'), 16)) AS y
);

-- Column vs column, with the values sharing a prefix of 0..16 bytes.
WITH
    sipHash128(number) AS x,
    sipHash128(number + 1) AS y,
    number % 17 AS prefix,
    toFixedString(concat(substring(x, 1, prefix), substring(y, prefix + 1, 16 - prefix)), 16) AS b
SELECT
    sum((x < b) != (toString(x) < toString(b))),
    sum((x > b) != (toString(x) > toString(b))),
    sum((x <= b) != (toString(x) <= toString(b))),
    sum((x >= b) != (toString(x) >= toString(b))),
    sum(x < b) > 0,
    sum(x > b) > 0,
    sum(x = b) > 0
FROM numbers(100000);

-- Column vs constant and constant vs column.
WITH
    sipHash128(number) AS x,
    toFixedString(unhex('80000000000000000000000000000000'), 16) AS c
SELECT
    sum((x < c) != (toString(x) < toString(c))),
    sum((x > c) != (toString(x) > toString(c))),
    sum((x <= c) != (toString(x) <= toString(c))),
    sum((x >= c) != (toString(x) >= toString(c))),
    sum((c < x) != (toString(c) < toString(x))),
    sum((c > x) != (toString(c) > toString(x))),
    sum(x < c) > 0,
    sum(x > c) > 0
FROM numbers(100000);
