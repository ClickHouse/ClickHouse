-- lag and lead: offset checks, offsets beyond the block or the partition, sparse input, and argument types.

SELECT '-- The offset must be a non-negative integer: not NULL, Nullable, fractional or a string';
SELECT lag(number, NULL) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lead(number, toNullable(1)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lag(number, if(number = 1, NULL, 1)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lag(number, 1.5) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lead(number, '1') OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lag(number, -1) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lead(number, -1) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT '-- lag and lead do not accept an explicit frame; lagInFrame and leadInFrame do';
SELECT lag(number) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lead(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT number,
    lagInFrame(number) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lag_in_frame,
    leadInFrame(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lead_in_frame
FROM numbers(3) ORDER BY number;

SELECT '-- The default must be compatible with the value type; a NULL default makes the result Nullable';
SELECT lag(number, 1, 'x') OVER (ORDER BY number) FROM numbers(3); -- { serverError NO_COMMON_TYPE }
SELECT number, lag(number, 1, NULL) OVER (ORDER BY number) AS l, toTypeName(l) FROM numbers(3) ORDER BY number;

SELECT '-- Several offsets in one query, with and without defaults';
SELECT number, lag(number) OVER w AS l1, lag(number, 2) OVER w AS l2, lag(number, 3, 100) OVER w AS l3, lead(number) OVER w AS n1, lead(number, 2) OVER w AS n2, lead(number, 3, 100) OVER w AS n3
FROM numbers(5) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- Offsets larger than the partition give the default';
SELECT number, lag(toInt32(number), 5) OVER (ORDER BY number) AS l, lead(toInt32(number), 5, -1) OVER (ORDER BY number) AS n FROM numbers(3) ORDER BY number;

SELECT '-- Offsets larger than one block';
SELECT countIf(l3000 != if(number >= 3000, number - 3000, 0)), countIf(n2047 != if(number + 2047 < 5000, number + 2047, 0)), count()
FROM
(
    SELECT number, lag(number, 3000) OVER (ORDER BY number) AS l3000, lead(number, 2047) OVER (ORDER BY number) AS n2047
    FROM numbers(5000)
    SETTINGS max_block_size = 100
);

SELECT '-- Offsets reaching the partition edge when partitions are not aligned to blocks';
SELECT countIf(l != if(number % 1000 = 999, number - 999, 0)), countIf(n != if(number % 1000 = 0, number + 999, 0)), count()
FROM
(
    SELECT number,
        lag(number, 999) OVER (PARTITION BY intDiv(number, 1000) ORDER BY number) AS l,
        lead(number, 999) OVER (PARTITION BY intDiv(number, 1000) ORDER BY number) AS n
    FROM numbers(5000)
    SETTINGS max_block_size = 128
);

SELECT '-- Sparse rows after a filter see their true neighbours';
SELECT countIf(nxt != if(number + 977 < 10000, number + 977, 0)), countIf(prv != if(number >= 977, number - 977, 0)), count()
FROM
(
    SELECT number, lead(number) OVER (ORDER BY number) AS nxt, lag(number) OVER (ORDER BY number) AS prv
    FROM numbers(10000) WHERE number % 977 = 0
    SETTINGS max_block_size = 64
);

SELECT '-- lag and lead agree with lagInFrame and leadInFrame over an unbounded frame';
SELECT countIf(a != b), countIf(c != d), count()
FROM
(
    SELECT
        lag(number, 7) OVER (PARTITION BY number % 3 ORDER BY number) AS a,
        lagInFrame(number, 7) OVER (PARTITION BY number % 3 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS b,
        lead(number, 7) OVER (PARTITION BY number % 3 ORDER BY number) AS c,
        leadInFrame(number, 7) OVER (PARTITION BY number % 3 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS d
    FROM numbers(3000)
    SETTINGS max_block_size = 50
);

SELECT '-- Argument types: the value at the partition edge is the default value of the type';
SELECT number,
    lag(toDate('2024-01-01') + number) OVER w AS d,
    lag(toDateTime('2024-01-01 00:00:00', 'UTC') + number) OVER w AS dt,
    lag(toString(number)) OVER w AS s,
    lag(toFixedString(toString(number), 2)) OVER w AS fs,
    lag(toLowCardinality(toString(number))) OVER w AS lc,
    lag(toDecimal64(number, 3)) OVER w AS dec,
    lag(toFloat32(number) / 4) OVER w AS f,
    lag([number, number]) OVER w AS arr,
    lag((number, toString(number))) OVER w AS tup,
    lag(map(number, number)) OVER w AS m,
    lag(toNullable(number)) OVER w AS nul,
    lag(CAST(number % 3, 'Enum8(\'a\' = 0, \'b\' = 1, \'c\' = 2)')) OVER w AS en,
    lag(toIPv4(number + 1)) OVER w AS ip
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- Explicit defaults for non-numeric types';
SELECT number,
    lag(toString(number), 1, 'none') OVER w AS s,
    lead(toDate('2024-01-01') + number, 1, toDate('1999-12-31')) OVER w AS d,
    lag([number], 1, [42]) OVER w AS arr,
    lead(toNullable(number), 1, NULL) OVER w AS nul
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- A window function cannot be an argument of another window function';
SELECT lead(lag(number) OVER (ORDER BY number)) OVER (ORDER BY number) FROM numbers(3); -- { serverError ILLEGAL_AGGREGATION }
