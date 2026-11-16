SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;
SET optimize_and_compare_chain_max_hash_work = 0;
SET allow_experimental_time_time64_type = 1;

-- Comparisons involving different type pairs do not necessarily use the same ordering.
-- The optimizer must not connect such pairs into a transitive chain.

-- `Decimal` to integer is exact, while `Decimal` to `Float64` converts both operands to
-- `Float64`. The inferred endpoint comparison used to round `a` up and reject this row.
SELECT 'decimal float rounding', count()
FROM values(
    'a Decimal64(1), b Int64',
    ('36028797018963966.9', 36028797018963967))
WHERE a < b AND b < toFloat64(36028797018963968);

-- Equality edges bridge chains the same way inequalities do. The bridge `a = b` holds exactly,
-- but the derived `Decimal` vs `Float64` endpoint would round `a` up and reject the row.
SELECT 'equality bridge', count()
FROM values(
    'a Decimal64(1), b Int64',
    ('36028797018963967.0', 36028797018963967))
WHERE a = b AND b < toFloat64(36028797018963968);

-- Checking only the generated endpoint types is insufficient. The endpoints here are native
-- numbers, but the unsafe `Decimal` intermediate still makes the inferred comparison false.
SELECT 'unsafe intermediate', count()
FROM values(
    'i Int64, d Decimal64(1)',
    (36028797018963969, '36028797018963969.1'))
WHERE i < d AND d <= toFloat64(36028797018963968);

-- A `String` constant converts once to its direct counterpart's type (`256` fits `UInt16`,
-- not `UInt8`); the derived endpoint must use that typed value and stay numerically true.
SELECT 'constant string conversion', count()
FROM values('a UInt16, b UInt8', (200, 100))
WHERE '256' > a AND a > b;

-- The literal converts using its direct counterpart's timezone; a derived endpoint
-- must reuse that instant, not re-parse the literal in the other column's timezone.
SELECT 'string literal timezone', count()
FROM values(
    'ts1 DateTime(\'Pacific/Kiritimati\'), ts2 DateTime(\'UTC\')',
    ('2020-01-01 09:00:00', '2019-12-31 22:00:00'))
WHERE ts1 < ts2 AND ts2 < '2020-01-01 00:00:00';

-- Both original comparisons have a common type, but the inferred `Time` to `Date` endpoint does
-- not. The optimization used to turn this valid query into an exception during analysis.
SELECT 'incompatible endpoint', count()
FROM values(
    't Time, dt DateTime(\'UTC\')',
    ('01:00:00', '1970-01-01 02:00:00'))
WHERE t < dt AND dt < toDate('1970-01-02');

-- `Date` to `DateTime` conversion uses the timezone of the `DateTime` operand, so the
-- inferred endpoint comparison may convert differently than the original ones.
SELECT 'date datetime timezone', count()
FROM values(
    'd Date, dt DateTime(\'Pacific/Kiritimati\')',
    ('2020-01-01', '2020-01-01 00:30:00'))
WHERE d <= dt AND dt <= toDateTime('2019-12-31 11:00:00', 'UTC');

-- Comparing `DateTime64` of different scales rescales the values and can throw
-- `DECIMAL_OVERFLOW`, so an inferred mixed-scale comparison may fail on valid data.
SELECT 'datetime64 mixed scales', count()
FROM values(
    'a DateTime64(0, \'UTC\'), b DateTime64(0, \'UTC\')',
    ('2299-12-31 23:59:59', '2000-01-02 00:00:00'))
WHERE a > b AND b > toDateTime64('2000-01-01 00:00:00.000000001', 9, 'UTC');

-- Mixed-scale `Time64` comparisons also rescale: interval arithmetic can leave values beyond
-- the clamped range, so an inferred mixed-scale comparison may throw `DECIMAL_OVERFLOW`.
SELECT 'time64 mixed scales', count()
FROM (SELECT (CAST('999:59:59', 'Time64(0)') + INTERVAL 300 YEAR)::Time64(0) AS a, CAST('00:00:01', 'Time64(0)') AS b)
WHERE a > b AND b > toTime64('00:00:00.5', 9);

-- Comparing `Decimal` of different scales rescales the values and can throw `DECIMAL_OVERFLOW`,
-- so an inferred mixed-scale comparison may fail on valid data.
SELECT 'decimal mixed scales', count()
FROM values(
    'a Decimal64(0), b Decimal64(0)',
    ('999999999999999999', '1'))
WHERE a > b AND b > toDecimal64('0.5', 1);

-- `Enum` compares to a `String` column by name but converts a `String` constant to its id;
-- a derived endpoint would flip between the two orders. Here the chain holds by name order
-- ('x' <= 'z') while the id-order endpoint `e <= 'z'` (5 <= 1) is false.
SELECT 'enum name id order mix', count()
FROM values('e Enum(\'x\' = 5, \'z\' = 1), s String', ('x', 'z'))
WHERE e <= s AND s <= 'z';

-- Keep deriving conditions inside explicitly supported domains.
SELECT
    'safe domains remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int32, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int32, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a String, b String', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a String, b String', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Integers of all widths, floats (BFloat16 included) and Enum values share one accurate
-- numeric order. Keep deriving conditions across the whole numeric domain.
SELECT
    'wide numeric domains remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int128, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int128, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a BFloat16, b Float64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a BFloat16, b Float64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Enum8(\'x\' = 1, \'y\' = 2), b Int32', ('x', 2))
           WHERE a < b AND b < 3
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Enum8(\'x\' = 1, \'y\' = 2), b Int32', ('x', 2))
           WHERE a < b AND b < 3
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- `Date` and `Date32` share a day-number order, while equal-scale time points compare their
-- underlying ticks. Keep deriving conditions inside both domains.
SELECT
    'date domains remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Date, b Date32', ('2020-01-01', '2020-01-02'))
           WHERE a < b AND b < toDate32('2020-01-03')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Date, b Date32', ('2020-01-01', '2020-01-02'))
           WHERE a < b AND b < toDate32('2020-01-03')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime(\'UTC\'), b DateTime(\'Pacific/Kiritimati\')', ('2020-01-01 00:00:00', '2020-01-02 00:00:01'))
           WHERE a < b AND b < toDateTime('2020-01-02 00:00:02', 'America/New_York')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime(\'UTC\'), b DateTime(\'Pacific/Kiritimati\')', ('2020-01-01 00:00:00', '2020-01-02 00:00:01'))
           WHERE a < b AND b < toDateTime('2020-01-02 00:00:02', 'America/New_York')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Equal tick scales never rescale: `DateTime` chains with `DateTime64(0)`, and
-- `DateTime64` chains with `DateTime64` of the same scale.
SELECT
    'same tick scale remains optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime(\'UTC\'), b DateTime64(0, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
           WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02', 0, 'UTC')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime(\'UTC\'), b DateTime64(0, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
           WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02', 0, 'UTC')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime64(3, \'UTC\'), b DateTime64(3, \'UTC\')', ('2020-01-01 00:00:00.1', '2020-01-01 00:00:00.2'))
           WHERE a < b AND b < toDateTime64('2020-01-01 00:00:00.3', 3, 'UTC')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a DateTime64(3, \'UTC\'), b DateTime64(3, \'UTC\')', ('2020-01-01 00:00:00.1', '2020-01-01 00:00:00.2'))
           WHERE a < b AND b < toDateTime64('2020-01-01 00:00:00.3', 3, 'UTC')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- `Time` and equal-scale `Time64` compare their ticks directly.
-- Keep deriving conditions inside the per-scale time-of-day domain.
SELECT
    'time same scale remains optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Time, b Time64(0)', ('01:00:00', '02:00:00'))
           WHERE a < b AND b < CAST('03:00:00', 'Time64(0)')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Time, b Time64(0)', ('01:00:00', '02:00:00'))
           WHERE a < b AND b < CAST('03:00:00', 'Time64(0)')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Time64(3), b Time64(3)', ('01:00:00.1', '01:00:00.2'))
           WHERE a < b AND b < CAST('01:00:00.3', 'Time64(3)')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Time64(3), b Time64(3)', ('01:00:00.1', '01:00:00.2'))
           WHERE a < b AND b < CAST('01:00:00.3', 'Time64(3)')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Equal-scale decimals compare their underlying integers directly, regardless of width.
-- Keep deriving conditions inside the per-scale `Decimal` domain.
SELECT
    'decimal same scale remains optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Decimal64(2), b Decimal64(2)', ('1.00', '2.00'))
           WHERE a < b AND b < toDecimal64('3.00', 2)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Decimal64(2), b Decimal64(2)', ('1.00', '2.00'))
           WHERE a < b AND b < toDecimal64('3.00', 2)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Decimal64(2), b Decimal128(2)', ('1.00', '2.00'))
           WHERE a < b AND b < toDecimal64('3.00', 2)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Decimal64(2), b Decimal128(2)', ('1.00', '2.00'))
           WHERE a < b AND b < toDecimal64('3.00', 2)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Cross-width `FixedString` comparisons zero-pad the shorter side into one shared byte order.
-- Keep deriving conditions across `FixedString` widths.
SELECT
    'fixed string widths remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f2 FixedString(2), f4 FixedString(4)', ('aa', 'bbbb'))
           WHERE f2 < f4 AND f4 < toFixedString('zzzz', 4)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f2 FixedString(2), f4 FixedString(4)', ('aa', 'bbbb'))
           WHERE f2 < f4 AND f4 < toFixedString('zzzz', 4)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- `LowCardinality` is normalized away before the domain is determined; chains over
-- `LowCardinality` columns keep deriving conditions.
SELECT
    'low cardinality remains optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a LowCardinality(String), b LowCardinality(String)', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a LowCardinality(String), b LowCardinality(String)', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- A const string operand converts once to the other side's type; such edges keep chaining.
SELECT
    'string literal edges remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('u1 UUID, u2 UUID', ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'))
           WHERE u1 < u2 AND u2 < '33333333-3333-3333-3333-333333333333'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('u1 UUID, u2 UUID', ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'))
           WHERE u1 < u2 AND u2 < '33333333-3333-3333-3333-333333333333'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('ts1 DateTime(\'UTC\'), ts2 DateTime(\'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
           WHERE ts1 < ts2 AND ts2 < '2020-01-02 00:00:00'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('ts1 DateTime(\'UTC\'), ts2 DateTime(\'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
           WHERE ts1 < ts2 AND ts2 < '2020-01-02 00:00:00'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('x UInt32, y UInt32', (1, 2))
           WHERE x < y AND y < '10'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('x UInt32, y UInt32', (1, 2))
           WHERE x < y AND y < '10'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Chains over one concrete type keep a single order and keep deriving conditions:
-- UUID via the exact-type domain, one concrete Enum and one FixedString width via
-- their shared numeric and byte-order domains.
SELECT
    'exact type domains remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('u1 UUID, u2 UUID', ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'))
           WHERE u1 < u2 AND u2 < toUUID('33333333-3333-3333-3333-333333333333')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('u1 UUID, u2 UUID', ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'))
           WHERE u1 < u2 AND u2 < toUUID('33333333-3333-3333-3333-333333333333')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('e1 Enum(\'x\' = 5, \'z\' = 1), e2 Enum(\'x\' = 5, \'z\' = 1)', ('z', 'z'))
           WHERE e1 < e2 AND e2 < CAST('x', 'Enum(\'x\' = 5, \'z\' = 1)')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('e1 Enum(\'x\' = 5, \'z\' = 1), e2 Enum(\'x\' = 5, \'z\' = 1)', ('z', 'z'))
           WHERE e1 < e2 AND e2 < CAST('x', 'Enum(\'x\' = 5, \'z\' = 1)')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f1 FixedString(3), f2 FixedString(3)', ('aaa', 'bbb'))
           WHERE f1 < f2 AND f2 < toFixedString('zzz', 3)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f1 FixedString(3), f2 FixedString(3)', ('aaa', 'bbb'))
           WHERE f1 < f2 AND f2 < toFixedString('zzz', 3)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Cross-domain edges must not connect a chain: Enum compares to a String column by name,
-- and named Tuple types that differ only in element names conservatively stay unchained
-- (their comparisons work, but the exact-type domain matches only equal types).
SELECT
    'exact type requires equal types',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Tuple(x Int32, y Int32), b Tuple(u Int32, v Int32)', ((1, 1), (2, 2)))
           WHERE a < b AND b < CAST((3, 3), 'Tuple(Int32, Int32)')
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        =
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Tuple(x Int32, y Int32), b Tuple(u Int32, v Int32)', ((1, 1), (2, 2)))
           WHERE a < b AND b < CAST((3, 3), 'Tuple(Int32, Int32)')
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('e Enum(\'x\' = 5, \'z\' = 1), s String', ('x', 'z'))
           WHERE e < s AND s < 'zz'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        =
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('e Enum(\'x\' = 5, \'z\' = 1), s String', ('x', 'z'))
           WHERE e < s AND s < 'zz'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');

-- Tuples with Nullable elements compare three-valued; their Nullable result makes the whole
-- `AND` chain Nullable, which the pass skips: results stay NULL and nothing is derived.
SELECT
    'tuple nullable keeps null',
    (SELECT x < y AND y < CAST(tuple(2), 'Tuple(Nullable(Int32))')
     FROM (SELECT materialize(CAST(tuple(3), 'Tuple(Nullable(Int32))')) AS x,
                  materialize(CAST(tuple(NULL), 'Tuple(Nullable(Int32))')) AS y)
     SETTINGS optimize_and_compare_chain = 1)
        IS NULL,
    (SELECT x < y AND y < CAST(tuple(2), 'Tuple(Nullable(Int32))')
     FROM (SELECT materialize(CAST(tuple(3), 'Tuple(Nullable(Int32))')) AS x,
                  materialize(CAST(tuple(NULL), 'Tuple(Nullable(Int32))')) AS y)
     SETTINGS optimize_and_compare_chain = 0)
        IS NULL,
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT x < y AND y < CAST(tuple(2), 'Tuple(Nullable(Int32))')
           FROM (SELECT materialize(CAST(tuple(3), 'Tuple(Nullable(Int32))')) AS x,
                        materialize(CAST(tuple(NULL), 'Tuple(Nullable(Int32))')) AS y)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        =
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT x < y AND y < CAST(tuple(2), 'Tuple(Nullable(Int32))')
           FROM (SELECT materialize(CAST(tuple(3), 'Tuple(Nullable(Int32))')) AS x,
                        materialize(CAST(tuple(NULL), 'Tuple(Nullable(Int32))')) AS y)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');
