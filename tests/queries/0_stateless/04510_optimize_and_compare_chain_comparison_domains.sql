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

-- Checking only the generated endpoint types is insufficient. The endpoints here are native
-- numbers, but the unsafe `Decimal` intermediate still makes the inferred comparison false.
SELECT 'unsafe intermediate', count()
FROM values(
    'i Int64, d Decimal64(1)',
    (36028797018963969, '36028797018963969.1'))
WHERE i < d AND d <= toFloat64(36028797018963968);

-- A constant `String` is converted independently to the type of the other operand. `256` fits
-- into `UInt16` but not into `UInt8`, so the implied endpoint comparison is false.
SELECT 'constant string conversion', count()
FROM values('a UInt16, b UInt8', (200, 100))
WHERE '256' > a AND a > b;

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

-- Types that accept only their own exact type as a comparison counterpart keep a single
-- order per type. Keep deriving conditions for chains over one such type: UUID, one
-- concrete Enum, one FixedString width.
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

-- The exact-type domain must not connect different types: FixedString of different widths
-- trims padding when converting while equal widths compare the padded bytes, and Enum
-- converts against a String column by name. No conditions may be derived across those.
SELECT
    'exact type requires equal types',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f3 FixedString(3), f5 FixedString(5)', ('aaa', 'bbbbb'))
           WHERE f3 < f5 AND f5 < toFixedString('zzzzz', 5)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        =
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('f3 FixedString(3), f5 FixedString(5)', ('aaa', 'bbbbb'))
           WHERE f3 < f5 AND f5 < toFixedString('zzzzz', 5)
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
