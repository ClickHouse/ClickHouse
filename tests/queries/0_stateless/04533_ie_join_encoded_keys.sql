-- Tags: long, no-old-analyzer

-- The fixed-width key fast path: every encodable key type joined against the cross-join oracle
-- (comma join with the conditions in WHERE; `cross_to_inner_join_rewrite = 0` keeps it out of
-- IEJoin) on duplicate-heavy data, so tie runs exercise the requirement that the encoding
-- preserves equality exactly, not just the order. Also covered: a Nullable encoded key with
-- NULL rows, the shared-column `BETWEEN` shapes (the second condition's encoding is derived
-- from the first), and String conditions that keep the generic comparator, both mixed with an
-- encoded condition in one query and on their own. The operator pairs vary across the types so
-- all four L1/L2 direction combinations are sampled.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
-- Several sort-output chunks per input, so the merge that builds L1 crosses chunk boundaries.
SET max_block_size = 128;

DROP TABLE IF EXISTS enc_l;
DROP TABLE IF EXISTS enc_r;

CREATE TABLE enc_l
(
    id UInt32,
    u8a UInt8, u8b UInt8,
    u16a UInt16, u16b UInt16,
    u32a UInt32, u32b UInt32,
    u64a UInt64, u64b UInt64,
    i8a Int8, i8b Int8,
    i16a Int16, i16b Int16,
    i32a Int32, i32b Int32,
    i64a Int64, i64b Int64,
    f32a Float32, f32b Float32,
    f64a Float64, f64b Float64,
    da Date, db Date,
    d32a Date32, d32b Date32,
    dta DateTime('UTC'), dtb DateTime('UTC'),
    dt64a DateTime64(3, 'UTC'), dt64b DateTime64(3, 'UTC'),
    dec32a Decimal32(2), dec32b Decimal32(2),
    dec64a Decimal64(4), dec64b Decimal64(4),
    e8a Enum8('a' = -5, 'b' = -1, 'c' = 3, 'd' = 100), e8b Enum8('a' = -5, 'b' = -1, 'c' = 3, 'd' = 100),
    e16a Enum16('w' = -300, 'x' = 0, 'y' = 5, 'z' = 1000), e16b Enum16('w' = -300, 'x' = 0, 'y' = 5, 'z' = 1000),
    ba Bool, bb Bool,
    ni32a Nullable(Int32), ni32b Nullable(Int32),
    sa String, sb String
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE enc_r AS enc_l;

-- Duplicate-heavy deterministic data: every column draws from a small set of values, negative
-- values included for the signed types, a negative zero mixed into the floats (it must compare
-- equal to positive zero), pre-epoch values for Date32/DateTime64.
INSERT INTO enc_l SELECT
    number,
    toUInt8(n % 7), toUInt8((n + 3) % 5),
    toUInt16((n * 7) % 11), toUInt16(n % 9),
    toUInt32(intDiv(n, 3) % 13), toUInt32((n * 5) % 7),
    intHash64(n) % 9, intHash64(n + 1) % 6,
    toInt8((n % 11) - 5), toInt8(((n + 2) % 9) - 4),
    toInt16((n % 15) - 7), toInt16(((n * 3) % 11) - 5),
    toInt32((n % 21) - 10), toInt32(((n + 5) % 13) - 6),
    toInt64((n % 17) - 8), toInt64(((n * 3 + 1) % 19) - 9),
    toFloat32(((n % 13) - 6) / 4), if(n % 8 = 0, toFloat32('-0'), toFloat32(((n + 4) % 9) - 4)),
    if(n % 9 = 0, toFloat64('-0'), ((n % 19) - 9) / 8), ((n + 7) % 11 - 5) / 2,
    toDate('2020-01-01') + (n % 9), toDate('2020-06-01') + ((n + 2) % 7),
    toDate32('1900-01-05') + (n % 11), toDate32('2020-01-01') + ((n * 3) % 8),
    toDateTime('2020-01-01 00:00:00', 'UTC') + (n % 13) * 3600, toDateTime('2021-01-01 00:00:00', 'UTC') + ((n + 5) % 8) * 60,
    addMilliseconds(toDateTime64('1969-12-31 23:59:59', 3, 'UTC'), (n % 7) * 250), addMilliseconds(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), ((n + 3) % 9) * 125),
    toDecimal32(((n % 15) - 7) * 0.25, 2), toDecimal32(toInt64((n + 4) % 9) - 4, 2),
    toDecimal64(((n % 23) - 11) * 0.125, 4), toDecimal64((((n * 3) % 13) - 6) * 0.5, 4),
    ['a', 'b', 'c', 'd'][(n % 4) + 1], ['a', 'b', 'c', 'd'][((n + 1) % 4) + 1],
    ['w', 'x', 'y', 'z'][(n % 4) + 1], ['w', 'x', 'y', 'z'][((n + 2) % 4) + 1],
    (n % 2) = 0, (n % 3) = 0,
    if(n % 7 = 0, NULL, toInt32((n % 13) - 6)), if(n % 5 = 0, NULL, toInt32((n % 9) - 4)),
    repeat(char(65 + (n % 5)), 1 + (n % 3)), repeat(char(97 + ((n + 2) % 4)), 1 + ((n + 1) % 3))
FROM (SELECT number, number AS n FROM numbers(400));

INSERT INTO enc_r SELECT
    number,
    toUInt8(n % 7), toUInt8((n + 3) % 5),
    toUInt16((n * 7) % 11), toUInt16(n % 9),
    toUInt32(intDiv(n, 3) % 13), toUInt32((n * 5) % 7),
    intHash64(n) % 9, intHash64(n + 1) % 6,
    toInt8((n % 11) - 5), toInt8(((n + 2) % 9) - 4),
    toInt16((n % 15) - 7), toInt16(((n * 3) % 11) - 5),
    toInt32((n % 21) - 10), toInt32(((n + 5) % 13) - 6),
    toInt64((n % 17) - 8), toInt64(((n * 3 + 1) % 19) - 9),
    toFloat32(((n % 13) - 6) / 4), if(n % 8 = 0, toFloat32('-0'), toFloat32(((n + 4) % 9) - 4)),
    if(n % 9 = 0, toFloat64('-0'), ((n % 19) - 9) / 8), ((n + 7) % 11 - 5) / 2,
    toDate('2020-01-01') + (n % 9), toDate('2020-06-01') + ((n + 2) % 7),
    toDate32('1900-01-05') + (n % 11), toDate32('2020-01-01') + ((n * 3) % 8),
    toDateTime('2020-01-01 00:00:00', 'UTC') + (n % 13) * 3600, toDateTime('2021-01-01 00:00:00', 'UTC') + ((n + 5) % 8) * 60,
    addMilliseconds(toDateTime64('1969-12-31 23:59:59', 3, 'UTC'), (n % 7) * 250), addMilliseconds(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), ((n + 3) % 9) * 125),
    toDecimal32(((n % 15) - 7) * 0.25, 2), toDecimal32(toInt64((n + 4) % 9) - 4, 2),
    toDecimal64(((n % 23) - 11) * 0.125, 4), toDecimal64((((n * 3) % 13) - 6) * 0.5, 4),
    ['a', 'b', 'c', 'd'][(n % 4) + 1], ['a', 'b', 'c', 'd'][((n + 1) % 4) + 1],
    ['w', 'x', 'y', 'z'][(n % 4) + 1], ['w', 'x', 'y', 'z'][((n + 2) % 4) + 1],
    (n % 2) = 0, (n % 3) = 0,
    if(n % 7 = 0, NULL, toInt32((n % 13) - 6)), if(n % 5 = 0, NULL, toInt32((n % 9) - 4)),
    repeat(char(65 + (n % 5)), 1 + (n % 3)), repeat(char(97 + ((n + 2) % 4)), 1 + ((n + 1) % 3))
FROM (SELECT number, number + 61 AS n FROM numbers(400));

-- These shapes must run as IEJoin, otherwise the oracle comparisons below are vacuous.
SELECT 'plan encoded', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.u8a <= r.u8a AND l.u8b > r.u8b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan nullable', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan mixed', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.i64b > r.i64b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan generic', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.sb > r.sb) WHERE explain LIKE '%IEJoin%';
SELECT 'plan decimal', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.dec32a < r.dec32a AND l.dec32b <= r.dec32b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan datetime64', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.dt64a >= r.dt64a AND l.dt64b > r.dt64b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan enum', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.e8a <= r.e8a AND l.e8b > r.e8b) WHERE explain LIKE '%IEJoin%';
SELECT 'plan bool', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.ba <= r.ba AND l.bb >= r.bb) WHERE explain LIKE '%IEJoin%';
SELECT 'plan date32', count() > 0 FROM (EXPLAIN SELECT count() FROM enc_l l JOIN enc_r r ON l.d32a > r.d32a AND l.d32b <= r.d32b) WHERE explain LIKE '%IEJoin%';

SELECT 'UInt8', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.u8a <= r.u8a AND l.u8b > r.u8b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.u8a <= r.u8a AND l.u8b > r.u8b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.u8a <= r.u8a AND l.u8b > r.u8b) AS cnt;
SELECT 'UInt16', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.u16a < r.u16a AND l.u16b >= r.u16b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.u16a < r.u16a AND l.u16b >= r.u16b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.u16a < r.u16a AND l.u16b >= r.u16b) AS cnt;
SELECT 'UInt32', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.u32a >= r.u32a AND l.u32b < r.u32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.u32a >= r.u32a AND l.u32b < r.u32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.u32a >= r.u32a AND l.u32b < r.u32b) AS cnt;
SELECT 'UInt64', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.u64a > r.u64a AND l.u64b <= r.u64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.u64a > r.u64a AND l.u64b <= r.u64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.u64a > r.u64a AND l.u64b <= r.u64b) AS cnt;
SELECT 'Int8', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i8a <= r.i8a AND l.i8b < r.i8b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i8a <= r.i8a AND l.i8b < r.i8b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i8a <= r.i8a AND l.i8b < r.i8b) AS cnt;
SELECT 'Int16', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i16a < r.i16a AND l.i16b <= r.i16b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i16a < r.i16a AND l.i16b <= r.i16b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i16a < r.i16a AND l.i16b <= r.i16b) AS cnt;
SELECT 'Int32', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i32a >= r.i32a AND l.i32b > r.i32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i32a >= r.i32a AND l.i32b > r.i32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i32a >= r.i32a AND l.i32b > r.i32b) AS cnt;
SELECT 'Int64', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i64a > r.i64a AND l.i64b >= r.i64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i64a > r.i64a AND l.i64b >= r.i64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i64a > r.i64a AND l.i64b >= r.i64b) AS cnt;
SELECT 'Float32', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.f32a <= r.f32a AND l.f32b > r.f32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.f32a <= r.f32a AND l.f32b > r.f32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.f32a <= r.f32a AND l.f32b > r.f32b) AS cnt;
SELECT 'Float64', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.f64a >= r.f64a AND l.f64b < r.f64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.f64a >= r.f64a AND l.f64b < r.f64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.f64a >= r.f64a AND l.f64b < r.f64b) AS cnt;
SELECT 'Date', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.da < r.da AND l.db >= r.db) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.da < r.da AND l.db >= r.db) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.da < r.da AND l.db >= r.db) AS cnt;
SELECT 'Date32', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.d32a > r.d32a AND l.d32b <= r.d32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.d32a > r.d32a AND l.d32b <= r.d32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.d32a > r.d32a AND l.d32b <= r.d32b) AS cnt;
SELECT 'DateTime', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.dta <= r.dta AND l.dtb < r.dtb) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.dta <= r.dta AND l.dtb < r.dtb) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.dta <= r.dta AND l.dtb < r.dtb) AS cnt;
SELECT 'DateTime64', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.dt64a >= r.dt64a AND l.dt64b > r.dt64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.dt64a >= r.dt64a AND l.dt64b > r.dt64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.dt64a >= r.dt64a AND l.dt64b > r.dt64b) AS cnt;
SELECT 'Decimal32', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.dec32a < r.dec32a AND l.dec32b <= r.dec32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.dec32a < r.dec32a AND l.dec32b <= r.dec32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.dec32a < r.dec32a AND l.dec32b <= r.dec32b) AS cnt;
SELECT 'Decimal64', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.dec64a > r.dec64a AND l.dec64b >= r.dec64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.dec64a > r.dec64a AND l.dec64b >= r.dec64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.dec64a > r.dec64a AND l.dec64b >= r.dec64b) AS cnt;
SELECT 'Enum8', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.e8a <= r.e8a AND l.e8b > r.e8b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.e8a <= r.e8a AND l.e8b > r.e8b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.e8a <= r.e8a AND l.e8b > r.e8b) AS cnt;
SELECT 'Enum16', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.e16a >= r.e16a AND l.e16b < r.e16b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.e16a >= r.e16a AND l.e16b < r.e16b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.e16a >= r.e16a AND l.e16b < r.e16b) AS cnt;
SELECT 'Bool', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.ba <= r.ba AND l.bb >= r.bb) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.ba <= r.ba AND l.bb >= r.bb) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.ba <= r.ba AND l.bb >= r.bb) AS cnt;

-- A Nullable encoded key with NULL rows: NULL keys match nothing for INNER and come back as
-- unmatched rows for LEFT (the LEFT count oracle: inner pairs plus left rows without a match).
SELECT 'Nullable', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) AS cnt;
SELECT 'Nullable left', (SELECT count() FROM enc_l l LEFT JOIN enc_r r ON l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) = (SELECT (SELECT count() FROM enc_l l, enc_r r WHERE l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) + (SELECT count() FROM enc_l) - (SELECT uniqExact(l.id) FROM enc_l l, enc_r r WHERE l.ni32a <= r.ni32a AND l.ni32b > r.ni32b)) AS ok, (SELECT count() FROM enc_l l LEFT JOIN enc_r r ON l.ni32a <= r.ni32a AND l.ni32b > r.ni32b) AS cnt;

-- Shared-column shapes: the left side reads one column in both conditions, so its second
-- encoding is derived from the first - once with the same sort direction (the `BETWEEN` shape,
-- which also takes the L2 merge shortcut) and once with the opposite one.
SELECT 'shared same dir', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i64a >= r.i64a AND l.i64a <= r.i64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i64a >= r.i64a AND l.i64a <= r.i64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i64a >= r.i64a AND l.i64a <= r.i64b) AS cnt;
SELECT 'shared flip dir', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i64a >= r.i64a AND l.i64a >= r.i64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i64a >= r.i64a AND l.i64a >= r.i64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i64a >= r.i64a AND l.i64a >= r.i64b) AS cnt;

-- String keys have no fixed-width encoding: the generic comparator serves that condition while
-- the other one is encoded, in either position, and both conditions stay generic together.
SELECT 'generic first', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.i64b > r.i64b) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.sa < r.sa AND l.i64b > r.i64b) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.i64b > r.i64b) AS cnt;
SELECT 'generic second', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.i64a > r.i64a AND l.sb < r.sb) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.i64a > r.i64a AND l.sb < r.sb) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.i64a > r.i64a AND l.sb < r.sb) AS cnt;
SELECT 'generic both', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.sb > r.sb) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM enc_l l, enc_r r WHERE l.sa < r.sa AND l.sb > r.sb) AS ok, (SELECT count() FROM enc_l l JOIN enc_r r ON l.sa < r.sa AND l.sb > r.sb) AS cnt;

DROP TABLE enc_l;
DROP TABLE enc_r;
