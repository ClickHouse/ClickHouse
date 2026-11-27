-- Tags: no-old-analyzer

-- Boundary values of the encoded key types (the sign-bit flip must hold at the extremes) and
-- wide types served by the generic comparator (Int128, UInt128, Decimal128, UUID, FixedString),
-- all verified against the cross-join oracle.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ext_l;
DROP TABLE IF EXISTS ext_r;

CREATE TABLE ext_l (id Int32, i64 Int64, u64 UInt64, i8 Int8, f64 Float64, i128 Int128, u128 UInt128, d128 Decimal128(10), uuid UUID, fs FixedString(4), y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ext_r AS ext_l;

INSERT INTO ext_l VALUES
    (1, -9223372036854775808, 0, -128, -1.7976931348623157e308, -170141183460469231731687303715884105728, 0, -1234567890.1234567890, '00000000-0000-0000-0000-000000000000', 'aaaa', 1),
    (2, -1, 9223372036854775807, -1, -0.0, -1, 170141183460469231731687303715884105727, -0.0000000001, '00000000-0000-0000-ffff-ffffffffffff', 'abcd', 3),
    (3, 0, 9223372036854775808, 0, 0.0, 0, 240282366920938463463374607431768211455, 0, '80000000-0000-0000-0000-000000000000', 'zzzz', 2),
    (4, 9223372036854775807, 18446744073709551615, 127, 1.7976931348623157e308, 170141183460469231731687303715884105727, 340282366920938463463374607431768211455, 1234567890.1234567890, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'mmmm', 4),
    (5, 42, 4242, 42, 42.5, 42, 42, 42.42, '42424242-4242-4242-4242-424242424242', 'qqqq', 0);

INSERT INTO ext_r VALUES
    (1, -9223372036854775807, 1, -127, -1.5e308, -170141183460469231731687303715884105727, 1, -1234567890.1234567889, '00000000-0000-0000-0000-000000000001', 'aaab', 2),
    (2, 0, 9223372036854775806, 0, 0.0, 0, 170141183460469231731687303715884105728, 0.0000000001, '7fffffff-ffff-ffff-ffff-ffffffffffff', 'abcc', 1),
    (3, 1, 9223372036854775809, 1, 1.5, 1, 240282366920938463463374607431768211454, 1, '80000000-0000-0000-0000-000000000001', 'yzzz', 4),
    (4, 9223372036854775806, 18446744073709551614, 126, 1.7e308, 170141183460469231731687303715884105726, 340282366920938463463374607431768211454, 1234567890.1234567891, 'fffffffe-ffff-ffff-ffff-ffffffffffff', 'nnnn', 0),
    (5, -42, 42, -42, -42.5, -42, 4242, -42.42, '24242424-2424-2424-2424-242424242424', 'pppp', 3);

SELECT 'plan i64', count() > 0 FROM (EXPLAIN SELECT count() FROM ext_l l JOIN ext_r r ON l.i64 < r.i64 AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'plan i128', count() > 0 FROM (EXPLAIN SELECT count() FROM ext_l l JOIN ext_r r ON l.i128 < r.i128 AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'plan uuid', count() > 0 FROM (EXPLAIN SELECT count() FROM ext_l l JOIN ext_r r ON l.uuid < r.uuid AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

SELECT 'Int64', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.i64 < r.i64 AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.i64 < r.i64 AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.i64 < r.i64 AND l.y > r.y) AS cnt;
SELECT 'UInt64', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.u64 >= r.u64 AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.u64 >= r.u64 AND l.y < r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.u64 >= r.u64 AND l.y < r.y) AS cnt;
SELECT 'Int8', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.i8 <= r.i8 AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.i8 <= r.i8 AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.i8 <= r.i8 AND l.y > r.y) AS cnt;
SELECT 'Float64', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.f64 < r.f64 AND l.y >= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.f64 < r.f64 AND l.y >= r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.f64 < r.f64 AND l.y >= r.y) AS cnt;
SELECT 'Int128', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.i128 < r.i128 AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.i128 < r.i128 AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.i128 < r.i128 AND l.y > r.y) AS cnt;
SELECT 'UInt128', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.u128 > r.u128 AND l.y <= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.u128 > r.u128 AND l.y <= r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.u128 > r.u128 AND l.y <= r.y) AS cnt;
SELECT 'Decimal128', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.d128 <= r.d128 AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.d128 <= r.d128 AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.d128 <= r.d128 AND l.y > r.y) AS cnt;
SELECT 'UUID', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.uuid < r.uuid AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.uuid < r.uuid AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.uuid < r.uuid AND l.y > r.y) AS cnt;
SELECT 'FixedString', (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l JOIN ext_r r ON l.fs < r.fs AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ext_l l, ext_r r WHERE l.fs < r.fs AND l.y > r.y) AS ok, (SELECT count() FROM ext_l l JOIN ext_r r ON l.fs < r.fs AND l.y > r.y) AS cnt;

DROP TABLE ext_l;
DROP TABLE ext_r;
