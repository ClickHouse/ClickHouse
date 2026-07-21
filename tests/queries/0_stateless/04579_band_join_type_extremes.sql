-- Tags: no-old-analyzer

-- Boundary values of the encoded key types: a point key that encodes to the greatest
-- possible value (Int64 max, UInt64 max) saturates the +1 fold of the loose lower bound,
-- and the type minima pin the sign-bit flip at the bottom of the encoding. All four
-- strict/loose bracket combinations per type, verified against the cross-join oracle.

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ext_p;
DROP TABLE IF EXISTS ext_i;

CREATE TABLE ext_p (id Int32, i64 Int64, u64 UInt64, f64 Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ext_i (id Int32, i64_lo Int64, i64_hi Int64, u64_lo UInt64, u64_hi UInt64, f64_lo Float64, f64_hi Float64) ENGINE = MergeTree ORDER BY id;

INSERT INTO ext_p VALUES
    (1, -9223372036854775808, 0, -1.7976931348623157e308),
    (2, -1, 1, -0.0),
    (3, 0, 9223372036854775806, 0.0),
    (4, 9223372036854775806, 18446744073709551614, 1.5),
    (5, 9223372036854775807, 18446744073709551615, 1.7976931348623157e308);

-- Interval 1 is degenerate at the type minimum, interval 5 at the maximum, interval 2 spans
-- the whole domain; 3 and 4 pin ties one step away from the extremes.
INSERT INTO ext_i VALUES
    (1, -9223372036854775808, -9223372036854775808, 0, 0, -1.7976931348623157e308, -1.7976931348623157e308),
    (2, -9223372036854775808, 9223372036854775807, 0, 18446744073709551615, -1.7976931348623157e308, 1.7976931348623157e308),
    (3, -2, 1, 1, 9223372036854775807, -0.5, 0.5),
    (4, 9223372036854775806, 9223372036854775807, 18446744073709551614, 18446744073709551615, 1.0, 2.0),
    (5, 9223372036854775807, 9223372036854775807, 18446744073709551615, 18446744073709551615, 1.7976931348623157e308, 1.7976931348623157e308);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM ext_p p JOIN ext_i i ON p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) WHERE explain LIKE '%BandJoin%';

SELECT 'Int64 loose/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) AS cnt;
SELECT 'Int64 strict/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.i64 > i.i64_lo AND p.i64 <= i.i64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.i64 > i.i64_lo AND p.i64 <= i.i64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.i64 > i.i64_lo AND p.i64 <= i.i64_hi) AS cnt;
SELECT 'Int64 loose/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.i64 >= i.i64_lo AND p.i64 < i.i64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.i64 >= i.i64_lo AND p.i64 < i.i64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.i64 >= i.i64_lo AND p.i64 < i.i64_hi) AS cnt;
SELECT 'Int64 strict/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.i64 > i.i64_lo AND p.i64 < i.i64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.i64 > i.i64_lo AND p.i64 < i.i64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.i64 > i.i64_lo AND p.i64 < i.i64_hi) AS cnt;

SELECT 'UInt64 loose/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.u64 >= i.u64_lo AND p.u64 <= i.u64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.u64 >= i.u64_lo AND p.u64 <= i.u64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.u64 >= i.u64_lo AND p.u64 <= i.u64_hi) AS cnt;
SELECT 'UInt64 strict/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) AS cnt;
SELECT 'UInt64 loose/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.u64 >= i.u64_lo AND p.u64 < i.u64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.u64 >= i.u64_lo AND p.u64 < i.u64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.u64 >= i.u64_lo AND p.u64 < i.u64_hi) AS cnt;
SELECT 'UInt64 strict/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.u64 > i.u64_lo AND p.u64 < i.u64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.u64 > i.u64_lo AND p.u64 < i.u64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.u64 > i.u64_lo AND p.u64 < i.u64_hi) AS cnt;

SELECT 'Float64 loose/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.f64 >= i.f64_lo AND p.f64 <= i.f64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.f64 >= i.f64_lo AND p.f64 <= i.f64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.f64 >= i.f64_lo AND p.f64 <= i.f64_hi) AS cnt;
SELECT 'Float64 strict/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.f64 > i.f64_lo AND p.f64 <= i.f64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.f64 > i.f64_lo AND p.f64 <= i.f64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.f64 > i.f64_lo AND p.f64 <= i.f64_hi) AS cnt;
SELECT 'Float64 loose/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.f64 >= i.f64_lo AND p.f64 < i.f64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.f64 >= i.f64_lo AND p.f64 < i.f64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.f64 >= i.f64_lo AND p.f64 < i.f64_hi) AS cnt;
SELECT 'Float64 strict/strict', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p JOIN ext_i i ON p.f64 > i.f64_lo AND p.f64 < i.f64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_p p, ext_i i WHERE p.f64 > i.f64_lo AND p.f64 < i.f64_hi) AS ok, (SELECT count() FROM ext_p p JOIN ext_i i ON p.f64 > i.f64_lo AND p.f64 < i.f64_hi) AS cnt;

-- The swapped orientation must apply the same saturation fold after normalization
SELECT 'swapped Int64 loose/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_i i JOIN ext_p p ON p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_i i, ext_p p WHERE p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) AS ok, (SELECT count() FROM ext_i i JOIN ext_p p ON p.i64 >= i.i64_lo AND p.i64 <= i.i64_hi) AS cnt;
SELECT 'swapped UInt64 strict/loose', (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_i i JOIN ext_p p ON p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) = (SELECT arraySort(groupArray((p.id, i.id))) FROM ext_i i, ext_p p WHERE p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) AS ok, (SELECT count() FROM ext_i i JOIN ext_p p ON p.u64 > i.u64_lo AND p.u64 <= i.u64_hi) AS cnt;

DROP TABLE ext_p;
DROP TABLE ext_i;
