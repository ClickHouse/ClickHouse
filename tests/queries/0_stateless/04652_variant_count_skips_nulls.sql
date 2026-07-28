-- A Variant is not Nullable, so the Null combinator (and with it AggregateFunctionCountNotNullUnary) is never
-- applied to it, but a Variant row can hold a NULL value. count(expr) counts the not-NULL values of its argument,
-- so the rows whose Variant value is NULL must not be counted -- the same result as counting over the
-- Nullable(supertype) form of the same data.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_count;
CREATE TABLE t_variant_count (v Variant(Int64, Float64), k UInt64) ENGINE = Memory;
INSERT INTO t_variant_count VALUES (1, 10), (2.5, 20), (NULL, 30), (3, 40), (NULL, 50);

SELECT 'count', count(v) FROM t_variant_count;
SELECT 'count all rows', count() FROM t_variant_count;
SELECT 'count is not null', countIf(NOT isNull(v)) FROM t_variant_count;
SELECT 'countIf', countIf(v, k > 15) FROM t_variant_count;
SELECT 'countIf all false', countIf(v, k > 1000) FROM t_variant_count;
-- countDistinct is the uniqExact function, which counts the NULL of a Variant as a distinct value, as it does for
-- any other value of a Variant.
SELECT 'countDistinct', countDistinct(v) FROM t_variant_count;

-- The same over the Nullable(supertype) form of the same values.
SELECT 'count over Nullable', count(CAST(v AS Nullable(Float64))) FROM t_variant_count;

-- The state form counts the same way, and its state stays byte-compatible with the plain count state, so it
-- merges with (and casts to) AggregateFunction(count).
SELECT 'countState', countMerge(s) FROM (SELECT countState(v) AS s FROM t_variant_count);
SELECT 'countState type', toTypeName(countState(v)) FROM t_variant_count;
SELECT 'countState merges with count()', countMerge(s) FROM
(
    SELECT CAST(countState(v) AS AggregateFunction(count)) AS s FROM t_variant_count
    UNION ALL
    SELECT countState() AS s FROM t_variant_count
);

-- With a group by, and with a Variant of a single alternative (every not-NULL row uses the same discriminator).
SELECT 'grouped', k % 4 AS m, count(v) FROM t_variant_count GROUP BY m ORDER BY m;
SELECT 'single alternative', count(CAST(number % 2 ? NULL : number AS Variant(UInt64))) FROM numbers(10);

-- A constant Variant argument, and an empty input.
SELECT 'constant NULL', count(CAST(NULL AS Variant(Int64, Float64))) FROM numbers(5);
SELECT 'constant value', count(CAST(1::Int64 AS Variant(Int64, Float64))) FROM numbers(5);
SELECT 'empty', count(v) FROM t_variant_count WHERE 0;

-- A Variant with no common supertype is counted natively as well: no cast to a supertype is involved.
SELECT 'no supertype', count(v) FROM
(
    SELECT CAST('a' AS Variant(String, UInt64)) AS v
    UNION ALL SELECT CAST(1::UInt64 AS Variant(String, UInt64))
    UNION ALL SELECT CAST(NULL AS Variant(String, UInt64))
);

-- Inside an array, the Array combinator counts the not-NULL elements of every array.
SELECT 'countArray', countArray([CAST(1::Int64 AS Variant(Int64, Float64)), CAST(NULL AS Variant(Int64, Float64))]);

DROP TABLE t_variant_count;
