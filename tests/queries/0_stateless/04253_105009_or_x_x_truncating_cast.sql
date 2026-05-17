-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105009.
--
-- `tryOptimizeCommonExpressionsInOr` in `LogicalExpressionOptimizerPass` collapses
-- `OR(x, x)` to `x`, then coerces the result back to the original `or` return type
-- (`UInt8`). Before the fix, the coercion used `_CAST` which truncates a non-`UInt8`
-- numeric value to its low byte: values whose low byte is zero (256, 512, 65536, ...)
-- silently became `0` and the matching rows were dropped by the filter. The fix
-- replaces the cast with `notEquals(x, 0)` for proper boolean coercion.
--
-- The reduced reproducer is the issue body's example. The original SQLancer++ shape
-- was `WHERE (c0 * c0) OR (c0 * c0)` with `c0 = 1702554224` (the square is divisible
-- by 256). Identical root cause exists in `tryOptimizeCommonExpressionsInAnd`.

DROP TABLE IF EXISTS t_105009;

CREATE TABLE t_105009 (c0 Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_105009 VALUES (256), (512), (1), (255);

-- Every row is non-zero, so every variant must return 4.
SELECT 'Int64 OR(x,x)', count() FROM t_105009 WHERE c0 OR c0;
SELECT 'Int64 OR(x,x,x)', count() FROM t_105009 WHERE c0 OR c0 OR c0;
SELECT 'Int64 (x*x) OR (x*x)', count() FROM t_105009 WHERE (c0 * c0) OR (c0 * c0);
SELECT 'Int64 control x != 0', count() FROM t_105009 WHERE c0 != 0;

DROP TABLE t_105009;

-- Original SQLancer++ value: 1702554224^2 = 2898690885660242176 ≡ 0 (mod 256).
-- The single row must satisfy the filter after the fix.
SELECT 'SQLancer++ shape', count() FROM (SELECT 1702554224::Int64 AS c0) WHERE (c0 * c0) OR (c0 * c0);

-- Per-type coverage. Every value below has a low byte of 0 in some variant.
SELECT 'Int16',   count() FROM (SELECT arrayJoin([toInt16(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'Int32',   count() FROM (SELECT arrayJoin([toInt32(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'Int64',   count() FROM (SELECT arrayJoin([toInt64(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'UInt16',  count() FROM (SELECT arrayJoin([toUInt16(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'UInt32',  count() FROM (SELECT arrayJoin([toUInt32(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'UInt64',  count() FROM (SELECT arrayJoin([toUInt64(256), 512, 1, 255]) AS c0) WHERE c0 OR c0;
SELECT 'Float32', count() FROM (SELECT arrayJoin([toFloat32(256), 512, 1.5, 255]) AS c0) WHERE c0 OR c0;
SELECT 'Float64', count() FROM (SELECT arrayJoin([toFloat64(256), 512, 1.5, 255]) AS c0) WHERE c0 OR c0;

-- Boolean / UInt8 paths were never broken -- the optimizer skipped the coercion when
-- types matched. Pin them to catch regressions if the path changes.
SELECT 'UInt8', count() FROM (SELECT arrayJoin([toUInt8(1), 2, 3, 4]) AS c0) WHERE c0 OR c0;
SELECT 'Bool',  count() FROM (SELECT arrayJoin([true, true, false, true]) AS c0) WHERE c0 OR c0;

-- Falsy values must still be dropped. The fix only changes how non-zero values are
-- recognized; `0` (or `false`) must continue to evaluate as false.
SELECT 'Int64 with zeroes (expect 2)', count()
  FROM (SELECT arrayJoin([toInt64(256), 512, 0, 0]) AS c0) WHERE c0 OR c0;

-- Per-row projection must agree with the filter on every row, including the values
-- that the truncating cast used to silently drop.
SELECT 'projection agreement',
       sumIf(1, (c0 OR c0) = 1) = countIf(c0 != 0)
  FROM (SELECT arrayJoin([toInt64(256), 512, 65536, 2147483648, 1, 255, 0]) AS c0);

-- Nullable coverage. `DataTypeNullable::getDefault()` returns `Null`, so a naive
-- `notEquals(x, expression_type->getDefault())` would build `notEquals(x, NULL)`,
-- which evaluates to `NULL` for both `x = 0` and `x != 0` and is treated as `false`
-- by the filter -- every non-`NULL` row would be silently dropped. The fix uses
-- the *nested* type's default (`removeNullable(...)->getDefault() = 0`) so that
-- nullable numerics are coerced as `notEquals(x, 0)`. NULL rows still evaluate to
-- NULL (filtered out), but non-NULL non-zero rows survive.
DROP TABLE IF EXISTS t_105009_nullable;
CREATE TABLE t_105009_nullable (c0 Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple()
  SETTINGS allow_nullable_key = 1;
INSERT INTO t_105009_nullable VALUES (256), (512), (1), (255), (NULL), (0);

-- Expected: 4 non-zero non-NULL rows. Before the fix this returned 0
-- (notEquals(x, NULL) -> NULL -> false for every row).
SELECT 'Nullable(Int64) OR(x,x)',     count() FROM t_105009_nullable WHERE c0 OR c0;
SELECT 'Nullable(Int64) OR(x,x,x)',   count() FROM t_105009_nullable WHERE c0 OR c0 OR c0;
SELECT 'Nullable(Int64) control',     count() FROM t_105009_nullable WHERE c0 != 0;

DROP TABLE t_105009_nullable;

-- Nullable per-type coverage (low byte zero in some variants, plus NULL row).
SELECT 'Nullable(Int16)',   count() FROM (SELECT arrayJoin([toNullable(toInt16(256)), 512, 1, 255, NULL]) AS c0) WHERE c0 OR c0;
SELECT 'Nullable(Int32)',   count() FROM (SELECT arrayJoin([toNullable(toInt32(256)), 512, 1, 255, NULL]) AS c0) WHERE c0 OR c0;
SELECT 'Nullable(Int64)',   count() FROM (SELECT arrayJoin([toNullable(toInt64(256)), 512, 1, 255, NULL]) AS c0) WHERE c0 OR c0;
SELECT 'Nullable(UInt32)',  count() FROM (SELECT arrayJoin([toNullable(toUInt32(256)), 512, 1, 255, NULL]) AS c0) WHERE c0 OR c0;
SELECT 'Nullable(UInt64)',  count() FROM (SELECT arrayJoin([toNullable(toUInt64(256)), 512, 1, 255, NULL]) AS c0) WHERE c0 OR c0;
SELECT 'Nullable(Float64)', count() FROM (SELECT arrayJoin([toNullable(toFloat64(256)), 512, 1.5, 255, NULL]) AS c0) WHERE c0 OR c0;

-- `LowCardinality(Nullable(Int64))`: same trap as `Nullable(Int64)` -- the
-- `LowCardinality` wrapper's `getDefault()` delegates to the dictionary type
-- (`Nullable(Int64)`), which returns `Null`. The fix uses
-- `recursiveRemoveLowCardinality` + `removeNullable` to strip both wrappers and
-- get the numeric `0` default.
SELECT 'LC(Nullable(Int64))', count()
  FROM (SELECT arrayJoin([toLowCardinality(toNullable(toInt64(256))), 512, 1, 255, NULL]) AS c0)
  WHERE c0 OR c0;

-- Nullable falsy values: `0` is non-NULL but falsy -> filtered out; `NULL` ->
-- filtered out (notEquals returns NULL); only the two non-zero rows survive.
SELECT 'Nullable(Int64) zeroes+nulls', count()
  FROM (SELECT arrayJoin([toNullable(toInt64(256)), 512, 0, 0, NULL, NULL]) AS c0)
  WHERE c0 OR c0;

-- Bare-NULL OR bare-NULL collapses to a `Nullable(Nothing)` filter -- every row
-- evaluates to NULL and is filtered out. We must not raise an exception during
-- optimization (the `Nothing` fallback in the coercion guards against
-- `DataTypeNothing::getDefault` not returning a usable value).
SELECT 'Nullable(Nothing) filter', count() FROM (SELECT 1 AS c0) WHERE NULL OR NULL;

-- AND counterpart: the same coercion is used in `tryOptimizeCommonExpressionsInAnd`.
-- Without the fix, `WHERE (x AND x) OR (x AND x)` on Nullable(Int64) would drop
-- every non-NULL row (the inner AND result is Nullable, coercion built
-- `notEquals(x, NULL)`).
SELECT 'Nullable(Int64) (x AND x) OR (x AND x)', count()
  FROM (SELECT arrayJoin([toNullable(toInt64(256)), 512, 1, 255, NULL, 0]) AS c0)
  WHERE (c0 AND c0) OR (c0 AND c0);
