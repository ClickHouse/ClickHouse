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
