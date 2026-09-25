-- Regression test for `optimize_redundant_comparisons` on DateTime64 columns.
--
-- The comparison-chain pruning converts comparison constants to the column type with a strict
-- (lossless) conversion and only folds/prunes when the conversion succeeds. However, strict
-- conversion did not reject lossy DateTime64/Time64 scale reduction (`convertFieldToType` silently
-- truncates a higher-scale value to a lower scale), so a finer-scale constant collapsed onto the
-- column scale and the optimizer folded the chain using a value different from the original
-- constant. This changed query results compared to running with the optimization disabled.
--
-- Each scenario below must produce the same result with `optimize_redundant_comparisons` enabled
-- and disabled.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS 04489_dt64;

CREATE TABLE 04489_dt64 (dt DateTime64(1)) ENGINE = Memory;
INSERT INTO 04489_dt64 VALUES (toDateTime64('1970-01-01 00:00:01.2', 1)), (toDateTime64('1970-01-01 00:00:01.3', 1));

-- equals + notEquals against a finer-scale constant that is NOT representable in the column scale.
-- `1.2 != 1.23`, so the row `1.2` must be kept; the chain must not be folded to `false`.
SELECT 'equals_not_equals_finer';
SELECT count() FROM 04489_dt64
WHERE dt = toDateTime64('1970-01-01 00:00:01.2', 1) AND dt != toDateTime64('1970-01-01 00:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04489_dt64
WHERE dt = toDateTime64('1970-01-01 00:00:01.2', 1) AND dt != toDateTime64('1970-01-01 00:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 1;

-- notEquals + lessOrEquals against a finer-scale constant: `1.2 != 1.23` and `1.2 <= 1.23`,
-- so the row `1.2` must be kept; the `<=` must not be strengthened to `<` using a truncated value.
SELECT 'not_equals_range_finer';
SELECT count() FROM 04489_dt64
WHERE dt != toDateTime64('1970-01-01 00:00:01.23', 2) AND dt <= toDateTime64('1970-01-01 00:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04489_dt64
WHERE dt != toDateTime64('1970-01-01 00:00:01.23', 2) AND dt <= toDateTime64('1970-01-01 00:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 1;

-- range tightening against two finer-scale constants: `1.2 < 1.23` and `1.2 < 1.27`, the row `1.2`
-- must be kept.
SELECT 'range_tighten_finer';
SELECT count() FROM 04489_dt64
WHERE dt < toDateTime64('1970-01-01 00:00:01.23', 2) AND dt < toDateTime64('1970-01-01 00:00:01.27', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04489_dt64
WHERE dt < toDateTime64('1970-01-01 00:00:01.23', 2) AND dt < toDateTime64('1970-01-01 00:00:01.27', 2)
SETTINGS optimize_redundant_comparisons = 1;

-- A lossless finer-scale constant (`1.20` of scale 2 is exactly `1.2` of scale 1) is still optimized:
-- `dt = 1.20 AND dt != 1.20` is a genuine contradiction and yields no rows with or without the setting.
SELECT 'equals_not_equals_lossless';
SELECT count() FROM 04489_dt64
WHERE dt = toDateTime64('1970-01-01 00:00:01.20', 2) AND dt != toDateTime64('1970-01-01 00:00:01.20', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04489_dt64
WHERE dt = toDateTime64('1970-01-01 00:00:01.20', 2) AND dt != toDateTime64('1970-01-01 00:00:01.20', 2)
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE 04489_dt64;
