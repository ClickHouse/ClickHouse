-- The insert-time coalescing of duplicate-key rows (`optimize_on_insert`) must resolve `last_value` with the
-- settings of the INSERT query, exactly like a background CoalescingMergeTree merge or `OPTIMIZE ... FINAL`
-- uses the settings of the OPTIMIZE query (see 04657). Otherwise the persisted result for the same input rows
-- would depend only on whether they arrive in one insert block or are merged later.

SET allow_experimental_variant_type = 1;

-- `optimize_on_insert` is a setting of the INSERT query (the flaky-check harness randomizes it to 0,
-- and a `CREATE TABLE ... SETTINGS` clause applies it only to the CREATE query itself).
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS t_variant_coalescing_insert;
CREATE TABLE t_variant_coalescing_insert (k UInt64, v Variant(String)) ENGINE = CoalescingMergeTree ORDER BY k;

-- Default: the trailing NULL Variant row is skipped, the previous non-NULL value survives.
INSERT INTO t_variant_coalescing_insert VALUES (1, 'x'), (1, NULL);
SELECT 'skip nulls', v FROM t_variant_coalescing_insert;
TRUNCATE TABLE t_variant_coalescing_insert;

-- Compatibility: with the setting disabled, the NULL is an ordinary value and wins as the last one.
SET aggregate_functions_skip_variant_nulls = 0;
INSERT INTO t_variant_coalescing_insert VALUES (1, 'x'), (1, NULL);
SELECT 'keep nulls', v FROM t_variant_coalescing_insert;

DROP TABLE t_variant_coalescing_insert;
