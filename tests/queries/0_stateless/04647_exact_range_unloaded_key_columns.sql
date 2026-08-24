-- Exact-range primary key analysis must not assume that the in-memory primary index resolves every key
-- column the filter references. A part may keep only a prefix of the key in memory (see the `MergeTree`
-- setting `primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns`); the remaining key columns
-- are then analysed as constant coordinates, so a granule strictly inside the binary-search boundary can
-- still hold rows the filter rejects even though the condition itself describes one continuous key range.
--
-- This used to trip the debug-only `Inconsistent KeyCondition behavior` logical error thrown from
-- `MergeTreeDataSelectExecutor::markRangesFromPKRange`, aborting the server on a plain
-- `WHERE a = 5 AND b = 3` - no monotonic function and no reverse-sorted key column involved.

SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_exact_loaded;
DROP TABLE IF EXISTS t_exact_unloaded;

-- `primary_key_lazy_load` and `use_primary_key_cache` are pinned only so that
-- `system.parts.primary_key_bytes_in_memory`, read at the end of the test, reports the index of the part
-- itself rather than 0.
CREATE TABLE t_exact_loaded (a UInt16, b UInt16, c UInt16)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 4, primary_key_lazy_load = 0, use_primary_key_cache = 0;

CREATE TABLE t_exact_unloaded (a UInt16, b UInt16, c UInt16)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 4, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.01,
         primary_key_lazy_load = 0, use_primary_key_cache = 0;

INSERT INTO t_exact_loaded SELECT intDiv(number, 100), intDiv(number % 100, 10), number % 10 FROM numbers(1000);
INSERT INTO t_exact_unloaded SELECT intDiv(number, 100), intDiv(number % 100, 10), number % 10 FROM numbers(1000);
OPTIMIZE TABLE t_exact_loaded FINAL;
OPTIMIZE TABLE t_exact_unloaded FINAL;

SELECT 'point condition on an unloaded key column';
SELECT count() FROM t_exact_unloaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT countIf(intDiv(number, 100) = 5 AND intDiv(number % 100, 10) = 3) FROM numbers(1000);

SELECT 'redundant range on the loaded key column added on top';
SELECT count() FROM t_exact_unloaded WHERE (a = 5 AND b = 3) AND a <= 100 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded WHERE (a = 5 AND b = 3) AND a <= 100 SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT 'point conditions on every key column, only the first one loaded';
SELECT count() FROM t_exact_unloaded WHERE a = 5 AND b = 3 AND c = 7 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded WHERE a = 5 AND b = 3 AND c = 7 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT countIf(intDiv(number, 100) = 5 AND intDiv(number % 100, 10) = 3 AND number % 10 = 7) FROM numbers(1000);

SELECT 'condition confined to the loaded key column';
SELECT count() FROM t_exact_unloaded WHERE a = 5 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded WHERE a = 5 SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT 'the whole key stays in memory without the setting, and the results agree';
SELECT count() FROM t_exact_loaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_loaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- Guards the premise of the test: the queries above are only interesting while the suffix key columns
-- really are missing from the in-memory index of `t_exact_unloaded`.
SELECT 'suffix key columns skipped in the in-memory index';
SELECT
    (SELECT sum(primary_key_bytes_in_memory) FROM system.parts
     WHERE database = currentDatabase() AND table = 't_exact_unloaded' AND active)
    < (SELECT sum(primary_key_bytes_in_memory) FROM system.parts
       WHERE database = currentDatabase() AND table = 't_exact_loaded' AND active);

DROP TABLE t_exact_loaded;
DROP TABLE t_exact_unloaded;

-- The same shape with a reverse-sorted unloaded key column, as hit by the stress test.
DROP TABLE IF EXISTS t_exact_unloaded_rev;
CREATE TABLE t_exact_unloaded_rev (a UInt16, b UInt16, c UInt16)
ENGINE = MergeTree ORDER BY (a, b DESC, c)
SETTINGS index_granularity = 4, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.01;
INSERT INTO t_exact_unloaded_rev SELECT intDiv(number, 100), 9 - intDiv(number % 100, 10), number % 10 FROM numbers(1000);
OPTIMIZE TABLE t_exact_unloaded_rev FINAL;

SELECT 'reverse-sorted unloaded key column';
SELECT count() FROM t_exact_unloaded_rev WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded_rev WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_exact_unloaded_rev WHERE (a = 5 AND b = 3) AND a <= 100 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_exact_unloaded_rev WHERE (a = 5 AND b = 3) AND a <= 100 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT countIf(intDiv(number, 100) = 5 AND 9 - intDiv(number % 100, 10) = 3) FROM numbers(1000);

DROP TABLE t_exact_unloaded_rev;
