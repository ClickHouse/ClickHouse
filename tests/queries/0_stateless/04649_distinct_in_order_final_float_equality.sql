-- `DISTINCT` canonicalizes negative zero and otherwise keeps binary identity, so 0.0 and -0.0 collapse
-- into one row, while NaN values with different payloads survive as separate rows, as they do under
-- `GROUP BY` and `LIMIT BY`. The final DISTINCT over a stream sorted by a prefix of the distinct
-- columns used to deduplicate that prefix by comparison equality and collapse them into one row per
-- equality class, so the row count depended on which variant the plan picked. A Float column now stops the sorted prefix, and the hash variant -
-- which is the one that agrees with the rest of the engine - runs instead.

-- The sorting key differs from the DISTINCT columns, so the pre-distinct is hash-based in any case and
-- the final distinct above the ORDER BY f sort is the only processor that sees a sorted stream.
CREATE TABLE t_distinct_float (k UInt64, f Float64, b UInt8) ENGINE = MergeTree ORDER BY k;

-- 0.0 and -0.0; two NaNs with different payloads (0x7FF8000000000001 and 0x7FF8000000000002)
INSERT INTO t_distinct_float VALUES (1, 0., 1), (2, -0., 1), (3, reinterpretAsFloat64(toUInt64(9221120237041090561)), 1), (4, reinterpretAsFloat64(toUInt64(9221120237041090562)), 1);

-- Keep the inner ORDER BY under the aggregating outer queries below.
SET query_plan_remove_redundant_sorting = 0;
-- One stream, so pipeline processor names come without the "x N" suffix.
SET max_threads = 1;
-- The test checks which DISTINCT variant runs; keep the final one a plain `DistinctTransform` rather than
-- `ExternalDistinctTransform`, which is the same hash variant with spilling to disk.
SET max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0;

SELECT '-- hash DISTINCT canonicalizes the zeros and keeps the two NaN payloads apart';
SET optimize_distinct_in_order = 0;
SELECT count(), arraySort(groupArray(reinterpretAsUInt64(f))) FROM (SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f);

SET optimize_distinct_in_order = 1;

SELECT '-- a Float column keeps the sorted variant out of the plan: both distincts are hash ones';
SELECT arraySort(groupArray(trimLeft(explain))) FROM (EXPLAIN PIPELINE SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f) WHERE trimLeft(explain) IN ('DistinctSortedStreamTransform', 'DistinctTransform');

SELECT '-- the in-order plan agrees: one zero and both NaN payloads survive';
-- Which value of a class is printed first depends on the order the equal values leave the sort, so
-- pin the equality classes, not the binary payloads.
SELECT count(), arraySort(groupArray(if(isNaN(f), 'nan', 'zero'))) FROM (SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f);

DROP TABLE t_distinct_float;
