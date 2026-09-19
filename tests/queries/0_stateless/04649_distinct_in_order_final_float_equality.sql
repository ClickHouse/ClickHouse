-- The final DISTINCT over a stream sorted by a prefix of the distinct columns deduplicates the
-- sorted prefix by comparison equality: Float values that compare equal (0.0 and -0.0, all NaN
-- payloads) collapse into one row per equality class, matching IEEE 754 and LIMIT BY. The
-- hash-based DISTINCT canonicalizes negative zero and so agrees on the zeros, but it keeps binary
-- identity for NaN, so NaN values with different payloads survive as separate rows.

-- The sorting key differs from the DISTINCT columns, so the pre-distinct stays hash-based and the
-- final distinct above the ORDER BY f sort is the only processor that sees a sorted stream.
CREATE TABLE t_distinct_float (k UInt64, f Float64, b UInt8) ENGINE = MergeTree ORDER BY k;

-- 0.0 and -0.0; two NaNs with different payloads (0x7FF8000000000001 and 0x7FF8000000000002)
INSERT INTO t_distinct_float VALUES (1, 0., 1), (2, -0., 1), (3, reinterpretAsFloat64(toUInt64(9221120237041090561)), 1), (4, reinterpretAsFloat64(toUInt64(9221120237041090562)), 1);

-- Keep the inner ORDER BY under the aggregating outer queries below.
SET query_plan_remove_redundant_sorting = 0;
-- One stream, so pipeline processor names come without the "x N" suffix.
SET max_threads = 1;

SELECT '-- hash DISTINCT canonicalizes the zeros and keeps the two NaN payloads apart';
SET optimize_distinct_in_order = 0;
SELECT count(), arraySort(groupArray(reinterpretAsUInt64(f))) FROM (SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f);

SET optimize_distinct_in_order = 1;

SELECT '-- the in-order plan: final sorted distinct above the sort, hash pre-distinct below it';
SELECT arraySort(groupArray(trimLeft(explain))) FROM (EXPLAIN PIPELINE SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f) WHERE trimLeft(explain) IN ('DistinctSortedStreamTransform', 'DistinctTransform');

SELECT '-- final sorted distinct groups the Float prefix by comparison equality: one row per class';
-- Which representative survives (0.0 or -0.0, which NaN payload) depends on the order the equal
-- values leave the sort, so pin the equality classes, not the binary payloads.
SELECT count(), arraySort(groupArray(if(isNaN(f), 'nan', 'zero'))) FROM (SELECT DISTINCT f, b FROM t_distinct_float ORDER BY f);

DROP TABLE t_distinct_float;
