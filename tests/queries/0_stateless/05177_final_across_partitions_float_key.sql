-- https://github.com/ClickHouse/ClickHouse/issues/116955
-- `enable_automatic_decision_for_merging_across_partitions_for_final` skips the cross-partition FINAL
-- merge when the partition expression only reads primary-key columns, on the assumption that equal
-- primary-key values give equal partition values. The FINAL comparator is coarser than value identity
-- for floats: `-0.0` equals `0.0` and every `NaN` bit pattern equals every other, and a partition
-- expression can tell them apart, so rows the comparator merges landed in different partitions.

DROP TABLE IF EXISTS t_final_neg_zero;
CREATE TABLE t_final_neg_zero (f Float64, v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(f) ORDER BY f;
INSERT INTO t_final_neg_zero VALUES (-0.0, 1);
INSERT INTO t_final_neg_zero VALUES (0.0, 2);
SELECT count() FROM t_final_neg_zero FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_neg_zero FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
SELECT count() FROM t_final_neg_zero FINAL SETTINGS do_not_merge_across_partitions_select_final = 1;
DROP TABLE t_final_neg_zero;

SELECT 'NaN payloads';
DROP TABLE IF EXISTS t_final_nan;
CREATE TABLE t_final_nan (f Float64, v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY reinterpretAsUInt64(f) ORDER BY f;
INSERT INTO t_final_nan VALUES (reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9218868437227405313))), 1);
INSERT INTO t_final_nan VALUES (reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))), 2);
SELECT count() FROM t_final_nan FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_nan FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
DROP TABLE t_final_nan;

SELECT 'a non-float primary key still skips the cross-partition merge';
DROP TABLE IF EXISTS t_final_int;
CREATE TABLE t_final_int (k UInt64, v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY intDiv(k, 10) ORDER BY k;
SYSTEM STOP MERGES t_final_int;
INSERT INTO t_final_int VALUES (1, 1);
INSERT INTO t_final_int VALUES (1, 2);
INSERT INTO t_final_int VALUES (11, 3);
INSERT INTO t_final_int VALUES (11, 4);
SELECT count() FROM t_final_int FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_int FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
-- Merging across partitions splits the parts into key ranges; the per-partition pipelines the
-- automatic decision builds do not.
SELECT count() FROM (EXPLAIN PIPELINE SELECT * FROM t_final_int FINAL
    SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
        max_threads = 4, max_final_threads = 4)
WHERE explain LIKE '%FilterSortedStreamByRange%';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT * FROM t_final_int FINAL
    SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
        max_threads = 4, max_final_threads = 4)
WHERE explain LIKE '%FilterSortedStreamByRange%';
DROP TABLE t_final_int;

SELECT 'a float primary key still deduplicates within a partition';
DROP TABLE IF EXISTS t_final_float_plan;
CREATE TABLE t_final_float_plan (f Float64, v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(f) ORDER BY f;
SYSTEM STOP MERGES t_final_float_plan;
INSERT INTO t_final_float_plan VALUES (1.0, 1);
INSERT INTO t_final_float_plan VALUES (1.0, 2);
INSERT INTO t_final_float_plan VALUES (2.0, 3);
INSERT INTO t_final_float_plan VALUES (2.0, 4);
SELECT count() FROM t_final_float_plan FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT max(v) FROM t_final_float_plan FINAL WHERE f = 1.0 SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
DROP TABLE t_final_float_plan;

SELECT 'a float partition column outside the primary key was already declined';
DROP TABLE IF EXISTS t_final_float_nonkey;
CREATE TABLE t_final_float_nonkey (k UInt64, f Float64, v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(f) ORDER BY k;
INSERT INTO t_final_float_nonkey VALUES (1, -0.0, 1);
INSERT INTO t_final_float_nonkey VALUES (1, 0.0, 2);
SELECT count() FROM t_final_float_nonkey FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_float_nonkey FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
DROP TABLE t_final_float_nonkey;

SELECT 'a float nested inside a key column is detected too';
-- The partition expression reads the whole tuple, so the required column keeps its declared
-- `Tuple(Float64, UInt8)` type and only the recursive walk over nested types can spot the float.
-- Reading `t.1` instead would surface a required column named `t.1`, which is not a primary key
-- column, and the check would already decline for that reason without exercising the walk.
DROP TABLE IF EXISTS t_final_tuple;
CREATE TABLE t_final_tuple (t Tuple(Float64, UInt8), v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(t) ORDER BY t;
INSERT INTO t_final_tuple VALUES ((-0.0, 1), 1);
INSERT INTO t_final_tuple VALUES ((0.0, 1), 2);
SELECT count() FROM t_final_tuple FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_tuple FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
DROP TABLE t_final_tuple;

-- The counts above stay correct even without the recursive walk, because the two values also end up
-- in one group once the merge happens. Probe the plan so the walk itself is load-bearing: merging
-- across partitions gives a single `ReplacingSorted` over all parts, while the automatic decision
-- builds one `ReplacingSorted` per partition. Dropping the walk turns the float answers into `2`.
DROP TABLE IF EXISTS t_final_tuple_plan;
CREATE TABLE t_final_tuple_plan (t Tuple(Float64, UInt8), v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(t) ORDER BY t;
SYSTEM STOP MERGES t_final_tuple_plan;
INSERT INTO t_final_tuple_plan VALUES ((1.0, 1), 1);
INSERT INTO t_final_tuple_plan VALUES ((1.0, 1), 2);
INSERT INTO t_final_tuple_plan VALUES ((2.0, 1), 3);
INSERT INTO t_final_tuple_plan VALUES ((2.0, 1), 4);
SELECT count() FROM (EXPLAIN PIPELINE SELECT * FROM t_final_tuple_plan FINAL
    SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
        max_threads = 4, max_final_threads = 4)
WHERE explain LIKE '%ReplacingSorted%';
DROP TABLE t_final_tuple_plan;

SELECT 'a tuple without a float still skips the cross-partition merge';
-- The negative control for the probe above: the same shape with no float passes the membership check
-- and keeps the automatic decision, so the whole-tuple required column is not what declines it.
DROP TABLE IF EXISTS t_final_tuple_int_plan;
CREATE TABLE t_final_tuple_int_plan (t Tuple(UInt64, UInt8), v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(t) ORDER BY t;
SYSTEM STOP MERGES t_final_tuple_int_plan;
INSERT INTO t_final_tuple_int_plan VALUES ((1, 1), 1);
INSERT INTO t_final_tuple_int_plan VALUES ((1, 1), 2);
INSERT INTO t_final_tuple_int_plan VALUES ((2, 1), 3);
INSERT INTO t_final_tuple_int_plan VALUES ((2, 1), 4);
SELECT count() FROM (EXPLAIN PIPELINE SELECT * FROM t_final_tuple_int_plan FINAL
    SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
        max_threads = 4, max_final_threads = 4)
WHERE explain LIKE '%ReplacingSorted%';
DROP TABLE t_final_tuple_int_plan;

SELECT 'an Array(Float64) key column is detected too';
DROP TABLE IF EXISTS t_final_array;
CREATE TABLE t_final_array (a Array(Float64), v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(a) ORDER BY a;
INSERT INTO t_final_array VALUES ([-0.0], 1);
INSERT INTO t_final_array VALUES ([0.0], 2);
SELECT count() FROM t_final_array FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count() FROM t_final_array FINAL SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 0;
DROP TABLE t_final_array;

DROP TABLE IF EXISTS t_final_array_plan;
CREATE TABLE t_final_array_plan (a Array(Float64), v UInt8) ENGINE = ReplacingMergeTree(v) PARTITION BY toString(a) ORDER BY a;
SYSTEM STOP MERGES t_final_array_plan;
INSERT INTO t_final_array_plan VALUES ([1.0], 1);
INSERT INTO t_final_array_plan VALUES ([1.0], 2);
INSERT INTO t_final_array_plan VALUES ([2.0], 3);
INSERT INTO t_final_array_plan VALUES ([2.0], 4);
SELECT count() FROM (EXPLAIN PIPELINE SELECT * FROM t_final_array_plan FINAL
    SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
        max_threads = 4, max_final_threads = 4)
WHERE explain LIKE '%ReplacingSorted%';
DROP TABLE t_final_array_plan;
