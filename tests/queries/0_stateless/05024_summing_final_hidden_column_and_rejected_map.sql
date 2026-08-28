-- A summed column that is invisible to both the query and the PREWHERE predicate still
-- decides whether the merge removes the row, so `FINAL` must read it anyway.

DROP TABLE IF EXISTS summing_final_hidden;
CREATE TABLE summing_final_hidden (a Int8, b Int32, note String, k UInt64) ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_hidden;

-- `a` sums to zero for key 1, `b` does not, so the row stays; for key 2 both sum to zero.
INSERT INTO summing_final_hidden VALUES (1, 5, 'x', 1), (1, 5, 'x', 2);
INSERT INTO summing_final_hidden VALUES (-1, 0, 'x', 1), (-1, -5, 'x', 2);

SELECT count() FROM summing_final_hidden FINAL PREWHERE note = 'x' SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_hidden FINAL PREWHERE note = 'x' SETTINGS enable_analyzer = 1;
SELECT k FROM summing_final_hidden FINAL PREWHERE note = 'x' ORDER BY k;

-- The FINAL read must agree with the state after a real merge.
SYSTEM START MERGES summing_final_hidden;
OPTIMIZE TABLE summing_final_hidden FINAL;
SELECT count() FROM summing_final_hidden;

DROP TABLE summing_final_hidden;

-- A nested table whose name ends with `Map` is only merged with `sumMap` when the whole
-- group is a valid map: at least two columns, an integer-like key and summable values.
-- The groups below are rejected by the merge, which merely copies them, so a `FINAL` read
-- must not read those arrays either. The read set is asserted on the pipeline instead of on
-- `read_bytes`, so that the check needs neither a large table nor a byte threshold.

DROP TABLE IF EXISTS summing_final_rejected_map;
CREATE TABLE summing_final_rejected_map
(
    k UInt64,
    s Int64,
    OnlyOneColumnMap Nested(ID UInt32),
    NonArithmeticValueMap Nested(ID UInt32, D Date)
)
ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_rejected_map;

INSERT INTO summing_final_rejected_map VALUES (1, 1, [1], [1], ['2020-01-01']);
INSERT INTO summing_final_rejected_map VALUES (1, 1, [1], [1], ['2020-01-01']);

-- Only `k` and `s` are read; neither rejected group appears in the pipeline. The read set is
-- asserted on `SELECT k` rather than on `SELECT count()`: a count reads the column that is
-- cheapest to read, which is chosen from the on-disk sizes and therefore depends on the
-- randomized `MergeTree` settings, not on the columns the merge requires.
SELECT count() FROM (EXPLAIN PIPELINE header = 1 SELECT k FROM summing_final_rejected_map FINAL) WHERE explain ILIKE '%Map.%';
SELECT count() FROM summing_final_rejected_map FINAL;

-- A valid map group, in contrast, participates in the summation and has to be read.
DROP TABLE IF EXISTS summing_final_valid_map;
CREATE TABLE summing_final_valid_map (k UInt64, s Int64, GoodMap Nested(ID UInt32, V UInt64)) ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_valid_map;

INSERT INTO summing_final_valid_map VALUES (1, 1, [1], [1]);
INSERT INTO summing_final_valid_map VALUES (1, 1, [1], [1]);

SELECT count() > 0 FROM (EXPLAIN PIPELINE header = 1 SELECT k FROM summing_final_valid_map FINAL) WHERE explain LIKE '%GoodMap.ID%';
SELECT count() > 0 FROM (EXPLAIN PIPELINE header = 1 SELECT k FROM summing_final_valid_map FINAL) WHERE explain LIKE '%GoodMap.V%';
SELECT count() FROM summing_final_valid_map FINAL;

DROP TABLE summing_final_rejected_map;
DROP TABLE summing_final_valid_map;

-- With `allow_tuple_element_aggregation`, the summed columns are the flattened tuple leaves.
-- A query that already reads the whole tuple must not additionally request its leaves: the
-- merge flattens the tuple on its own, so requesting both would read the same data twice.
DROP TABLE IF EXISTS summing_final_tuple;
CREATE TABLE summing_final_tuple (k UInt64, s Int64, tup Tuple(a Int64, b Int64))
ENGINE = SummingMergeTree ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
SYSTEM STOP MERGES summing_final_tuple;

-- For key 1 the tuple sums to zero but `s` does not, so the row stays; for key 2 all sum to zero.
INSERT INTO summing_final_tuple VALUES (1, 5, (3, 4)), (2, 5, (3, 4));
INSERT INTO summing_final_tuple VALUES (1, 5, (-3, -4)), (2, -5, (-3, -4));

SELECT count() FROM (EXPLAIN PIPELINE header = 1 SELECT tup FROM summing_final_tuple FINAL) WHERE explain LIKE '%tup.%';
SELECT k, s, tup FROM summing_final_tuple FINAL ORDER BY k;
SELECT tup FROM summing_final_tuple FINAL ORDER BY tup;
SELECT count() FROM summing_final_tuple FINAL;

SYSTEM START MERGES summing_final_tuple;
OPTIMIZE TABLE summing_final_tuple FINAL;
SELECT k, s, tup FROM summing_final_tuple ORDER BY k;

DROP TABLE summing_final_tuple;

-- The same holds for an intermediate tuple subcolumn: reading `tup.inner` already covers the
-- leaves below it, so `FINAL` must not request `tup.inner.c` and `tup.inner.d` on top of it.
DROP TABLE IF EXISTS summing_final_nested_tuple;
CREATE TABLE summing_final_nested_tuple (k UInt64, s Int64, tup Tuple(a Int64, inner Tuple(c Int64, d Int64)))
ENGINE = SummingMergeTree ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
SYSTEM STOP MERGES summing_final_nested_tuple;

-- For key 1 the inner tuple sums to zero but `tup.a` does not, so the row stays; for key 2 all sum to zero.
INSERT INTO summing_final_nested_tuple VALUES (1, 0, (7, (3, 4))), (2, 0, (7, (3, 4)));
INSERT INTO summing_final_nested_tuple VALUES (1, 0, (7, (-3, -4))), (2, 0, (-7, (-3, -4)));

SELECT count() FROM (EXPLAIN PIPELINE header = 1 SELECT tup.inner FROM summing_final_nested_tuple FINAL) WHERE explain LIKE '%tup.inner.%';
SELECT k, s, tup FROM summing_final_nested_tuple FINAL ORDER BY k;

SYSTEM START MERGES summing_final_nested_tuple;
OPTIMIZE TABLE summing_final_nested_tuple FINAL;
SELECT k, s, tup FROM summing_final_nested_tuple ORDER BY k;

DROP TABLE summing_final_nested_tuple;
