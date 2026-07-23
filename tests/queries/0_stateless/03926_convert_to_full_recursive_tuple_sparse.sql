-- Tags: no-random-merge-tree-settings

-- Reproduces assertion failure in Set::appendSetElements when ColumnTuple has
-- inner sparse columns from one MergeTree part and non-sparse from another.
-- The fix: IColumn::convertToFullIfNeeded now recursively converts subcolumns.
--
-- The bug path: Set::appendSetElements is called only when fill_set_elements is
-- true, which happens when KeyCondition calls buildOrderedSetInplace for index
-- evaluation. So the IN column must be part of the ORDER BY key.
--
-- The assertion fires when the non-sparse chunk is read first (into set_elements)
-- and the sparse chunk is inserted second, because ColumnVector::insertRangeFrom
-- asserts typeid equality with the source.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_sparse_tuple;

-- ORDER BY val is essential: it makes KeyCondition call buildOrderedSetInplace
-- for the IN expression, which triggers fillSetElements and appendSetElements.
CREATE TABLE t_sparse_tuple (key UInt64, val Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY val
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_sparse_tuple;

-- Part 1 (all_1_1_0): non-sparse inner columns (no defaults in second element).
-- This part is read first into set_elements, so set_elements uses ColumnVector.
INSERT INTO t_sparse_tuple SELECT number, (number + 1, number + 1) FROM numbers(100);

-- Part 2 (all_2_2_0): second tuple element is all zeros → sparse serialization.
-- When this chunk is inserted into the Set, insertRangeFrom sees ColumnSparse
-- source vs ColumnVector destination, triggering the assertion.
INSERT INTO t_sparse_tuple SELECT number + 200, (number + 200, 0) FROM numbers(100);

-- Building a Set from a subquery that reads both parts triggers the bug.
-- val must be in ORDER BY for KeyCondition to use buildOrderedSetInplace.
SELECT count() FROM t_sparse_tuple WHERE val IN (SELECT val FROM t_sparse_tuple);

DROP TABLE t_sparse_tuple;
