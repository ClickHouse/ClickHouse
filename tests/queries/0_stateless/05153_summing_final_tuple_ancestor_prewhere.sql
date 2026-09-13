-- With `allow_tuple_element_aggregation` the columns a Summing merge aggregates are the flattened
-- tuple leaves, and the column that carries a leaf into the merge can be a tuple ancestor. When the
-- query outputs a tuple subcolumn and its ancestor is read by `PREWHERE` (or by a row policy), the
-- ancestor is the only carrier of the leaves outside the subcolumn, so it has to reach the merge:
-- otherwise the merge decides the removal of a row from a subset of the aggregated columns.
--
-- The block that reaches the merge then holds both the ancestor and the subcolumn, and the merge
-- flattens the leaves below the subcolumn through both of them. That is the same shape a query
-- reading a tuple and its subcolumn at once produces without any of this, and it is summed per
-- carrier, so the result stays correct - this test pins that down.

DROP TABLE IF EXISTS summing_final_tuple_ancestor;
CREATE TABLE summing_final_tuple_ancestor (k UInt64, s Int64, tup Tuple(a Int64, inner Tuple(c Int64, d Int64)))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
SYSTEM STOP MERGES summing_final_tuple_ancestor;

-- Key 1: every aggregated column sums to a non-zero value, so a real merge keeps the row.
-- Key 2: every aggregated column sums to zero, so a real merge removes the row.
-- Key 3: only `tup.a` sums to a non-zero value, so a real merge keeps the row - and `tup.a` is
--        reachable only through the `tup` ancestor when the query outputs `tup.inner` alone.
INSERT INTO summing_final_tuple_ancestor VALUES (1, 1, (1, (5, 3))), (2, 1, (1, (5, 3))), (3, 1, (7, (5, 3)));
INSERT INTO summing_final_tuple_ancestor VALUES (1, 2, (2, (5, 3))), (2, -1, (-1, (-5, -3))), (3, -1, (0, (-5, -3)));

SELECT '--- an output subcolumn with the ancestor in PREWHERE';
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL PREWHERE tup != (9, (9, 9)) ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL PREWHERE tup != (9, (9, 9)) ORDER BY k SETTINGS enable_analyzer = 1;

SELECT '--- an output leaf with the ancestor in PREWHERE';
SELECT k, tup.inner.c FROM summing_final_tuple_ancestor FINAL PREWHERE tup != (9, (9, 9)) ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup.inner.c FROM summing_final_tuple_ancestor FINAL PREWHERE tup != (9, (9, 9)) ORDER BY k SETTINGS enable_analyzer = 1;

-- The same shape without a predicate: here no ancestor is read, so every leaf outside the output
-- subcolumn is requested on its own and the carriers do not overlap.
SELECT '--- an output subcolumn without a predicate';
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 1;

-- A query asking for a tuple and its subcolumn at once produces the same overlapping pair of
-- carriers on its own, with no help from the `FINAL` read set. It is the control for the shape above.
SELECT '--- a tuple and its subcolumn in the output';
SELECT k, tup, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 1;

SELECT '--- an output subcolumn with the ancestor in a row policy';
CREATE ROW POLICY summing_final_tuple_ancestor_policy ON summing_final_tuple_ancestor USING tup != (9, (9, 9)) TO ALL;
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup.inner FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 1;
SELECT k, tup.inner.c FROM summing_final_tuple_ancestor FINAL ORDER BY k SETTINGS enable_analyzer = 1;
DROP ROW POLICY summing_final_tuple_ancestor_policy ON summing_final_tuple_ancestor;

-- Every `FINAL` read above must agree with the state a real merge leaves behind.
SELECT '--- the state after a real merge';
SYSTEM START MERGES summing_final_tuple_ancestor;
OPTIMIZE TABLE summing_final_tuple_ancestor FINAL;
SELECT k, s, tup FROM summing_final_tuple_ancestor ORDER BY k;

DROP TABLE summing_final_tuple_ancestor;
