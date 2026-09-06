-- A `PREWHERE` consumes the columns its predicate reads, so a column that the query itself does
-- not output is dropped from the block once the filtering is done. The columns a Summing merge
-- aggregates have to be asked back, otherwise the merge decides the removal of a row from a
-- subset of them.
--
-- With `allow_tuple_element_aggregation` the aggregated columns are the flattened tuple leaves,
-- but the column that carries them into the merge can be a tuple ancestor: when the read already
-- fetches `tup.inner`, the leaves below it are not requested on top of that (the merge flattens
-- the ancestor again on its own). Such an ancestor is not in the flattened list of aggregated
-- columns, so it has to be kept by the carrier it actually is.

DROP TABLE IF EXISTS summing_final_tuple_prewhere;
CREATE TABLE summing_final_tuple_prewhere (k UInt64, s Int64, tup Tuple(a Int64, inner Tuple(c Int64, d Int64)))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
SYSTEM STOP MERGES summing_final_tuple_prewhere;

-- For key 1 only the inner tuple sums to a non-zero value, so a real merge keeps that row.
-- For key 2 every aggregated column sums to zero, so a real merge removes it.
INSERT INTO summing_final_tuple_prewhere VALUES (1, 1, (1, (5, 0))), (2, 1, (1, (5, 0)));
INSERT INTO summing_final_tuple_prewhere VALUES (1, -1, (-1, (5, 0))), (2, -1, (-1, (-5, 0)));

SELECT '--- an intermediate tuple subcolumn in PREWHERE';
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) SETTINGS enable_analyzer = 1;
SELECT k FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) ORDER BY k SETTINGS enable_analyzer = 1;

SELECT '--- the whole tuple in PREWHERE';
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE tup != (9, (9, 9)) SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE tup != (9, (9, 9)) SETTINGS enable_analyzer = 1;

-- Under `FINAL` the optimizer moves a condition into `PREWHERE` only when it is over the sorting
-- key (`MergeTreeWhereOptimizer`), so a tuple ancestor reaches `PREWHERE` only when the query
-- writes one. The moved shape must keep the aggregated columns just the same, and the shape that
-- is not eligible for the move has to be right whether it is moved or not.
SELECT '--- WHERE with optimize_move_to_prewhere_if_final';
SELECT k, s FROM summing_final_tuple_prewhere FINAL WHERE k != 9 ORDER BY k
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, enable_analyzer = 0;
SELECT k, s FROM summing_final_tuple_prewhere FINAL WHERE k != 9 ORDER BY k
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, enable_analyzer = 1;
SELECT count() FROM summing_final_tuple_prewhere FINAL WHERE tup.inner != (9, 9)
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, enable_analyzer = 0;
SELECT count() FROM summing_final_tuple_prewhere FINAL WHERE tup.inner != (9, 9)
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, enable_analyzer = 1;

-- The subcolumn in the predicate is covered by the whole tuple in the output, which is the only
-- carrier the merge needs: the leaves must not arrive through both of them at once.
SELECT '--- the tuple is also in the output';
SELECT k, tup FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) ORDER BY k SETTINGS enable_analyzer = 0;
SELECT k, tup FROM summing_final_tuple_prewhere FINAL PREWHERE tup.inner != (9, 9) ORDER BY k SETTINGS enable_analyzer = 1;

-- A summed column that the predicate itself reads has to come back as well.
SELECT '--- a summed column in PREWHERE';
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE s != 9 SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_tuple_prewhere FINAL PREWHERE s != 9 SETTINGS enable_analyzer = 1;

-- Every `FINAL` read above must agree with the state a real merge leaves behind.
SELECT '--- the state after a real merge';
SYSTEM START MERGES summing_final_tuple_prewhere;
OPTIMIZE TABLE summing_final_tuple_prewhere FINAL;
SELECT count() FROM summing_final_tuple_prewhere;
SELECT k, s, tup FROM summing_final_tuple_prewhere ORDER BY k;

DROP TABLE summing_final_tuple_prewhere;
