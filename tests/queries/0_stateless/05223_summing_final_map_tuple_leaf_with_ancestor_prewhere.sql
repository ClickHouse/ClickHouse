-- With `allow_tuple_element_aggregation` a tuple named `...Map` whose leaves are arrays is summed as
-- a `Nested ...Map` group, and `SummingSortedAlgorithm` classifies the arrays of a group positionally:
-- the first one is a key column. When the query outputs a value leaf of such a tuple while a deferred
-- filter (`PREWHERE` applied after `FINAL`, or a row policy) reads the whole tuple, the block that
-- reaches the merge holds both the tuple and the leaf, and the leaf has to come after its tuple:
-- otherwise the group flattens to `ratesMap.Value, ratesMap.ID, ratesMap.Value`, the value leaf
-- becomes a key, and the map is merged through the composite-key path with the wrong result.
-- The same pair of carriers is produced by a query that outputs the leaf and then the tuple.

DROP TABLE IF EXISTS summing_final_map_tuple;
CREATE TABLE summing_final_map_tuple (k UInt64, s Int64, ratesMap Tuple(ID Array(UInt64), Value Array(UInt64)))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
SYSTEM STOP MERGES summing_final_map_tuple;

-- Key 1: the same map key in both parts, so its values sum up.
-- Key 2: different map keys, so the merged map holds both.
INSERT INTO summing_final_map_tuple VALUES (1, 1, ([1], [1])), (2, 1, ([1], [1]));
INSERT INTO summing_final_map_tuple VALUES (1, 1, ([1], [1])), (2, 1, ([2], [5]));

SELECT '--- a value leaf in the output, the tuple in PREWHERE applied after FINAL';
SELECT k, ratesMap.Value FROM summing_final_map_tuple FINAL PREWHERE ratesMap != ([9], [9]) ORDER BY k SETTINGS apply_prewhere_after_final = 1;

SELECT '--- a value leaf in the output, the tuple in PREWHERE applied before FINAL';
SELECT k, ratesMap.Value FROM summing_final_map_tuple FINAL PREWHERE ratesMap != ([9], [9]) ORDER BY k SETTINGS apply_prewhere_after_final = 0;

SELECT '--- the key leaf in the output, the tuple in PREWHERE applied after FINAL';
SELECT k, ratesMap.ID FROM summing_final_map_tuple FINAL PREWHERE ratesMap != ([9], [9]) ORDER BY k SETTINGS apply_prewhere_after_final = 1;

SELECT '--- the tuple in the output, a value leaf in PREWHERE applied after FINAL';
SELECT k, ratesMap FROM summing_final_map_tuple FINAL PREWHERE ratesMap.Value != [9] ORDER BY k SETTINGS apply_prewhere_after_final = 1;

SELECT '--- a value leaf in the output, the tuple in a row policy';
CREATE ROW POLICY summing_final_map_tuple_policy ON summing_final_map_tuple USING ratesMap != ([9], [9]) TO ALL;
SELECT k, ratesMap.Value FROM summing_final_map_tuple FINAL ORDER BY k;
DROP ROW POLICY summing_final_map_tuple_policy ON summing_final_map_tuple;

-- The same pair of carriers, both in the output, in both orders.
SELECT '--- a value leaf and then the tuple in the output';
SELECT k, ratesMap.Value, ratesMap FROM summing_final_map_tuple FINAL ORDER BY k;
SELECT '--- the tuple and then a value leaf in the output';
SELECT k, ratesMap, ratesMap.Value FROM summing_final_map_tuple FINAL ORDER BY k;
SELECT '--- the key leaf and then the tuple in the output';
SELECT k, ratesMap.ID, ratesMap FROM summing_final_map_tuple FINAL ORDER BY k;

-- The merge has to receive the tuple ahead of its leaf: in the header of the reading step, the
-- tuple has to be listed before the leaf.
SELECT '--- the tuple comes before its leaf in the header of the reading step';
WITH
    (
        SELECT groupArray(explain) FROM
        (
            EXPLAIN PIPELINE header = 1
            SELECT k, ratesMap.Value FROM summing_final_map_tuple FINAL PREWHERE ratesMap != ([9], [9]) SETTINGS apply_prewhere_after_final = 1
        )
    ) AS pipeline,
    arraySlice(pipeline, arrayFirstIndex(line -> line LIKE '%MergeTreeSelect%', pipeline)) AS reading_step
SELECT
    arrayFirstIndex(line -> line LIKE '%ratesMap Tuple(%', reading_step) > 0
    AND arrayFirstIndex(line -> line LIKE '%ratesMap Tuple(%', reading_step) < arrayFirstIndex(line -> line LIKE '%ratesMap.Value Array(%', reading_step);

-- Every `FINAL` read above must agree with the state a real merge leaves behind.
SELECT '--- the state after a real merge';
SYSTEM START MERGES summing_final_map_tuple;
OPTIMIZE TABLE summing_final_map_tuple FINAL;
SELECT k, s, ratesMap FROM summing_final_map_tuple ORDER BY k;

DROP TABLE summing_final_map_tuple;
