-- Aggregate over Array(Nothing) (all-NULL arrays) collapses the nested aggregate to a
-- zero-byte state. Growing the -ForEach state array used to call the nested merge() with
-- new_state == old_state (a zero-byte arena allocation does not advance), tripping the
-- self-aliasing assertion. These must not abort and must return the right results.

SELECT arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]) LIMIT -432;
SELECT arrayReduce('groupArrayStateForEach', [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduceInRanges('uniqStateForEach', [(1, 2)], [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduce('uniqForEach', [[NULL], [NULL, NULL, NULL]]);

-- Non-zero-size nested states must still migrate correctly when the -ForEach array grows.
SELECT arrayReduce('sumForEach', [[1, 2], [3, 4, 5], [6, 7]]);
SELECT arrayReduce('uniqForEach', [[1], [1, 2, 3], [5, 5]]);
SELECT arrayReduce('groupArrayForEach', [[1], [2, 3], [4, 5, 6]]);
