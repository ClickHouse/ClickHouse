-- Aggregate over Array(Nothing) (all-NULL arrays) collapses the nested aggregate to a
-- zero-byte state. A zero-byte arena allocation does not advance the arena, so distinct
-- nested states alias the same address and merge(state, state) tripped the self-aliasing
-- assertion. These must not abort and must return the right results.

-- Migration merge path (ensureAggregateData grows the -ForEach array).
SELECT arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]) LIMIT -432;
SELECT arrayReduce('groupArrayStateForEach', [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduceInRanges('uniqStateForEach', [(1, 2)], [[NULL], [NULL, NULL, NULL]]);
SELECT arrayReduce('uniqForEach', [[NULL], [NULL, NULL, NULL]]);

-- mergeImpl merge path: arrayReduceInRanges pre-aggregates one -ForEach state per 64 rows
-- (minimum_step) then merges those states. With > 128 all-NULL rows it performs a real
-- state-state merge of two zero-size nested states (which alias the same arena slot).
SELECT arrayReduceInRanges('uniqStateForEach', [(1, 200)], arrayMap(x -> [NULL], range(200)));
SELECT arrayReduceInRanges('uniqStateForEach', [(1, 200)], arrayMap(x -> [NULL, NULL], range(200)));
SELECT arrayReduceInRanges('groupArrayStateForEach', [(1, 200)], arrayMap(x -> [NULL], range(200)));
SELECT arrayReduceInRanges('uniqForEach', [(1, 200)], arrayMap(x -> [NULL], range(200)));

-- Non-zero-size nested states must still migrate and merge correctly when the -ForEach array grows.
SELECT arrayReduce('sumForEach', [[1, 2], [3, 4, 5], [6, 7]]);
SELECT arrayReduce('uniqForEach', [[1], [1, 2, 3], [5, 5]]);
SELECT arrayReduce('groupArrayForEach', [[1], [2, 3], [4, 5, 6]]);
-- Non-zero-size state through the arrayReduceInRanges pre-aggregation merge path.
SELECT arrayReduceInRanges('sumForEach', [(1, 200)], arrayMap(x -> [1, 2], range(200)));
