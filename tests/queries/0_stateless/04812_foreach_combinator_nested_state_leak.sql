-- When the -ForEach array grows, ensureAggregateData creates fresh nested states and merges
-- the old ones into them, and the old states used to be abandoned without being destroyed.
-- A quantilesExact state keeps 5 values inline and allocates past that, so an element must
-- take at least 6 values before a later, longer row regrows the array. Under a leak
-- sanitizer these queries reported the abandoned allocations.

-- add() allocation site: 8 rows of length 1 spill element 0 to the heap, then a length-2 row regrows.
SELECT quantilesExactForEach(0.5)(arr) FROM (SELECT arrayMap(x -> number + x, range(if(number < 8, 1, 2))) AS arr FROM numbers(16));

-- Repeated regrows also abandon states allocated by the migration merge itself.
SELECT quantilesExactForEach(0.5)(arr) FROM (SELECT arrayMap(x -> number + x, range(intDiv(number, 8) + 2)) AS arr FROM numbers(64));

-- Same through GROUP BY, so every key's state regrows independently.
SELECT k, quantilesExactForEach(0.5)(arr)
FROM (SELECT number % 2 AS k, arrayMap(x -> number + x, range(intDiv(number, 8) + 1)) AS arr FROM numbers(32))
GROUP BY k ORDER BY k;

-- State-state merge path: arrayReduceInRanges pre-aggregates one -ForEach state per 64 rows
-- and merges those states, so ensureAggregateData regrows from mergeImpl rather than from add.
SELECT arrayReduceInRanges('quantilesExactForEach(0.5)', [(1, 200)], arrayMap(x -> arrayMap(y -> y, range(intDiv(x, 20) + 1)), range(200)));

-- Results must be unchanged: the migrated values are still all present after the fix.
SELECT arrayReduce('quantilesExactForEach(0.5)', [[1], [1, 2, 3], [5, 5]]);
SELECT arrayReduce('sumForEach', [[1, 2], [3, 4, 5], [6, 7]]);
