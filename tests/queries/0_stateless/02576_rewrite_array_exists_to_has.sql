SET enable_analyzer = 1;

set optimize_rewrite_array_exists_to_has = false;
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);

set optimize_rewrite_array_exists_to_has = true;
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);

-- Verify that the optimization is not applied when types are incompatible (Date vs String).
-- arrayExists with lambda can compare Date and String via implicit conversion in equals,
-- but has() requires a common supertype which doesn't exist for Date and String.
SELECT arrayExists(date -> (date = '2022-07-31'), [toDate('2022-07-31')]) AS date_exists;
