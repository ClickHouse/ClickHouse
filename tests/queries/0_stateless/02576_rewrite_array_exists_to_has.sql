set enable_analyzer = true;

set optimize_rewrite_array_exists_to_has = false;
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);

set optimize_rewrite_array_exists_to_has = true;
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN QUERY TREE run_passes = 1  select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);

set enable_analyzer = false;

set optimize_rewrite_array_exists_to_has = false;
EXPLAIN SYNTAX select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN SYNTAX select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);

set optimize_rewrite_array_exists_to_has = true;
EXPLAIN SYNTAX select arrayExists(x -> x = 5 , materialize(range(10))) from numbers(10);
EXPLAIN SYNTAX select arrayExists(x -> 5 = x , materialize(range(10))) from numbers(10);
