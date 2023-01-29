set optimize_rewrite_aggregate_function_with_if = false;

explain syntax select sum(if(number % 2, number, 0)) from numbers(100);
explain syntax select sum(if(number % 2, 0, number)) from numbers(100);

explain syntax select sum(if(number % 2, number, null)) from numbers(100);
explain syntax select sum(if(number % 2, null, number)) from numbers(100);

explain syntax select avg(if(number % 2, number, null)) from numbers(100);
explain syntax select avg(if(number % 2, null, number)) from numbers(100);

explain syntax select quantiles(0.5, 0.9, 0.99)(if(number % 2, number, null)) from numbers(100);
explain syntax select quantiles(0.5, 0.9, 0.99)(if(number % 2, null, number)) from numbers(100);

set optimize_rewrite_aggregate_function_with_if = true;

explain syntax select sum(if(number % 2, number, 0)) from numbers(100);
explain syntax select sum(if(number % 2, 0, number)) from numbers(100);

explain syntax select sum(if(number % 2, number, null)) from numbers(100);
explain syntax select sum(if(number % 2, null, number)) from numbers(100);

explain syntax select avg(if(number % 2, number, null)) from numbers(100);
explain syntax select avg(if(number % 2, null, number)) from numbers(100);

explain syntax select quantiles(0.5, 0.9, 0.99)(if(number % 2, number, null)) from numbers(100);
explain syntax select quantiles(0.5, 0.9, 0.99)(if(number % 2, null, number)) from numbers(100);

set allow_experimental_analyzer = true;

EXPLAIN QUERY TREE run_passes = 1 select sum(if(number % 2, number, 0)) from numbers(100);
EXPLAIN QUERY TREE run_passes = 1 select sum(if(number % 2, 0, number)) from numbers(100);

EXPLAIN QUERY TREE run_passes = 1 select sum(if(number % 2, number, null)) from numbers(100);
EXPLAIN QUERY TREE run_passes = 1 select sum(if(number % 2, null, number)) from numbers(100);

EXPLAIN QUERY TREE run_passes = 1 select avg(if(number % 2, number, null)) from numbers(100);
EXPLAIN QUERY TREE run_passes = 1 select avg(if(number % 2, null, number)) from numbers(100);

EXPLAIN QUERY TREE run_passes = 1 select quantiles(0.5, 0.9, 0.99)(if(number % 2, number, null)) from numbers(100);
EXPLAIN QUERY TREE run_passes = 1 select quantiles(0.5, 0.9, 0.99)(if(number % 2, null, number)) from numbers(100);
