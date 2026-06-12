SET enable_analyzer = 1;
SET optimize_rewrite_sum_if_to_count_if = 1;

EXPLAIN QUERY TREE (SELECT sumIf(1, (number % 2) == 0) FROM numbers(10));

SELECT '--';

SELECT sumIf(1, (number % 2) == 0) FROM numbers(10);

SELECT '--';

EXPLAIN QUERY TREE (SELECT sum(if((number % 2) == 0, 1, 0)) FROM numbers(10));

SELECT '--';

SELECT sum(if((number % 2) == 0, 1, 0)) FROM numbers(10);

SELECT '--';

EXPLAIN QUERY TREE (SELECT sum(if((number % 2) == 0, 0, 1)) FROM numbers(10));

SELECT '--';

SELECT sum(if((number % 2) == 0, 0, 1)) FROM numbers(10);
