set allow_experimental_analyzer = 1;

SELECT  'set optimize_duplicate_order_by_and_distinct = 1';
set optimize_duplicate_order_by_and_distinct = 1;

EXPLAIN QUERY TREE SELECT DISTINCT number
FROM
(
   SELECT DISTINCT number
   FROM numbers(3)
);

EXPLAIN QUERY TREE SELECT DISTINCT t
FROM
(
   SELECT DISTINCT number as t
   FROM numbers(3)
);


EXPLAIN QUERY TREE SELECT DISTINCT number
FROM
(
   SELECT number
   FROM
   (
       SELECT DISTINCT number
       FROM numbers(3)
   )
);


SELECT  'set optimize_duplicate_order_by_and_distinct = 0';
set optimize_duplicate_order_by_and_distinct = 0;

EXPLAIN QUERY TREE SELECT DISTINCT number
FROM
(
   SELECT DISTINCT number
   FROM numbers(3)
);

EXPLAIN QUERY TREE SELECT DISTINCT t
FROM
(
   SELECT DISTINCT number as t
   FROM numbers(3)
);


EXPLAIN QUERY TREE SELECT DISTINCT number
FROM
(
   SELECT number
   FROM
   (
       SELECT DISTINCT number
       FROM numbers(3)
   )
);
