SET max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- With a single input thread, `groupArrayInsertAt` keeps the first value at each
-- position. Removing the subquery's `ORDER BY` must not change which value wins.
SELECT 'ascending', groupArrayInsertAt(number, 0)
FROM numbers(3);

SELECT 'descending', groupArrayInsertAt(number, 0)
FROM (SELECT number FROM numbers(3) ORDER BY number DESC);

-- Verify that sort removal is active for an order-independent aggregate.
SELECT countIf(explain LIKE '%Sorting%') AS sorting_steps
FROM (EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT sum(number)
      FROM (SELECT number FROM numbers(3) ORDER BY number DESC));
