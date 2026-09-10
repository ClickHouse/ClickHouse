SET max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- Without the `is_order_dependent` property, `removeRedundantSorting` drops the
-- descending sort and `groupArrayInsertAt` keeps 0 instead of 2 at position zero.
-- No grouping key or dictionary sharding can affect this case.
SELECT groupArrayInsertAt(number, 0)
FROM (SELECT number FROM numbers(3) ORDER BY number DESC);

-- Confirm that sorting removal is active for an order-independent aggregate.
SELECT countIf(explain LIKE '%Sorting%') AS sorting_steps
FROM (EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT sum(number)
      FROM (SELECT number FROM numbers(3) ORDER BY number DESC));
