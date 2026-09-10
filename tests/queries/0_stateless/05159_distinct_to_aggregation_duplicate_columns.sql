SET query_plan_convert_distinct_to_aggregation = 1;
SET query_plan_remove_redundant_distinct = 0;
SET max_threads = 2;

-- Repeated projections of the same input or computed expression allow aggregation.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number, number FROM numbers(10));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number + 1 AS x, number + 1 AS x FROM numbers(10));

-- Ordinary and adaptive aggregation preserve the repeated output columns.
SET enable_adaptive_aggregator = 0;
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number, number FROM numbers(3));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number + 1 AS x, number + 1 AS x FROM numbers(3));
SET enable_adaptive_aggregator = 1;
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number, number FROM numbers(3));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number + 1 AS x, number + 1 AS x FROM numbers(3));

-- A union can give different positional values the same output name, so it retains `DISTINCT`.
SET max_threads = 1;
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT number AS x, number AS x FROM numbers(1)
      UNION DISTINCT SELECT number + 10, number + 20 FROM numbers(1));
SELECT number AS x, number AS x FROM numbers(1)
UNION DISTINCT SELECT number + 10, number + 20 FROM numbers(1);
