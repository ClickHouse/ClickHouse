SET max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- One-sample `SGD` batches make training depend on sample order. A zero feature
-- leaves only the bias to train: targets -1 then 1 produce a positive bias,
-- while targets 1 then -1 produce a negative bias for both functions.
-- Test them separately so one function's metadata cannot preserve the other's sort.
SELECT 'linear ascending',
    stochasticLinearRegression(0.5, 0, 1, 'SGD')(if(number = 0, -1, 1), 0)[2] > 0
FROM numbers(2);

SELECT 'linear descending',
    stochasticLinearRegression(0.5, 0, 1, 'SGD')(if(number = 0, -1, 1), 0)[2] < 0
FROM (SELECT number FROM numbers(2) ORDER BY number DESC);

SELECT 'logistic ascending',
    stochasticLogisticRegression(1, 0, 1, 'SGD')(if(number = 0, -1, 1), 0)[2] > 0
FROM numbers(2);

SELECT 'logistic descending',
    stochasticLogisticRegression(1, 0, 1, 'SGD')(if(number = 0, -1, 1), 0)[2] < 0
FROM (SELECT number FROM numbers(2) ORDER BY number DESC);

-- Confirm that sorting removal is active for an order-independent aggregate.
SELECT countIf(explain LIKE '%Sorting%') AS sorting_steps
FROM (EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT sum(number)
      FROM (SELECT number FROM numbers(2) ORDER BY number DESC));
