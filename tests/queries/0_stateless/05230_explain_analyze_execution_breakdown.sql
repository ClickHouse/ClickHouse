-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- The output of EXPLAIN ANALYZE carries timings and is non-deterministic, so the
-- test asserts only invariants of the `Execution` line of the query summary.

SET enable_analyzer = 1;

-- 1. The line is printed exactly once with `time = 1` and has three parts.
SELECT countIf(explain LIKE '  Execution:   in steps %· outside steps %· idle %') = 1
FROM (EXPLAIN ANALYZE time = 1 SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- 2. With `time = 0` the work intervals are not collected, so the line is not printed.
SELECT countIf(explain LIKE '%Execution:%') = 0
FROM (EXPLAIN ANALYZE time = 0 SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- 3. The three shares partition the execution time: they add up to 100% up to the rounding of
--    three values with two decimals, and each of them is within [0, 100].
--    The `in steps` part is the branch of the root step, so its share is printed with the same
--    text as the `branch` share of the first step of the plan.
WITH
    groupArray(explain) AS lines,
    arrayFirst(l -> l LIKE '%Execution:%', lines) AS execution_line,
    arrayFirst(l -> l LIKE '%Time: step%', lines) AS root_time_line,
    toFloat64(extract(execution_line, 'in steps [^(]*\\((\\d+\\.\\d+)%')) AS in_steps,
    toFloat64(extract(execution_line, 'outside steps [^(]*\\((\\d+\\.\\d+)%')) AS outside_steps,
    toFloat64(extract(execution_line, 'idle [^(]*\\((\\d+\\.\\d+)%')) AS idle
SELECT
    (in_steps + outside_steps + idle) BETWEEN 99.9 AND 100.1,
    least(in_steps, outside_steps, idle) >= 0,
    greatest(in_steps, outside_steps, idle) <= 100,
    extract(execution_line, 'in steps [^(]*\\((\\d+\\.\\d+%)') = extract(root_time_line, 'branch [^(]*\\((\\d+\\.\\d+%)')
FROM (EXPLAIN ANALYZE time = 1 SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);
