-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- The output of EXPLAIN ANALYZE carries timings and is non-deterministic, so the
-- test asserts only invariants of the `Time` and `Concurrency` lines printed with `time = 1`.

SET enable_analyzer = 1;

-- 1. The step is a subset of its branch, so the step share never exceeds the branch share.
SELECT countIf(
    toFloat64(extract(explain, 'step [^(]*\\((\\d+\\.\\d+)%')) >
    toFloat64(extract(explain, 'branch [^(]*\\((\\d+\\.\\d+)%'))) = 0
FROM (EXPLAIN ANALYZE time = 1 SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k)
WHERE explain LIKE '%Time: step%';

-- 2. The denominator is `max_threads`, and the concurrency never exceeds it.
--    A step that owns no work prints `Unknown` instead of a fraction.
SELECT
    countIf(explain NOT LIKE '%/4 · branch %/4' AND explain NOT LIKE '%step Unknown · branch %/4') = 0,
    countIf(toFloat64OrZero(extract(explain, 'step (\\d+\\.\\d+)/')) > 4) = 0,
    countIf(toFloat64OrZero(extract(explain, 'branch (\\d+\\.\\d+)/')) > 4) = 0
FROM (EXPLAIN ANALYZE time = 1 SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4)
WHERE explain LIKE '%Concurrency:%';

-- 3. For a leaf step the subtree is the step itself, so `step` and `branch` print the same text.
SELECT countIf(extract(explain, 'step (.*) · branch') = extract(explain, '· branch (.*)$')) >= 1
FROM (EXPLAIN ANALYZE time = 1 SELECT count() FROM numbers_mt(1000000))
WHERE explain LIKE '%Time: step%';

-- 4. With a single thread the concurrency is exactly one wherever there was work.
SELECT countIf(explain NOT LIKE '%step 1.00/1 · branch 1.00/1%'
           AND explain NOT LIKE '%step Unknown · branch 1.00/1%') = 0
FROM (EXPLAIN ANALYZE time = 1 SELECT count() FROM numbers(1000000) WHERE number % 7 = 1 SETTINGS max_threads = 1)
WHERE explain LIKE '%Concurrency:%';

-- 5. A step that owns no processors, such as `Union` of inputs with equal headers, has no
--    concurrency of its own: the `step` part is `Unknown`, the `branch` part is still a fraction.
SELECT
    countIf(explain LIKE '%Concurrency: step Unknown · branch %/%') >= 1,
    countIf(explain LIKE '%step 0.00/%') = 0,
    countIf(explain LIKE '%Time: step %') = countIf(explain LIKE '%Concurrency: step %')
FROM (EXPLAIN ANALYZE time = 1
    SELECT sum(x) FROM (SELECT number AS x FROM numbers(10) UNION ALL SELECT number FROM numbers(10)));
