-- Tags: no-old-analyzer

-- Output chunk caps and probe resumability: the probe transform must split its output by
-- `max_joined_block_size_rows` / `max_joined_block_size_bytes` and resume mid-walk, so a
-- single point row with more matches than a cap allows spreads over many chunks without
-- losing or duplicating pairs.

-- Keep the written join order: the band join detects only the point-side-on-the-left
-- orientation for now, so a planner swap would silently change the executed algorithm.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';

-- Pin the shape to the band join, so the caps below exercise its probe and not a fallback
SELECT 'plan', count() > 0 FROM (EXPLAIN
    SELECT count()
    FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
    JOIN (SELECT (number % 100) :: Int64 AS lo, (number % 100 + 50) :: Int64 AS hi FROM numbers(1000)) i
    ON p.t >= i.lo AND p.t <= i.hi)
WHERE explain LIKE '%BandJoin%';

-- The whole output is much larger than the row cap
SELECT 'output larger than cap', count(), sum(p.t), sum(i.lo)
FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
JOIN (SELECT (number % 100) :: Int64 AS lo, (number % 100 + 50) :: Int64 AS hi FROM numbers(1000)) i
ON p.t >= i.lo AND p.t <= i.hi
SETTINGS max_joined_block_size_rows = 1000;

-- A single point row matching more intervals than the row cap splits mid-row
SELECT 'one row over row cap', count(), sum(i.id), min(i.lo), max(i.hi)
FROM (SELECT 500000 :: Int64 AS t) p
JOIN (SELECT number AS id, number :: Int64 AS lo, (number + 1000000) :: Int64 AS hi FROM numbers(100000)) i
ON p.t >= i.lo AND p.t <= i.hi
SETTINGS max_joined_block_size_rows = 100;

-- A fat interval row (long strings) with many matches trips the byte cap; the row cap alone
-- would allow far bigger chunks
SELECT 'fat rows over byte cap', count(), sum(length(i.payload)), sum(p.t)
FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
JOIN (SELECT number :: Int64 AS lo, (number + 200) :: Int64 AS hi, repeat('x', 10000) AS payload FROM numbers(1000)) i
ON p.t >= i.lo AND p.t <= i.hi
SETTINGS max_joined_block_size_rows = 65536, max_joined_block_size_bytes = 262144;

-- A row cap of zero disables splitting entirely (hash-join semantics; the byte cap needs a
-- non-zero row cap) - the result must not change
SELECT 'caps disabled', count(), sum(p.t), sum(i.lo)
FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
JOIN (SELECT (number % 100) :: Int64 AS lo, (number % 100 + 50) :: Int64 AS hi FROM numbers(1000)) i
ON p.t >= i.lo AND p.t <= i.hi
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;
