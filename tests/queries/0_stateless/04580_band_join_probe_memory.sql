-- Tags: no-old-analyzer, no-random-settings
-- no-random-settings: the memory ceiling below is calibrated for the pinned block sizes

-- Regression guard for the probe-side output accumulator: scattered probe points whose
-- matches hop between index blocks on almost every row, with payload columns on the interval
-- side. A per-match-run column accumulator (the defect fixed on this branch) allocates a tiny
-- buffer per output column per block change and needs hundreds of MiB here; the flat
-- (block, row) accumulator stays under ~10 MiB, so the ceiling has an order of magnitude of
-- headroom on both sides.

SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';

DROP TABLE IF EXISTS mem_p;
DROP TABLE IF EXISTS mem_i;

CREATE TABLE mem_p (id UInt32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mem_i (id UInt32, lo Int64, hi Int64, pay1 UInt64, pay2 UInt64, pay3 UInt64) ENGINE = MergeTree ORDER BY id;

-- Points jump across the whole [0, 500000) domain; thin intervals [5n, 5n+9] tile it, so
-- with 1000-row blocks consecutive points nearly always probe different index blocks.
INSERT INTO mem_p SELECT number, (number * 40503) % 500000 FROM numbers(200000);
INSERT INTO mem_i SELECT number, number * 5, number * 5 + 9, number, number + 1, number + 2 FROM numbers(100000);

SELECT count(), sum(pay1 + pay2 + pay3) > 0
FROM mem_p p JOIN mem_i i ON p.t >= i.lo AND p.t <= i.hi
SETTINGS max_memory_usage = 100000000, max_threads = 4, max_block_size = 1000, max_joined_block_size_rows = 65536;

DROP TABLE mem_p;
DROP TABLE mem_i;
