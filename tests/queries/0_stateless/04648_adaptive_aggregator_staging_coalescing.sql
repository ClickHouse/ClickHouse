-- Tags: long

-- Exercises the coalescing of the adaptive aggregator's staged batches: with a small
-- max_block_size every consumed block publishes a small per-block staging batch, and a thread's
-- buffered batches are sealed into one bucket-grouped chunk before the merge-time drain (large
-- batches are enqueued as-is, the rest are merged either when enough bytes accumulate or at the
-- end of the input). The keys are nearly unique, so with a tiny freeze threshold almost every
-- row is staged. Every cell compares the same query with the feature off and on.

SET max_threads = 4;
SET max_block_size = 4096;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'General aggregates coalesced across many small blocks';
SELECT
    (SELECT count(), sum(s), sum(mn), sum(c) FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s), sum(mn), sum(c) FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1));

SELECT 'String arguments cross the seal target mid-production';
SELECT
    (SELECT count(), sum(cityHash64(m)), sum(c) FROM (SELECT number % 100000 AS g, max(repeat(toString(number), 8)) AS m, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(m)), sum(c) FROM (SELECT number % 100000 AS g, max(repeat(toString(number), 8)) AS m, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Count-only value staging coalesced';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c) FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1));

SELECT '-Array combinator across coalesced batches';
SELECT
    (SELECT count(), sum(s) FROM (SELECT number % 50000 AS g, sumArray([number, number * 2]) AS s FROM numbers_mt(200000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT number % 50000 AS g, sumArray([number, number * 2]) AS s FROM numbers_mt(200000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Constant argument stays correct across batches';
SELECT
    (SELECT count(), sum(s), sum(cityHash64(m)) FROM (SELECT number % 50000 AS g, sum(7) AS s, max('const payload') AS m FROM numbers_mt(200000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s), sum(cityHash64(m)) FROM (SELECT number % 50000 AS g, sum(7) AS s, max('const payload') AS m FROM numbers_mt(200000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Freeze at the first opportunity (threshold 0)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT number % 100000 AS g, sum(number) AS s FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT number % 100000 AS g, sum(number) AS s FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0));

-- With full-size blocks and wide string arguments a single batch reaches half the seal target
-- on its own and is enqueued without coalescing.
SELECT 'Large batches are enqueued without coalescing';
SELECT
    (SELECT count(), sum(cityHash64(m)) FROM (SELECT number % 200000 AS g, max(repeat(toString(number), 12)) AS m FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0, max_block_size = 65536))
    =
    (SELECT count(), sum(cityHash64(m)) FROM (SELECT number % 200000 AS g, max(repeat(toString(number), 12)) AS m FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_block_size = 65536));

-- The virtual column `_part` is a LowCardinality(String) argument whose dictionary differs
-- between the two parts, so the seal's argument gather appends LowCardinality columns with
-- unshared dictionaries.
SELECT 'LowCardinality argument gathered across batches';
DROP TABLE IF EXISTS t_coalesce;
CREATE TABLE t_coalesce (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_coalesce SELECT number, number FROM numbers(200000);
INSERT INTO t_coalesce SELECT number + 200000, number FROM numbers(200000);
SELECT
    (SELECT count(), sum(cityHash64(p)), sum(s) FROM (SELECT k % 100000 AS g, max(_part) AS p, sum(v) AS s FROM t_coalesce GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(p)), sum(s) FROM (SELECT k % 100000 AS g, max(_part) AS p, sum(v) AS s FROM t_coalesce GROUP BY g SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE t_coalesce;

-- Self-comparison cannot catch a bug shared by both paths, so pin exact values over
-- deterministic data.
SELECT 'Analytic guard';
SELECT count(), sum(g), sum(s), sum(c) FROM (SELECT number % 30000 AS g, sum(number) AS s, count() AS c FROM numbers_mt(120000) GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1;
