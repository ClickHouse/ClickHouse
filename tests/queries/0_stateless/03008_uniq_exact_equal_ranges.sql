DROP TABLE IF EXISTS t_uniq_exact;

CREATE TABLE t_uniq_exact (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a;

SET group_by_two_level_threshold_bytes = 1;
SET group_by_two_level_threshold = 1;
SET max_threads = 4;
SET max_block_size = 16384;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;

-- Data volume is kept small (30k rows per key, 10 parts, 300k total) to let
-- `OPTIMIZE FINAL` complete quickly under slow CI configurations (MSan +
-- WasmEdge + aggressive randomized `MergeTree` settings such as tiny
-- `index_granularity` and forced vertical merge). `max_block_size` is pinned
-- so that both the threshold=1.0 and threshold=0.5 paths for
-- `min_hit_rate_to_use_consecutive_keys_optimization` are exercised across
-- multiple blocks per thread regardless of the randomizer.
INSERT INTO t_uniq_exact SELECT 0, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 1, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 2, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 3, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 4, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 5, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 6, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 7, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 8, randomPrintableASCII(5), rand() FROM numbers(30000);
INSERT INTO t_uniq_exact SELECT 9, randomPrintableASCII(5), rand() FROM numbers(30000);

OPTIMIZE TABLE t_uniq_exact FINAL;

SELECT a, uniqExact(b) FROM t_uniq_exact GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 1.0
EXCEPT
SELECT a, uniqExact(b) FROM t_uniq_exact GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 0.5;

SELECT a, sum(c) FROM t_uniq_exact GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 1.0
EXCEPT
SELECT a, sum(c) FROM t_uniq_exact GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 0.5;

DROP TABLE t_uniq_exact;
