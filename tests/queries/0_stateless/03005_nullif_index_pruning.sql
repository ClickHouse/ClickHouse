DROP TABLE IF EXISTS t_nullif_pruning;

CREATE TABLE t_nullif_pruning (team UInt64, k UInt8) 
ENGINE = MergeTree 
ORDER BY (team, k) 
SETTINGS index_granularity = 8192;

INSERT INTO t_nullif_pruning SELECT 1, number % 200 FROM numbers(2000000);
OPTIMIZE TABLE t_nullif_pruning FINAL;

-- This should successfully prune granules (2/245) instead of a full scan
EXPLAIN indexes = 1 SELECT count() FROM t_nullif_pruning WHERE team = 1 AND nullIf(k, 255) = 5;

SELECT '--- NEGATIVE TESTS (Should fallback to full scan) ---';
EXPLAIN indexes = 1 SELECT count() FROM t_nullif_pruning WHERE team = 1 AND nullIf(k, 255) != 5;
EXPLAIN indexes = 1 SELECT count() FROM t_nullif_pruning WHERE team = 1 AND nullIf(k, k) = 5;
EXPLAIN indexes = 1 SELECT count() FROM t_nullif_pruning WHERE team = 1 AND nullIf(k, toInt16(5)) = 5;

DROP TABLE t_nullif_pruning;
