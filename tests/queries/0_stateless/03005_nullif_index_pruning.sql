-- Tags: no-distributed-interpreters, no-random-settings, no-random-merge-tree-settings
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

-- Regression test for FixedString zero-padding type matching
DROP TABLE IF EXISTS t_nullif_fixedstring;
CREATE TABLE t_nullif_fixedstring (s FixedString(5)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_nullif_fixedstring VALUES ('abc\0\0');

EXPLAIN indexes = 1 SELECT count() FROM t_nullif_fixedstring WHERE nullIf(s, 'abc') = 'abc\0';
SELECT count() FROM t_nullif_fixedstring WHERE nullIf(s, 'abc') = 'abc\0';

DROP TABLE IF EXISTS t_nullif_fixedstring;

-- Regression test for Partition Pruning
DROP TABLE IF EXISTS t_nullif_partition;
CREATE TABLE t_nullif_partition (p UInt8, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY v SETTINGS index_granularity = 8192;
INSERT INTO t_nullif_partition SELECT number % 10, number FROM numbers(2000000);
OPTIMIZE TABLE t_nullif_partition FINAL;

EXPLAIN indexes = 1 SELECT count() FROM t_nullif_partition WHERE nullIf(p, 255) = 5 SETTINGS optimize_use_implicit_projections = 0;
DROP TABLE t_nullif_partition;

-- Regression test for MinMax Skip Index Pruning
DROP TABLE IF EXISTS t_nullif_skip_index;
CREATE TABLE t_nullif_skip_index (id UInt64, s UInt8, INDEX idx_s s TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
-- Shape data monotonically so each granule contains a distinct narrow minmax range
INSERT INTO t_nullif_skip_index SELECT number, toUInt8(number / 8192) FROM numbers(2000000);
OPTIMIZE TABLE t_nullif_skip_index FINAL;

EXPLAIN indexes = 1 SELECT count() FROM t_nullif_skip_index WHERE nullIf(s, 255) = 50;
DROP TABLE IF EXISTS t_nullif_skip_index;
