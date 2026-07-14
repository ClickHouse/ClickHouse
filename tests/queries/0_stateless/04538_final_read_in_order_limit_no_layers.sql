-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local FINAL reading pipeline.

DROP TABLE IF EXISTS t_final_layers;

CREATE TABLE t_final_layers (k UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES t_final_layers;

INSERT INTO t_final_layers SELECT number, 1 FROM numbers(100000);
INSERT INTO t_final_layers SELECT number, 2 FROM numbers(100000);
INSERT INTO t_final_layers SELECT number, 3 FROM numbers(100000);

SET max_threads = 4, max_final_threads = 4;

-- Read-in-order with a small limit: intersecting ranges must not be split into layers,
-- the result is expected to be produced by a single lazy merging stream.
SELECT 'in-order small limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 10);

-- Read-in-order without a limit consumes the whole ordered stream: layers must be kept.
SELECT 'in-order no limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k);

-- A limit comparable to the layer size (300000 rows / 4 layers = 75000): layers must be kept.
SELECT 'in-order large limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 100000);

-- Aggregation in order sets read-in-order but has no limit: layers must be kept.
SELECT 'aggregation in order, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(v) FROM t_final_layers FINAL GROUP BY k SETTINGS optimize_aggregation_in_order = 1);

-- No read-in-order (ORDER BY a non-key column): layers must be kept even with a small limit.
SELECT 'no in-order small limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY v, k LIMIT 10);

-- The result must be the same with and without splitting into layers.
SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 5;
SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 5
SETTINGS split_intersecting_parts_ranges_into_layers_final = 0;

DROP TABLE t_final_layers;
