-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local FINAL reading pipeline.

DROP TABLE IF EXISTS t_final_layers;
DROP TABLE IF EXISTS t_final_layers_dups;

CREATE TABLE t_final_layers (k UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merged base part (level > 0, so keys are unique in it) plus two unmerged update parts.
INSERT INTO t_final_layers SELECT number, 1 FROM numbers(300000);
OPTIMIZE TABLE t_final_layers FINAL;
SYSTEM STOP MERGES t_final_layers;
INSERT INTO t_final_layers SELECT number, 2 FROM numbers(100000);
INSERT INTO t_final_layers SELECT number, 3 FROM numbers(100000);

SET max_threads = 4, max_final_threads = 4;
-- The test relies on settings that are randomized by the test runner: pin them.
SET optimize_read_in_order = 1;
SET split_intersecting_parts_ranges_into_layers_final = 1;

-- Read-in-order with a small limit: intersecting ranges must not be split into layers,
-- the result is expected to be produced by a single lazy merging stream.
SELECT 'in-order small limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 10);

-- The limit counts rows after the FINAL collapse. The lower bound of a layer's output counts
-- only parts of non-zero level: between 300000 and 500000 rows / 4 layers / 3 parts, i.e. at
-- most 41666 (the update parts have non-zero level only when they were collapsed on insert),
-- so a limit of 50000 may need more than one layer and must keep them.
SELECT 'in-order mid limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 50000);

-- Read-in-order without a limit consumes the whole ordered stream: layers must be kept.
SELECT 'in-order no limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k);

-- A limit comparable to the number of selected rows: layers must be kept.
SELECT 'in-order large limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 100000);

-- Aggregation in order sets read-in-order but has no limit: layers must be kept.
SELECT 'aggregation in order, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(v) FROM t_final_layers FINAL GROUP BY k SETTINGS optimize_aggregation_in_order = 1);

-- No read-in-order (ORDER BY a non-key column): layers must be kept even with a small limit.
SELECT 'no in-order small limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY v, k LIMIT 10);

-- An unmerged part may contain many versions of the same key (e.g. inserted with
-- optimize_on_insert = 0), so it gives no lower bound on the collapsed size: with only
-- unmerged parts layers must be kept even with a small limit.
SET optimize_on_insert = 0;
CREATE TABLE t_final_layers_dups (k UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_final_layers_dups;
INSERT INTO t_final_layers_dups SELECT number % 30000, number FROM numbers(300000);
INSERT INTO t_final_layers_dups SELECT number % 30000, number + 300000 FROM numbers(300000);

SELECT 'unmerged duplicates small limit, layers:', countIf(explain LIKE '%FilterSortedStreamByRange%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers_dups FINAL WHERE v > 0 ORDER BY k LIMIT 10);

-- The result must be the same with and without splitting into layers.
SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 5;
SELECT k, v FROM t_final_layers FINAL WHERE v > 0 ORDER BY k LIMIT 5
SETTINGS split_intersecting_parts_ranges_into_layers_final = 0;

DROP TABLE t_final_layers;
DROP TABLE t_final_layers_dups;
