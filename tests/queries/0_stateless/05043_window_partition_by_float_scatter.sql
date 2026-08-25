-- The sort that feeds a window function scatters rows across threads by the hash of the
-- PARTITION BY columns, but the window finds partition boundaries with `compareAt`. For Float64
-- the two disagree on -0. and 0. (equal by `compareAt`, different hashes), so the scatter would
-- split one logical partition across threads. Such windows must sort in one stream.

DROP TABLE IF EXISTS t_window_scatter_float;

CREATE TABLE t_window_scatter_float (k Float64, u UInt64, v UInt64) ENGINE = MergeTree() ORDER BY v;

SYSTEM STOP MERGES t_window_scatter_float;

-- Several parts, so the read produces several streams.
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), number % 3, number FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), number % 3, number + 25000 FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), number % 3, number + 50000 FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), number % 3, number + 75000 FROM numbers(25000);

-- All 100000 rows form one window partition (-0. and 0. compare as equal): the only
-- count value is 100000.
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_float) ORDER BY c
SETTINGS max_threads = 8;

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_float) ORDER BY c
SETTINGS max_threads = 1;

-- The float partition key gets a single-stream sort: no scatter in the pipeline.
SELECT count() FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY k) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- A safe partition key type keeps the parallel scattered sort.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY u) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

DROP TABLE t_window_scatter_float;
