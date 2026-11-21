-- The sort that feeds a window function scatters rows across threads by the hash of the
-- PARTITION BY columns, but the window finds partition boundaries with `compareAt`. For Float64
-- the two disagree on -0. and 0. (equal by `compareAt`, different hashes), so the scatter would
-- split one logical partition across threads. Such windows must sort in one stream.

DROP TABLE IF EXISTS t_window_scatter_float;

CREATE TABLE t_window_scatter_float (k Float64, nk Nullable(Float64), u UInt64, v UInt64) ENGINE = MergeTree() ORDER BY v;

SYSTEM STOP MERGES t_window_scatter_float;

-- Several parts, so the read produces several streams.
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), if(number % 2 = 0, 0., -0.), number % 3, number FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), if(number % 2 = 0, 0., -0.), number % 3, number + 25000 FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), if(number % 2 = 0, 0., -0.), number % 3, number + 50000 FROM numbers(25000);
INSERT INTO t_window_scatter_float SELECT if(number % 2 = 0, 0., -0.), if(number % 2 = 0, 0., -0.), number % 3, number + 75000 FROM numbers(25000);

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

-- A float nested inside Nullable must not scatter either.
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY nk) AS c FROM t_window_scatter_float) ORDER BY c
SETTINGS max_threads = 8;

SELECT count() FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY nk) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- Dynamic hashes by the physical layout of the value, which can differ between blocks for
-- values that compare as equal, so it must not scatter either. Here every value is a Float64
-- inside Dynamic, and -0. and 0. still compare as equal.
DROP TABLE IF EXISTS t_window_scatter_dynamic;

CREATE TABLE t_window_scatter_dynamic (d Dynamic, v UInt64) ENGINE = MergeTree() ORDER BY v;

SYSTEM STOP MERGES t_window_scatter_dynamic;

INSERT INTO t_window_scatter_dynamic SELECT if(number % 2 = 0, 0., -0.)::Dynamic, number FROM numbers(25000);
INSERT INTO t_window_scatter_dynamic SELECT if(number % 2 = 0, 0., -0.)::Dynamic, number + 25000 FROM numbers(25000);
INSERT INTO t_window_scatter_dynamic SELECT if(number % 2 = 0, 0., -0.)::Dynamic, number + 50000 FROM numbers(25000);
INSERT INTO t_window_scatter_dynamic SELECT if(number % 2 = 0, 0., -0.)::Dynamic, number + 75000 FROM numbers(25000);

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY d) AS c FROM t_window_scatter_dynamic) ORDER BY c
SETTINGS max_threads = 8, allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 1;

SELECT count() FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY d) FROM t_window_scatter_dynamic SETTINGS max_threads = 8, allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 1)
WHERE explain ILIKE '%ScatterByPartition%';

-- JSON also hashes by the physical layout, so it must not scatter. A wrong result needs
-- layouts that differ between blocks, which is hard to arrange deterministically, so only
-- the pipeline shape is pinned.
DROP TABLE IF EXISTS t_window_scatter_json;

CREATE TABLE t_window_scatter_json (j JSON, v UInt64) ENGINE = MergeTree() ORDER BY v;

SYSTEM STOP MERGES t_window_scatter_json;

INSERT INTO t_window_scatter_json SELECT toJSONString(map('a', if(number % 2 = 0, 0., -0.)))::JSON, number FROM numbers(25000);
INSERT INTO t_window_scatter_json SELECT toJSONString(map('a', if(number % 2 = 0, 0., -0.)))::JSON, number + 25000 FROM numbers(25000);
INSERT INTO t_window_scatter_json SELECT toJSONString(map('a', if(number % 2 = 0, 0., -0.)))::JSON, number + 50000 FROM numbers(25000);
INSERT INTO t_window_scatter_json SELECT toJSONString(map('a', if(number % 2 = 0, 0., -0.)))::JSON, number + 75000 FROM numbers(25000);

SELECT count() FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY j) FROM t_window_scatter_json SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- The old interpreter must apply the same guard.
SET enable_analyzer = 0;

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_float) ORDER BY c
SETTINGS max_threads = 8;

SELECT count() FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY k) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY u) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

DROP TABLE t_window_scatter_float;
DROP TABLE t_window_scatter_dynamic;
DROP TABLE t_window_scatter_json;
