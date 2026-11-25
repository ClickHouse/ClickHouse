-- The sort that feeds a window function scatters rows across threads by the hash of the
-- PARTITION BY columns, but the window finds partition boundaries with `compareAt`. For floats
-- -0. and 0., and all NaNs, compare as equal but have different bits, so the scatter hashes one
-- representation of each to keep a logical partition in one thread.

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

-- The float partition key keeps the parallel scattered sort.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY k) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- A safe partition key type keeps the parallel scattered sort.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY u) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- The same for a float inside Nullable.
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY nk) AS c FROM t_window_scatter_float) ORDER BY c
SETTINGS max_threads = 8;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY nk) FROM t_window_scatter_float SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- A parallel DISTINCT below the window partitions its streams by the bits of the key, which the
-- window must not reuse: -0. and 0. can be in different streams.
SELECT (SELECT groupArraySorted(16)(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k FROM t_window_scatter_float))
        SETTINGS max_threads = 8, allow_parallel_distinct = 1)
     = (SELECT groupArraySorted(16)(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k FROM t_window_scatter_float))
        SETTINGS max_threads = 1);

-- NaNs of both signs and with payloads compare as equal as well. The table is sorted by the key, so
-- the window reads it in order and scatters the merged stream.
DROP TABLE IF EXISTS t_window_scatter_nan;

CREATE TABLE t_window_scatter_nan (k Float64, f Float32, v UInt64) ENGINE = MergeTree() ORDER BY k;

SYSTEM STOP MERGES t_window_scatter_nan;

INSERT INTO t_window_scatter_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_scatter_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_scatter_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_scatter_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_nan) ORDER BY c
SETTINGS max_threads = 8;

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_nan) ORDER BY c
SETTINGS max_threads = 1;

-- The same for Float32.
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY f) AS c FROM t_window_scatter_nan) ORDER BY c
SETTINGS max_threads = 8;

-- Equal floats can be in different partitions of the table, which the window must not reuse.
DROP TABLE IF EXISTS t_window_scatter_partitioned;

CREATE TABLE t_window_scatter_partitioned (k Float64, v UInt64) ENGINE = MergeTree() PARTITION BY k ORDER BY v
SETTINGS allow_floating_point_partition_key = 1;

INSERT INTO t_window_scatter_partitioned SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 4 + 1], number FROM numbers(20000);

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_scatter_partitioned) ORDER BY c
SETTINGS max_threads = 8, allow_window_partitions_independently = 1, force_window_partitions_independently = 1;

SELECT count() FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY k) FROM t_window_scatter_partitioned
    SETTINGS max_threads = 8, allow_window_partitions_independently = 1, force_window_partitions_independently = 1)
WHERE explain LIKE '%Read each partition through separate port%';

-- A window read by layers of the primary key keeps a partition in one layer.
DROP TABLE IF EXISTS t_window_scatter_layers;

CREATE TABLE t_window_scatter_layers (a UInt8, k Float64, v UInt64) ENGINE = MergeTree() ORDER BY (a, k);

SYSTEM STOP MERGES t_window_scatter_layers;

INSERT INTO t_window_scatter_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_scatter_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_scatter_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_scatter_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);

SELECT (SELECT groupArraySorted(64)(c) FROM (SELECT count() OVER (PARTITION BY a, k) AS c FROM t_window_scatter_layers) SETTINGS max_threads = 8)
     = (SELECT groupArraySorted(64)(c) FROM (SELECT count() OVER (PARTITION BY a, k) AS c FROM t_window_scatter_layers) SETTINGS max_threads = 1);

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

DROP TABLE t_window_scatter_float;
DROP TABLE t_window_scatter_nan;
DROP TABLE t_window_scatter_partitioned;
DROP TABLE t_window_scatter_layers;
DROP TABLE t_window_scatter_dynamic;
DROP TABLE t_window_scatter_json;
