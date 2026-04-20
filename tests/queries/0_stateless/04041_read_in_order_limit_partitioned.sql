-- optimize_read_in_order with LIMIT on a partitioned table: parts from partitions that cannot
-- contribute to the top-N are trimmed, so the merge fan-in is small (e.g. 5, the limit, not 20, the number of parts).

DROP TABLE IF EXISTS t_read_in_order_partitioned;

CREATE TABLE t_read_in_order_partitioned (dt DateTime, val UInt64)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY dt
SETTINGS index_granularity = 8192;

SYSTEM STOP MERGES t_read_in_order_partitioned;

-- 5 parts per month × 4 months = 20 parts (Jan=lowest dt, Apr=highest).
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-01-01 00:00:00') + number * 60, number        FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-01-01 03:00:00') + number * 60, number + 200  FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-01-01 06:00:00') + number * 60, number + 400  FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-01-01 09:00:00') + number * 60, number + 600  FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-01-01 12:00:00') + number * 60, number + 800  FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-02-01 00:00:00') + number * 60, number + 1000 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-02-01 03:00:00') + number * 60, number + 1200 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-02-01 06:00:00') + number * 60, number + 1400 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-02-01 09:00:00') + number * 60, number + 1600 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-02-01 12:00:00') + number * 60, number + 1800 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-03-01 00:00:00') + number * 60, number + 2000 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-03-01 03:00:00') + number * 60, number + 2200 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-03-01 06:00:00') + number * 60, number + 2400 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-03-01 09:00:00') + number * 60, number + 2600 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-03-01 12:00:00') + number * 60, number + 2800 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-04-01 00:00:00') + number * 60, number + 3000 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-04-01 03:00:00') + number * 60, number + 3200 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-04-01 06:00:00') + number * 60, number + 3400 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-04-01 09:00:00') + number * 60, number + 3600 FROM numbers(200);
INSERT INTO t_read_in_order_partitioned SELECT toDateTime('2024-04-01 12:00:00') + number * 60, number + 3800 FROM numbers(200);

-- No filter, ascending order
SELECT val FROM t_read_in_order_partitioned ORDER BY dt ASC LIMIT 5
SETTINGS optimize_read_in_order = 1, max_threads = 2;
-- No filter, descending order
SELECT val FROM t_read_in_order_partitioned ORDER BY dt DESC LIMIT 5
SETTINGS optimize_read_in_order = 1, max_threads = 2;

-- Query with WHERE filter that returns a value in the last part
SELECT val
FROM t_read_in_order_partitioned
WHERE val = 3800
ORDER BY dt ASC
LIMIT 1
SETTINGS optimize_read_in_order = 1, max_threads = 2;

-- Query with PREWHERE filter that returns a value in the last part
SELECT val
FROM t_read_in_order_partitioned
PREWHERE val = 3800
ORDER BY dt ASC
LIMIT 1
SETTINGS optimize_read_in_order = 1, max_threads = 2;

-- A key-range WHERE predicate causes PK pruning to select only a subset of granules inside
-- the first partition's parts (partial ranges)
SELECT val
FROM t_read_in_order_partitioned
WHERE dt >= toDateTime('2024-01-01 00:35:00')
ORDER BY dt ASC
LIMIT 5
SETTINGS optimize_read_in_order = 1, max_threads = 2;

SYSTEM START MERGES t_read_in_order_partitioned;
DROP TABLE t_read_in_order_partitioned;

-- Use ReplacingMergeTree table to test FINAL
DROP TABLE IF EXISTS t_read_in_order_partitioned_final;

CREATE TABLE t_read_in_order_partitioned_final
(
    dt DateTime,
    id UInt32,
    ver UInt8
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toYYYYMM(dt)
ORDER BY dt;

SYSTEM STOP MERGES t_read_in_order_partitioned_final;

-- January: obsolete versions (ver=1)
INSERT INTO t_read_in_order_partitioned_final
SELECT toDateTime('2024-01-01 00:00:00') + number * 60 AS dt,
       number AS id,
       toUInt8(1) AS ver
FROM numbers(2000);
-- February: surviving versions (ver=2)
INSERT INTO t_read_in_order_partitioned_final
SELECT toDateTime('2024-02-01 00:00:00') + number * 60 AS dt,
       number AS id,
       toUInt8(2) AS ver
FROM numbers(2000);

-- Query using FINAL
SELECT id
FROM t_read_in_order_partitioned_final
FINAL
ORDER BY dt ASC
LIMIT 5
SETTINGS optimize_read_in_order = 1, max_threads = 2;

SYSTEM START MERGES t_read_in_order_partitioned_final;
DROP TABLE t_read_in_order_partitioned_final;

-- Test is_deleted
DROP TABLE IF EXISTS t_read_in_order_partitioned_deleted;

CREATE TABLE t_read_in_order_partitioned_deleted
(
    dt DateTime,
    id UInt32,
    ver UInt8,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
PARTITION BY toYYYYMM(dt)
ORDER BY dt;

SYSTEM STOP MERGES t_read_in_order_partitioned_deleted;

INSERT INTO t_read_in_order_partitioned_deleted
SELECT toDateTime('2024-01-01 00:00:00') + number * 60 AS dt,
       number AS id,
       toUInt8(1) AS ver,
       toUInt8(1) AS is_deleted  -- deleted rows
FROM numbers(2000);

INSERT INTO t_read_in_order_partitioned_deleted
SELECT toDateTime('2024-02-01 00:00:00') + number * 60 AS dt,
       number AS id,
       toUInt8(2) AS ver,
       toUInt8(0) AS is_deleted  -- live rows
FROM numbers(2000);

SELECT id
FROM t_read_in_order_partitioned_deleted
ORDER BY dt ASC
LIMIT 5
SETTINGS optimize_read_in_order = 0, max_threads = 2;

SELECT id
FROM t_read_in_order_partitioned_deleted
ORDER BY dt ASC
LIMIT 5
SETTINGS optimize_read_in_order = 1, max_threads = 2;

SYSTEM START MERGES t_read_in_order_partitioned_deleted;
DROP TABLE t_read_in_order_partitioned_deleted;

-- Non-monotone partition key (PARTITION BY c1 % N) must not cause incorrect trimming.
-- Lexicographic partition ID order differs from sort key order.
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY c1 PARTITION BY (c1 % 6451);
SET min_insert_block_size_rows = 64, optimize_trivial_insert_select = 1;

SYSTEM STOP MERGES t0;
INSERT INTO TABLE t0 (c1) SELECT number FROM numbers(500);

SELECT * FROM t0 ORDER BY c1 LIMIT 10;
SYSTEM START MERGES t0;
DROP TABLE t0;