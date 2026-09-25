-- Tags: no-fasttest

-- Regression test for the "Virtual row does not cover sort column" logical error.
-- ORDER BY matches only the key prefix (a): the collated constant stops the match. The virtual row
-- conversion it builds reads one key column but outputs two, since the leading constant is emitted as
-- a fixed column. The preliminary DISTINCT then widens the read prefix to (a, b). The stale one-column
-- virtual row must be dropped on that widening; counting the conversion's outputs instead of its key
-- inputs kept it, so the preliminary merge over (a, b) received a virtual row covering only a.

DROP TABLE IF EXISTS t_virtual_row_widen_const;

CREATE TABLE t_virtual_row_widen_const (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

-- Keep the parts unmerged: the preliminary merge only exists for several parts.
SYSTEM STOP MERGES t_virtual_row_widen_const;

INSERT INTO t_virtual_row_widen_const SELECT number % 10, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_widen_const SELECT number % 10, number % 7 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_widen_const SELECT number % 10, number % 7 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_widen_const SELECT number % 10, number % 7 FROM numbers(6000, 2000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, optimize_distinct_in_order = 1,
    read_in_order_two_level_merge_threshold = 1, max_threads = 2, max_block_size = 64;

-- DISTINCT per partition would read each partition through its own port without a preliminary merge.
SET allow_distinct_partitions_independently = 0, force_distinct_partitions_independently = 0;

-- The widened read must not keep the virtual row that covers only a.
SELECT count()
FROM (EXPLAIN PLAN actions = 1 SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a, 'd' COLLATE 'cs', b)
WHERE explain LIKE '%Virtual row conversions%';

-- Must not throw and must match the unoptimized read, in both read directions.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a, 'd' COLLATE 'cs', b))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a, 'd' COLLATE 'cs', b SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a DESC, 'd' COLLATE 'cs', b DESC))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a DESC, 'd' COLLATE 'cs', b DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

-- Same widening without the collation: ORDER BY covers only a, DISTINCT widens to (a, b).
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen_const ORDER BY 'x', a SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_widen_const;
