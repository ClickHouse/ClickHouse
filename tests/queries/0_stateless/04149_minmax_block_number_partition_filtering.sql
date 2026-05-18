-- Tags: no-shared-merge-tree, no-random-merge-tree-settings
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree
PARTITION BY (a, b) ORDER BY c
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset',
         add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t;

INSERT INTO t SELECT 1, 1, 1; -- partition 1-1, block 1
INSERT INTO t SELECT 1, 1, 2; -- partition 1-1, block 2
INSERT INTO t SELECT 1, 1, 3; -- partition 1-1, block 3
INSERT INTO t SELECT 1, 2, 4; -- partition 1-2, block 4
INSERT INTO t SELECT 2, 1, 5; -- partition 2-1, block 5
INSERT INTO t SELECT 2, 2, 6; -- partition 2-2, block 6
INSERT INTO t SELECT 2, 2, 7; -- partition 2-2, block 7
INSERT INTO t SELECT 2, 2, 8; -- partition 2-2, block 8

SELECT '== all rows ==';
SELECT a, b, c, _part, _partition_id, _block_number FROM t ORDER BY _part;

-- Min-Max prunes 4/8 parts: only partitions 1-1 and 2-2 with the right block_number range.
SELECT '== filter by partition columns + _block_number ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1
    SELECT a, b, c FROM t
    WHERE (a = 1 AND b = 1 AND _block_number >= 1) OR (a = 2 AND b = 2 AND _block_number >= 8))
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

-- _partition_id constraint should not break filtering
SELECT '== filter by partition columns + _partition_id + _block_number ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1
    SELECT a, b, c FROM t
    WHERE (_partition_id = '1-1' AND a = 1 AND b = 1 AND _block_number >= 1) OR (_partition_id = '2-2' AND a = 2 AND b = 2 AND _block_number >= 8))
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

-- _partition_id is not part of the per-part minmax key, so Min-Max only prunes by _block_number.
SELECT '== filter by _partition_id + _block_number ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1
    SELECT a, b, c FROM t
    WHERE (_partition_id = '1-1' AND _block_number >= 1) OR (_partition_id = '2-2' AND _block_number >= 8))
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

DROP TABLE t;
