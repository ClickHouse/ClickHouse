-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, p UInt64) ENGINE = MergeTree() PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';

SYSTEM STOP MERGES t;

-- Four 0-level parts, two per partition.
INSERT INTO t SELECT number, 0 FROM numbers(10); -- 0_1_1_0
INSERT INTO t SELECT number, 1 FROM numbers(10); -- 1_2_2_0
INSERT INTO t SELECT number, 0 FROM numbers(10); -- 2_3_3_0
INSERT INTO t SELECT number, 1 FROM numbers(10); -- 3_4_4_0

SELECT '== 0-level parts: filter by _block_number and _block_offset works ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _block_number > 3)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _block_offset > 1000)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT '== Single partition filter ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '0' and _block_number >= 1 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '1' and _block_number >= 4 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SYSTEM START MERGES t;
OPTIMIZE TABLE t PARTITION 0 FINAL;
OPTIMIZE TABLE t PARTITION 1 FINAL;

SELECT '== Single partition filter on merged parts ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '0' and _block_number >= 1 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '1' and _block_number >= 4 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT '== Single partition filter on merged parts filter ==';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '0' and _block_number >= 10 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE _partition_id = '1' and _block_number >= 10 and _block_offset > 6)
WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%';

DROP TABLE t;
