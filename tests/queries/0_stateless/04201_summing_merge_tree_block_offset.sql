-- Test: exercises `SummingSortedAlgorithm::defineColumns` _block_offset exclusion.
-- Covers: src/Processors/Merges/Algorithms/SummingSortedAlgorithm.cpp:312
--   `if (column.name == BlockNumberColumn::name || column.name == BlockOffsetColumn::name)`
-- Without the BlockOffsetColumn::name exclusion, _block_offset (UInt64, summable) would be
-- aggregated as a sum across rows with the same sort key during merge → incorrect values.

DROP TABLE IF EXISTS t_smt_block_offset;

CREATE TABLE t_smt_block_offset (id UInt32, v UInt32) ENGINE = SummingMergeTree
ORDER BY id
SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1;

INSERT INTO t_smt_block_offset(id, v) VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO t_smt_block_offset(id, v) VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO t_smt_block_offset(id, v) VALUES (1, 1), (2, 1), (3, 1);

OPTIMIZE TABLE t_smt_block_offset FINAL;

-- Expected: v summed (3 each), _block_offset preserved (0,1,2 — NOT 0,3,6 as if summed).
SELECT id, v, _block_offset FROM t_smt_block_offset ORDER BY id;

DROP TABLE t_smt_block_offset;
