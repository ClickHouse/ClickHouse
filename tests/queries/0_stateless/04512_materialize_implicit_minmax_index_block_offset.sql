-- Tags: no-random-settings, no-random-merge-tree-settings

-- MATERIALIZE INDEX on the implicit minmax index over the persistent virtual
-- column _block_offset (or _block_number) must build the index on a freshly
-- inserted 0-level part, which does not materialize those columns on disk.
-- Those columns are synthesized on read (as in a merge), so the index is built
-- rather than silently skipped.

DROP TABLE IF EXISTS t_mat_imv_offset;

CREATE TABLE t_mat_imv_offset (c0 Int)
ENGINE = MergeTree ORDER BY c0
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1;

INSERT INTO t_mat_imv_offset VALUES (1);

-- 0-level part: no materialized _block_offset/_block_number. Must not throw.
ALTER TABLE t_mat_imv_offset MATERIALIZE INDEX auto_minmax_index__block_offset SETTINGS mutations_sync = 2;
ALTER TABLE t_mat_imv_offset MATERIALIZE INDEX auto_minmax_index__block_number SETTINGS mutations_sync = 2;

SELECT count() FROM t_mat_imv_offset;

-- The index must actually be built on the 0-level part: it must be usable to
-- prune. _block_offset of the single row is 0, so a query for _block_offset = 999
-- must prune all granules.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_mat_imv_offset WHERE _block_offset = 999
) WHERE explain ILIKE '%auto_minmax_index__block_offset%';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_mat_imv_offset WHERE _block_offset = 999
) WHERE explain ILIKE '%Granules: 0/1%';

-- Merged part: _block_offset/_block_number are materialized and the index must
-- still be (re)built correctly.
INSERT INTO t_mat_imv_offset VALUES (2);
OPTIMIZE TABLE t_mat_imv_offset FINAL;

ALTER TABLE t_mat_imv_offset MATERIALIZE INDEX auto_minmax_index__block_offset SETTINGS mutations_sync = 2;
ALTER TABLE t_mat_imv_offset MATERIALIZE INDEX auto_minmax_index__block_number SETTINGS mutations_sync = 2;

SELECT count() FROM t_mat_imv_offset;
SELECT c0 FROM t_mat_imv_offset ORDER BY c0;

DROP TABLE t_mat_imv_offset;
