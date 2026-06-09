-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106780
-- ALTER TABLE UPDATE on a 0-level part (no _block_number/_block_offset data yet)
-- must not crash with NOT_FOUND_COLUMN_IN_BLOCK when part_minmax_index_columns
-- is set to 'with_block_number_offset'.

DROP TABLE IF EXISTS t_minmax_block_mutation;

CREATE TABLE t_minmax_block_mutation (c0 UInt64, c1 String)
    ENGINE = MergeTree ORDER BY c0
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_minmax_block_mutation VALUES (1, 'a');

ALTER TABLE t_minmax_block_mutation UPDATE c1 = 'b' WHERE c0 = 1 SETTINGS mutations_sync = 1;

SELECT c1 FROM t_minmax_block_mutation;

-- Also verify that UPDATE works correctly on a merged part (which has _block_number).
OPTIMIZE TABLE t_minmax_block_mutation FINAL;

ALTER TABLE t_minmax_block_mutation UPDATE c1 = 'c' WHERE c0 = 1 SETTINGS mutations_sync = 1;

SELECT c1 FROM t_minmax_block_mutation;

DROP TABLE t_minmax_block_mutation;
