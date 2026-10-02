-- `force_index_by_date` requires a condition on the partition key. With
-- `part_minmax_index_columns = 'with_block_number_offset'` the part min-max index also covers
-- `_block_number` and `_block_offset`, and a condition on them alone must not satisfy the setting.

DROP TABLE IF EXISTS t_force_index_by_date_block_number;

CREATE TABLE t_force_index_by_date_block_number (d Date, x UInt64)
ENGINE = MergeTree PARTITION BY d ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_force_index_by_date_block_number VALUES ('2026-01-01', 1);
INSERT INTO t_force_index_by_date_block_number VALUES ('2026-01-02', 2);

SELECT count() FROM t_force_index_by_date_block_number WHERE _block_number > 1 SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_force_index_by_date_block_number WHERE _block_number > 1 SETTINGS force_index_by_date = 1, use_skip_indexes = 0; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_force_index_by_date_block_number WHERE _block_offset = 0 SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_force_index_by_date_block_number WHERE d = '2026-01-02' SETTINGS force_index_by_date = 1;
SELECT count() FROM t_force_index_by_date_block_number WHERE d = '2026-01-02' SETTINGS force_index_by_date = 1, use_skip_indexes = 0;
SELECT count() FROM t_force_index_by_date_block_number WHERE d = '2026-01-02' AND _block_number > 1 SETTINGS force_index_by_date = 1;

-- Without `force_index_by_date` the same condition is fine.
SELECT count() FROM t_force_index_by_date_block_number WHERE _block_number > 1;

DROP TABLE t_force_index_by_date_block_number;
