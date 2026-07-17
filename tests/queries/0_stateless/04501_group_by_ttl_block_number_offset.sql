-- Rows rolled up by `GROUP BY` TTL get `any` values of the persisted virtual columns
-- `_block_number` / `_block_offset` instead of an exception during the merge or the mutation.

SET mutations_sync = 2;
SET materialize_ttl_after_modify = 1;

DROP TABLE IF EXISTS t_ttl_group_by_block_columns SYNC;

CREATE TABLE t_ttl_group_by_block_columns (v1 DateTime64(3), v2 Int16)
ENGINE = MergeTree ORDER BY v1
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_group_by_block_columns (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_block_columns (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_block_columns (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_block_columns (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);

SELECT 'before';
SELECT _block_number, _block_offset, * FROM t_ttl_group_by_block_columns ORDER BY ALL;

OPTIMIZE TABLE t_ttl_group_by_block_columns FINAL;

SELECT 'after merge';
SELECT _block_number, _block_offset, * FROM t_ttl_group_by_block_columns ORDER BY ALL;

ALTER TABLE t_ttl_group_by_block_columns MODIFY TTL toStartOfDay(v1) + INTERVAL 1 DAY GROUP BY v1;

SELECT 'after ttl';
SELECT _block_number, _block_offset, * FROM t_ttl_group_by_block_columns ORDER BY ALL;

ALTER TABLE t_ttl_group_by_block_columns REWRITE PARTS;

SELECT 'after rewrite';
SELECT _block_number, _block_offset, * FROM t_ttl_group_by_block_columns ORDER BY ALL;

DROP TABLE t_ttl_group_by_block_columns;
