
DROP TABLE IF EXISTS t_block_offset;

CREATE TABLE t_block_offset (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;

INSERT INTO t_block_offset SELECT number * 2 FROM numbers(8);

INSERT INTO t_block_offset SELECT number * 2 FROM numbers(8, 8);

OPTIMIZE TABLE t_block_offset FINAL;

SELECT _part, _block_number, _block_offset, _part_offset, id FROM t_block_offset ORDER BY _block_number, _block_offset;

ALTER TABLE t_block_offset MODIFY SETTING enable_block_number_column = 1;
ALTER TABLE t_block_offset MODIFY SETTING enable_block_offset_column = 1;

INSERT INTO t_block_offset SELECT number * 2 + 1 FROM numbers(16);

OPTIMIZE TABLE t_block_offset FINAL;

SELECT '===========';
SELECT _part,  _block_number, _block_offset, _part_offset, id FROM t_block_offset ORDER BY _block_number, _block_offset;


DROP TABLE t_block_offset;
