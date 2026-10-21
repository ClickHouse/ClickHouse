DROP TABLE IF EXISTS t_lightweight;
SET enable_lightweight_update = 1;

CREATE TABLE t_lightweight (d Date, e Enum8('foo' = 1, 'bar' = 2))
ENGINE = MergeTree ORDER BY d
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lightweight (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO t_lightweight (d, e) VALUES ('2018-01-02', 'bar');

UPDATE t_lightweight SET e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d = '2018-01-02';

SELECT e FROM t_lightweight ORDER BY d;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_lightweight' ORDER BY name;

DROP TABLE t_lightweight;
