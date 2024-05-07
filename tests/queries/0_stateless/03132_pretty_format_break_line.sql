DROP TABLE IF EXISTS t_break_line;

CREATE TABLE t_break_line (id UInt64, value String, value1 String) ENGINE=MergeTree ORDER BY id;

INSERT INTO t_break_line VALUES(0, 'hello\nworld', 'hello world');

SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes;
SELECT * FROM t_break_line FORMAT PrettyCompact;
SELECT * FROM t_break_line FORMAT Pretty;
SELECT * FROM t_break_line FORMAT PrettySpace SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line FORMAT PrettySpace;

INSERT INTO t_break_line VALUES(1, 'hello world', 'hello\nworld');
SELECT * FROM t_break_line ORDER BY id FORMAT PrettyMonoBlock SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line ORDER BY id FORMAT PrettyMonoBlock;

TRUNCATE TABLE t_break_line;

INSERT INTO t_break_line VALUES(0, 'привет\nworld', 'hello world');

SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes;
SELECT * FROM t_break_line FORMAT PrettyCompact;
SELECT * FROM t_break_line FORMAT Pretty;
SELECT * FROM t_break_line FORMAT PrettySpace SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line FORMAT PrettySpace;

INSERT INTO t_break_line VALUES(1, 'hello world', 'hellow\nмир');
SELECT * FROM t_break_line ORDER BY id FORMAT PrettyMonoBlock SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line ORDER BY id FORMAT PrettyMonoBlock;

DROP TABLE t_break_line;