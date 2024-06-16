DROP TABLE IF EXISTS t_tabs;

CREATE TABLE t_tabs (id UInt64, value String, value1 String) ENGINE=MergeTree ORDER BY id;

INSERT INTO t_tabs VALUES(0, 'test test', '\tsomething');

SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock;

TRUNCATE TABLE t_tabs;

INSERT INTO t_tabs VALUES(0, 'test\n\ttest', 'something');

SELECT * FROM t_tabs ORDER BY id FORMAT PrettyCompactNoEscapes SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs ORDER BY id FORMAT PrettyCompactNoEscapes;
SELECT * FROM t_tabs FORMAT PrettySpace SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs FORMAT PrettySpace;

TRUNCATE TABLE t_tabs;

INSERT INTO t_tabs VALUES(0, 'something', 'test\n\ttest');

SELECT * FROM t_tabs ORDER BY id FORMAT PrettyCompactNoEscapes SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs ORDER BY id FORMAT PrettyCompactNoEscapes;
SELECT * FROM t_tabs FORMAT PrettySpace SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs FORMAT PrettySpace;

INSERT INTO t_tabs VALUES(1, '\tsome\tthing\t', 'test\n\ttest');

SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock;

TRUNCATE TABLE t_tabs;

SET output_format_pretty_max_value_width = 5;

INSERT INTO t_tabs VALUES(0, 'someth\ning\t', 'test\ntesttest');

SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_tabs ORDER BY id FORMAT PrettyMonoBlock;

DROP TABLE t_tabs;
