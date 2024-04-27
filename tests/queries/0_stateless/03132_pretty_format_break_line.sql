DROP TABLE IF EXISTS t_break_line;

CREATE TABLE t_break_line (id UInt64, value String) ENGINE=MergeTree ORDER BY id;

INSERT INTO t_break_line VALUES(0, 'hello\nworld');

SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes SETTINGS output_format_pretty_row_numbers = 0;
SELECT * FROM t_break_line FORMAT PrettyCompactNoEscapes;

DROP TABLE t_break_line;