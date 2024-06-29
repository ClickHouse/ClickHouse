DROP TABLE IF EXISTS t_transform;

CREATE TABLE t_transform (id UInt64, value String, value1 String, value2 String, value3 String, value4 String, value5 String, value6 String, value7 String, value8 String, value9 String, value10 String, value11 String, value12 String, value13 String, value14 String, value15 String, value16 String, value17 String, value18 String, value19 String, value20 String, value21 String, value22 String, value23 String) ENGINE=MergeTree ORDER BY id;

INSERT INTO t_transform VALUES(0, 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value');

SET max_threads = 1;
SELECT * FROM t_transform FORMAT PrettyCompact;

SET output_format_pretty_min_columns_for_vertical_row=26;
SELECT * FROM t_transform FORMAT PrettyCompact;

SET output_format_pretty_min_columns_for_vertical_row=25;
INSERT INTO t_transform VALUES(1, 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value');

SELECT * FROM t_transform ORDER BY id LIMIT 1 FORMAT PrettyCompact;

SET output_format_pretty_max_rows=1;
SELECT * FROM t_transform ORDER BY id FORMAT PrettyCompact;

SET output_format_pretty_max_rows=1000;
SELECT * FROM t_transform ORDER BY id FORMAT PrettyMonoBlock;

DROP TABLE t_transform;
