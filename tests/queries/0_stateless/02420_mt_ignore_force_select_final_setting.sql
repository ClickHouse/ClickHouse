CREATE TABLE tbl (id UInt64, val String) engine=ReplacingMergeTree() ORDER BY id SETTINGS force_select_final=1;

SYSTEM STOP MERGES tbl;
INSERT INTO tbl SELECT number as id, 'foo' AS val FROM numbers(100000);
INSERT INTO tbl SELECT number as id, 'bar' AS val FROM numbers(100000);

-- { echoOn }

SELECT * FROM tbl WHERE id = 10000; -- single row expected (bar), because the force_select_final is in action
SELECT * FROM tbl final WHERE id = 10000; -- single row expected (bar), because the force_select_final is in action and FINAL is there.

SELECT * FROM tbl WHERE id = 10000 ORDER BY val DESC SETTINGS ignore_force_select_final=1; -- now we see 2 'real' rows
SELECT * FROM tbl FINAL WHERE id = 10000 SETTINGS ignore_force_select_final=1; -- now we see single row again.

SYSTEM START MERGES tbl;
OPTIMIZE TABLE tbl FINAL;

SELECT * FROM tbl WHERE id = 10000 SETTINGS ignore_force_select_final=1; -- now we see single row DROP TABLE tbl;
DROP TABLE tbl;