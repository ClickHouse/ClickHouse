SET max_result_rows = 10;

SELECT count() FROM (SELECT * FROM system.numbers LIMIT 11);

CREATE TEMPORARY TABLE t AS SELECT * FROM system.numbers LIMIT 11;
SELECT count() FROM t;

INSERT INTO t SELECT * FROM system.numbers LIMIT 11;
SELECT count() FROM t;
