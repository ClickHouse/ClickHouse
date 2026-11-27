CREATE TABLE t ENGINE = Log AS SELECT * FROM system.numbers LIMIT 20;
SELECT number FROM (select number FROM t ORDER BY number OFFSET 3) WHERE number < NULL;

