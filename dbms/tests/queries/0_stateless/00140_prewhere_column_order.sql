DROP TABLE IF EXISTS test.prewhere;

CREATE TABLE test.prewhere (d Date, a String, b String) ENGINE = MergeTree(d, d, 8192);
INSERT INTO test.prewhere VALUES ('2015-01-01', 'hello', 'world');

ALTER TABLE test.prewhere ADD COLUMN a1 String AFTER a;
INSERT INTO test.prewhere VALUES ('2015-01-01', 'hello1', 'xxx', 'world1');

SELECT d, a, a1, b FROM test.prewhere PREWHERE a LIKE 'hello%' ORDER BY a1;

DROP TABLE test.prewhere;
