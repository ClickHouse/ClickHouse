DROP TABLE IF EXISTS test;

CREATE TABLE test(key FixedString(10)) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY (key);
INSERT INTO test SELECT toString(intDiv(number, 8)) FROM numbers(100);
SELECT count() FROM test WHERE key = '1';

DROP TABLE IF EXISTS test;
