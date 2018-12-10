SELECT toFixedString('', 4) AS str, empty(str) AS is_empty;
SELECT toFixedString('\0abc', 4) AS str, empty(str) AS is_empty;

DROP TABLE IF EXISTS test.defaulted;
CREATE TABLE test.defaulted (v6 FixedString(16)) ENGINE=Memory;
INSERT INTO test.defaulted SELECT toFixedString('::0', 16) FROM numbers(32768);
SELECT count(), notEmpty(v6) e FROM test.defaulted GROUP BY e;
DROP TABLE test.defaulted;
