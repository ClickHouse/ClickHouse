DROP TABLE IF EXISTS test_set;

CREATE TABLE test_set (k UInt64) ENGINE = Set;

INSERT INTO test_set SELECT * FROM numbers(10);

SELECT * FROM test_set ORDER BY k;

ALTER TABLE test_set DELETE WHERE k = 5;

SELECT * FROM test_set ORDER BY k;

ALTER TABLE test_set DELETE WHERE k < 5;

SELECT * FROM test_set ORDER BY k;

DROP TABLE test_set;
