DROP TABLE IF EXISTS test_null_as_default;
CREATE TABLE test_null_as_default (s String DEFAULT 'WORLD') ENGINE = Memory;

INSERT INTO test_null_as_default SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY s;
SELECT '';

INSERT INTO test_null_as_default SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY s;
SELECT '';

REPLACE TABLE test_null_as_default (s String DEFAULT 'WORLD', ss String DEFAULT 'PEOPLE') ENGINE = Memory;

INSERT INTO test_null_as_default(s) SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY s;
