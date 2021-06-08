DROP TABLE IF EXISTS test_null_as_default;
CREATE TABLE test_null_as_default (a String DEFAULT 'WORLD') ENGINE = Memory;

INSERT INTO test_null_as_default SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

INSERT INTO test_null_as_default SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP TABLE IF EXISTS test_null_as_default;
CREATE TABLE test_null_as_default (a String DEFAULT 'WORLD', b String DEFAULT 'PEOPLE') ENGINE = Memory;

INSERT INTO test_null_as_default(a) SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP TABLE IF EXISTS test_null_as_default;
CREATE TABLE test_null_as_default (a Int8, b Int64 DEFAULT a + 1000) ENGINE = Memory;

INSERT INTO test_null_as_default SELECT 1, NULL UNION ALL SELECT 2, NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP TABLE IF EXISTS test_null_as_default;
CREATE TABLE test_null_as_default (a Int8, b Int64 DEFAULT c - 500, c Int32 DEFAULT a + 1000) ENGINE = Memory;

INSERT INTO test_null_as_default(a, c) SELECT 1, NULL UNION ALL SELECT 2, NULL;
SELECT * FROM test_null_as_default ORDER BY a;

DROP TABLE test_null_as_default;
