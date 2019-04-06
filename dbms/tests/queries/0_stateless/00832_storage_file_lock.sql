DROP TABLE IF EXISTS test.file;
CREATE TABLE test.file (number UInt64) ENGINE = File(TSV);
SELECT * FROM test.file; -- { serverError 107 }
INSERT INTO test.file VALUES (1);
SELECT * FROM test.file;
DROP TABLE test.file;
