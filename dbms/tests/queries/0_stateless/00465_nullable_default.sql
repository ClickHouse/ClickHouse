DROP TABLE IF EXISTS test.nullable;
CREATE TABLE test.nullable (id Nullable(UInt32), cat String) ENGINE = Log;
INSERT INTO test.nullable (cat) VALUES ('test');
SELECT * FROM test.nullable;
DROP TABLE test.nullable;
