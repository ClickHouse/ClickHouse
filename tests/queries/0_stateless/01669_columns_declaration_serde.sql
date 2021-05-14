CREATE TEMPORARY TABLE test ("\\" String DEFAULT '\r\n\t\\' || '
');

INSERT INTO test VALUES ('Hello, world!');
INSERT INTO test ("\\") VALUES ('Hello, world!');

SELECT * FROM test;

DROP TEMPORARY TABLE test;
DROP TABLE IF EXISTS test;

CREATE TABLE test (x UInt64, "\\" String DEFAULT '\r\n\t\\' || '
') ENGINE = MergeTree ORDER BY x;

INSERT INTO test (x) VALUES (1);

SELECT * FROM test;

DROP TABLE test;

DROP TABLE IF EXISTS test_r1;
DROP TABLE IF EXISTS test_r2;

CREATE TABLE test_r1 (x UInt64, "\\" String DEFAULT '\r\n\t\\' || '
') ENGINE = ReplicatedMergeTree('/clickhouse/test_01669', 'r1') ORDER BY "\\";

INSERT INTO test_r1 ("\\") VALUES ('\\');

CREATE TABLE test_r2 (x UInt64, "\\" String DEFAULT '\r\n\t\\' || '
') ENGINE = ReplicatedMergeTree('/clickhouse/test_01669', 'r2') ORDER BY "\\";

SYSTEM SYNC REPLICA test_r2;

SELECT '---';
SELECT * FROM test_r1;
SELECT '---';
SELECT * FROM test_r2;

DROP TABLE test_r1;
DROP TABLE test_r2;
