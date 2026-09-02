-- Tags: no-parallel

DROP TABLE IF EXISTS test_alter_attach_01901S;
DROP TABLE IF EXISTS test_alter_attach_01901D;

CREATE TABLE test_alter_attach_01901S (A Int64, D date) ENGINE = MergeTree PARTITION BY D ORDER BY A;
INSERT INTO test_alter_attach_01901S VALUES (1, '2020-01-01');

CREATE TABLE test_alter_attach_01901D (A Int64, D date) 
Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test_alter_attach_01901D', 'r1')
PARTITION BY D ORDER BY A;

ALTER TABLE test_alter_attach_01901D ATTACH PARTITION '2020-01-01' FROM test_alter_attach_01901S;

SELECT count() FROM test_alter_attach_01901D;
SELECT count() FROM test_alter_attach_01901S;

INSERT INTO test_alter_attach_01901S VALUES (1, '2020-01-01');
ALTER TABLE test_alter_attach_01901D REPLACE PARTITION '2020-01-01' FROM test_alter_attach_01901S;

SELECT count() FROM test_alter_attach_01901D;
SELECT count() FROM test_alter_attach_01901S;

DROP TABLE test_alter_attach_01901S;
DROP TABLE test_alter_attach_01901D;
