DROP TABLE IF EXISTS test.alter_attach;
CREATE TABLE test.alter_attach (x UInt64, p UInt8) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p;
INSERT INTO test.alter_attach VALUES (1, 1), (2, 1), (3, 1);

ALTER TABLE test.alter_attach DETACH PARTITION 1;

ALTER TABLE test.alter_attach ADD COLUMN s String;
INSERT INTO test.alter_attach VALUES (4, 2, 'Hello'), (5, 2, 'World');

ALTER TABLE test.alter_attach ATTACH PARTITION 1;
SELECT * FROM test.alter_attach ORDER BY x;

ALTER TABLE test.alter_attach DETACH PARTITION 2;
ALTER TABLE test.alter_attach DROP COLUMN s;
INSERT INTO test.alter_attach VALUES (6, 3), (7, 3);

ALTER TABLE test.alter_attach ATTACH PARTITION 2;
SELECT * FROM test.alter_attach ORDER BY x;

DROP TABLE test.alter_attach;
