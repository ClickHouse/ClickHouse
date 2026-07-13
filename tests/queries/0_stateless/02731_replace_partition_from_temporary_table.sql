-- Tags: no-replicated-database, no-shared-merge-tree
-- SharedMergeTree doesn't support replace partition from MergeTree engine

DROP TEMPORARY TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS rdst;

CREATE TEMPORARY TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE dst (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE rdst (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_alter_attach_00626_rdst', 'r1') PARTITION BY p ORDER BY k;

SELECT 'Initial';
INSERT INTO src VALUES (0, '0', 1);
INSERT INTO src VALUES (1, '0', 1);
INSERT INTO src VALUES (1, '1', 1);
INSERT INTO src VALUES (2, '0', 1);
INSERT INTO src VALUES (3, '0', 1);
INSERT INTO src VALUES (3, '1', 1);

INSERT INTO dst VALUES (0, '1', 2);
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO dst VALUES (2, '1', 2);
INSERT INTO dst VALUES (3, '1', 2), (3, '2', 2);

INSERT INTO rdst VALUES (0, '1', 2);
INSERT INTO rdst VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO rdst VALUES (2, '1', 2);
INSERT INTO rdst VALUES (3, '1', 2), (3, '2', 2);

SELECT count(), sum(d) FROM dst;
SELECT count(), sum(d) FROM rdst;

SELECT 'REPLACE simple';
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;
ALTER TABLE rdst REPLACE PARTITION 3 FROM src;
SELECT count(), sum(d) FROM rdst;

SELECT 'ATTACH FROM';
ALTER TABLE dst DROP PARTITION 1;
ALTER TABLE dst ATTACH PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;
ALTER TABLE rdst DROP PARTITION 3;
ALTER TABLE rdst ATTACH PARTITION 1 FROM src;
SELECT count(), sum(d) FROM rdst;

DROP TEMPORARY TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS rdst;
