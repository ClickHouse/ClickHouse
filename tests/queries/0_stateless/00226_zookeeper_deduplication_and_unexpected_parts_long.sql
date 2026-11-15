-- Tags: long, zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: Old syntax is not allowed
-- no-shared-merge-tree: implemented replacement

DROP TABLE IF EXISTS deduplication;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE deduplication (d Date DEFAULT '2015-01-01', x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00226/deduplication', 'r1', d, x, 1);

INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);

SELECT * FROM deduplication;

DETACH TABLE deduplication;
ATTACH TABLE deduplication;

SELECT * FROM deduplication;

DROP TABLE deduplication;
