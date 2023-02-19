-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP TABLE IF EXISTS primary_key;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE primary_key (d Date DEFAULT today(), x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00215/primary_key', 'r1', d, -x, 1);

INSERT INTO primary_key (x) VALUES (1), (2), (3);
INSERT INTO primary_key (x) VALUES (1), (3), (2);
INSERT INTO primary_key (x) VALUES (2), (1), (3);
INSERT INTO primary_key (x) VALUES (2), (3), (1);
INSERT INTO primary_key (x) VALUES (3), (1), (2);
INSERT INTO primary_key (x) VALUES (3), (2), (1);

SELECT x FROM primary_key ORDER BY x;
SELECT x FROM primary_key WHERE -x < -1 ORDER BY x;

DROP TABLE primary_key;
