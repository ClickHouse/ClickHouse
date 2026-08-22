-- Tags: long, replica, no-shared-merge-tree
-- no-shared-merge-tree: not possible to stop replicated sends

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;

CREATE TABLE r1 (x String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY x;
CREATE TABLE r2 (x String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r2') ORDER BY x;

SYSTEM STOP REPLICATED SENDS r1;
SYSTEM STOP REPLICATED SENDS r2;

INSERT INTO r1 VALUES ('Hello, world');
SELECT * FROM r1;
SELECT * FROM r2;
INSERT INTO r2 VALUES ('Hello, world');
SELECT '---';
SELECT * FROM r1;
SELECT * FROM r2;

SYSTEM START REPLICATED SENDS r1;
SYSTEM START REPLICATED SENDS r2;
SYSTEM SYNC REPLICA r1;
SYSTEM SYNC REPLICA r2;

SELECT '---';
SELECT * FROM r1;
SELECT * FROM r2;

DROP TABLE r1;
DROP TABLE r2;
