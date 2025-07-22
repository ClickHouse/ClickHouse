-- Tags: long, replica, no-shared-merge-tree
-- no-shared-merge-tree: require sync replica, added new test

DROP TABLE IF EXISTS replicated_truncate1;
DROP TABLE IF EXISTS replicated_truncate2;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE replicated_truncate1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r1') order by k partition by toYYYYMM(d);
CREATE TABLE replicated_truncate2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r2') order by k partition by toYYYYMM(d);

SELECT '======Before Truncate======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE replicated_truncate1 SETTINGS replication_alter_partitions_sync=2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Insert Data======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

DROP TABLE IF EXISTS replicated_truncate1;
DROP TABLE IF EXISTS replicated_truncate2;
