-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP TABLE IF EXISTS replicated_truncate1;
DROP TABLE IF EXISTS replicated_truncate2;

CREATE TABLE replicated_truncate1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r1', d, k, 8192);
CREATE TABLE replicated_truncate2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r2', d, k, 8192);

SELECT '======Before Truncate======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE replicated_truncate1;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Insert Data======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

DROP TABLE IF EXISTS replicated_truncate1;
DROP TABLE IF EXISTS replicated_truncate2;
