-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS attach_r1;
DROP TABLE IF EXISTS attach_r2;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE attach_r1 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00236/01/attach', 'r1', d, d, 8192);
CREATE TABLE attach_r2 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00236/01/attach', 'r2', d, d, 8192);

INSERT INTO attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');

SELECT d FROM attach_r1 ORDER BY d;

ALTER TABLE attach_r2 DROP PARTITION 201402;

SELECT d FROM attach_r1 ORDER BY d;

DROP TABLE attach_r1;
DROP TABLE attach_r2;
