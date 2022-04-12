-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

SET optimize_on_insert = 0;

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS old_style;
CREATE TABLE old_style(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00754/old_style', 'r1', d, x, 8192);
ALTER TABLE old_style ADD COLUMN y UInt32, MODIFY ORDER BY (x, y); -- { serverError 36 }
DROP TABLE old_style;

DROP TABLE IF EXISTS summing_r1;
DROP TABLE IF EXISTS summing_r2;
CREATE TABLE summing_r1(x UInt32, y UInt32, val UInt32) ENGINE ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r1') ORDER BY (x, y);
CREATE TABLE summing_r2(x UInt32, y UInt32, val UInt32) ENGINE ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r2') ORDER BY (x, y);

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing_r1 MODIFY ORDER BY (x, y, -val); -- { serverError 36 }

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing_r1 ADD COLUMN z UInt32 DEFAULT x + 1, MODIFY ORDER BY (x, y, -z); -- { serverError 36 }

/* Can't add nonexistent column to ORDER BY. */
ALTER TABLE summing_r1 MODIFY ORDER BY (x, y, nonexistent); -- { serverError 47 }

/* Can't modyfy ORDER BY so that it is no longer a prefix of the PRIMARY KEY. */
ALTER TABLE summing_r1 MODIFY ORDER BY x; -- { serverError 36 }

ALTER TABLE summing_r1 ADD COLUMN z UInt32 AFTER y, MODIFY ORDER BY (x, y, -z);

INSERT INTO summing_r1(x, y, z, val) values (1, 2, 0, 10), (1, 2, 1, 30), (1, 2, 2, 40);
SYSTEM SYNC REPLICA summing_r2;

SELECT '*** Check that the parts are sorted according to the new key. ***';
SELECT * FROM summing_r2;

INSERT INTO summing_r1(x, y, z, val) values (1, 2, 0, 20), (1, 2, 2, 50);
SYSTEM SYNC REPLICA summing_r2;

SELECT '*** Check that the rows are collapsed according to the new key. ***';
SELECT * FROM summing_r2 FINAL ORDER BY x, y, z;

SELECT '*** Check SHOW CREATE TABLE ***';
SHOW CREATE TABLE summing_r2;

DETACH TABLE summing_r2;
ALTER TABLE summing_r1 ADD COLUMN t UInt32 AFTER z, MODIFY ORDER BY (x, y, t * t) SETTINGS replication_alter_partitions_sync = 2; -- { serverError 341 }
ATTACH TABLE summing_r2;

SYSTEM SYNC REPLICA summing_r2;

SELECT '*** Check SHOW CREATE TABLE after offline ALTER ***';
SHOW CREATE TABLE summing_r2;

DROP TABLE summing_r1;
DROP TABLE summing_r2;
