-- insert data duplicates by providing deduplication token on insert

DROP TABLE IF EXISTS insert_dedup_token1 SYNC;
DROP TABLE IF EXISTS insert_dedup_token2 SYNC;

select 'create replica 1 and check deduplication';
CREATE TABLE insert_dedup_token1 (
    id Int32, val UInt32
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/insert_dedup_token', 'r1') ORDER BY id;

select 'two inserts with exact data, one inserted, one deduplicated by data digest';
INSERT INTO insert_dedup_token1 VALUES(1, 1001);
INSERT INTO insert_dedup_token1 VALUES(1, 1001);
SELECT * FROM insert_dedup_token1 ORDER BY id;

SYSTEM FLUSH LOGS system.part_log;
SELECT DISTINCT exception FROM system.part_log
WHERE table = 'insert_dedup_token1'
  AND database = currentDatabase()
  AND event_type = 'NewPart'
  AND error = 389;

select 'two inserts with the same dedup token, one inserted, one deduplicated by the token';
set insert_deduplication_token = '1';
INSERT INTO insert_dedup_token1 VALUES(1, 1001);
INSERT INTO insert_dedup_token1 VALUES(2, 1002);
SELECT * FROM insert_dedup_token1 ORDER BY id;

select 'reset deduplication token and insert new row';
set insert_deduplication_token = '';
INSERT INTO insert_dedup_token1 VALUES(2, 1002);
SELECT * FROM insert_dedup_token1 ORDER BY id;

select 'create replica 2 and check deduplication';
CREATE TABLE insert_dedup_token2 (
    id Int32, val UInt32
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/insert_dedup_token', 'r2') ORDER BY id;
SYSTEM SYNC REPLICA insert_dedup_token2;

select 'inserted value deduplicated by data digest, the same result as before';
set insert_deduplication_token = '';
INSERT INTO insert_dedup_token2 VALUES(1, 1001); -- deduplicated by data digest
SELECT * FROM insert_dedup_token2 ORDER BY id;

select 'inserted value deduplicated by dedup token, the same result as before';
set insert_deduplication_token = '1';
INSERT INTO insert_dedup_token2 VALUES(3, 1003); -- deduplicated by dedup token
SELECT * FROM insert_dedup_token2 ORDER BY id;

select 'new record inserted by providing new deduplication token';
set insert_deduplication_token = '2';
INSERT INTO insert_dedup_token2  VALUES(2, 1002); -- inserted
SELECT * FROM insert_dedup_token2 ORDER BY id;

DROP TABLE insert_dedup_token1 SYNC;
DROP TABLE insert_dedup_token2 SYNC;
