-- insert data duplicates by providing deduplication token on insert

DROP TABLE IF EXISTS insert_dedup_token SYNC;

select 'create and check deduplication';
CREATE TABLE insert_dedup_token (
    id Int32, val UInt32
) ENGINE=MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=0xFFFFFFFF;

select 'two inserts with exact data, one inserted, one deduplicated by data digest';
INSERT INTO insert_dedup_token VALUES(0, 1000);
INSERT INTO insert_dedup_token VALUES(0, 1000);
SELECT * FROM insert_dedup_token ORDER BY id;

select 'two inserts with the same dedup token, one inserted, one deduplicated by the token';
set insert_deduplication_token = '\x61\x00\x62';
INSERT INTO insert_dedup_token VALUES(1, 1001);
INSERT INTO insert_dedup_token VALUES(2, 1002);
SELECT * FROM insert_dedup_token ORDER BY id;

select 'update dedup token, two inserts with the same dedup token, one inserted, one deduplicated by the token';
set insert_deduplication_token = '\x61\x00\x63';
INSERT INTO insert_dedup_token VALUES(1, 1001);
INSERT INTO insert_dedup_token VALUES(2, 1002);
SELECT * FROM insert_dedup_token ORDER BY id;

select 'reset deduplication token and insert new row';
set insert_deduplication_token = '';
INSERT INTO insert_dedup_token VALUES(2, 1002);
SELECT * FROM insert_dedup_token ORDER BY id;

DROP TABLE insert_dedup_token SYNC;
