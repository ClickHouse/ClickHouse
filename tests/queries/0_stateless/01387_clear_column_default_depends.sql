-- It's Ok to CLEAR column when there are columns with default expression depending on it.
-- But it's not Ok to DROP such column.

DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8, y UInt8 DEFAULT x + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT * FROM test ORDER BY x, y;
ALTER TABLE test CLEAR COLUMN x;
SELECT * FROM test ORDER BY x, y;
ALTER TABLE test DROP COLUMN x; -- { serverError 44 }
DROP TABLE test;

DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8, y UInt8 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT x, y FROM test ORDER BY x, y;
ALTER TABLE test CLEAR COLUMN x;
SELECT x, y FROM test ORDER BY x, y;
ALTER TABLE test DROP COLUMN x; -- { serverError 44 }
DROP TABLE test;

DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8, y UInt8 ALIAS x + 1, z String DEFAULT 'Hello') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT x, y FROM test ORDER BY x, y;
ALTER TABLE test CLEAR COLUMN x;
SELECT x, y FROM test ORDER BY x, y;
ALTER TABLE test DROP COLUMN x; -- { serverError 44 }
DROP TABLE test;


-- The original report from Mikhail Petrov
DROP TABLE IF EXISTS Test;
set allow_deprecated_syntax_for_merge_tree=1;
create table Test (impression_id String,impression_id_compressed FixedString(16) DEFAULT UUIDStringToNum(substring(impression_id, 1, 36)), impression_id_hashed UInt16 DEFAULT reinterpretAsUInt16(impression_id_compressed), event_date Date ) ENGINE = MergeTree(event_date, impression_id_hashed, (event_date, impression_id_hashed), 8192);
alter table Test clear column impression_id in partition '202001';
DROP TABLE Test;
