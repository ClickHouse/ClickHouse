DROP DATABASE IF EXISTS rdbtest;
DROP DATABASE IF EXISTS replicatwo;
DROP DATABASE IF EXISTS replicathree;

CREATE DATABASE rdbtest ENGINE = Replicated('/clickhouse/db/test1/', 'id1');
CREATE DATABASE replicatwo ENGINE = Replicated('/clickhouse/db/test1/', 'id2');
CREATE DATABASE replicathree ENGINE = Replicated('/clickhouse/db/test1/', 'id3');

USE rdbtest;

CREATE TABLE alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

ALTER TABLE alter_test ADD COLUMN Added0 UInt32;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32;
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 AFTER Added0;

ALTER TABLE alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;
ALTER TABLE alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;
ALTER TABLE alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;

ALTER TABLE alter_test DROP COLUMN ToDrop;

ALTER TABLE alter_test MODIFY COLUMN Added0 String;

ALTER TABLE alter_test DROP COLUMN NestedColumn.A;
ALTER TABLE alter_test DROP COLUMN NestedColumn.S;

ALTER TABLE alter_test DROP COLUMN AddedNested1.B;

ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS Added0 UInt32;
ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS AddedNested1 Nested(A UInt32, B UInt64);
ALTER TABLE alter_test ADD COLUMN IF NOT EXISTS AddedNested1.C Array(String);
ALTER TABLE alter_test MODIFY COLUMN IF EXISTS ToDrop UInt64;
ALTER TABLE alter_test DROP COLUMN IF EXISTS ToDrop;
ALTER TABLE alter_test COMMENT COLUMN IF EXISTS ToDrop 'new comment';

DESC TABLE rdbtest.alter_test;
DESC TABLE replicatwo.alter_test;
DESC TABLE replicathree.alter_test;
