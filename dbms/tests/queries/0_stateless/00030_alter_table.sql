DROP TABLE IF EXISTS test.alter_test;

CREATE TABLE test.alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

INSERT INTO test.alter_test VALUES (1, '2014-01-01', 2, 3, [1,2,3], ['a','b','c'], 4);

ALTER TABLE test.alter_test ADD COLUMN Added0 UInt32;
ALTER TABLE test.alter_test ADD COLUMN Added2 UInt32;
ALTER TABLE test.alter_test ADD COLUMN Added1 UInt32 AFTER Added0;

ALTER TABLE test.alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;
ALTER TABLE test.alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;
ALTER TABLE test.alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;

DESC TABLE test.alter_test;

ALTER TABLE test.alter_test DROP COLUMN ToDrop;

ALTER TABLE test.alter_test MODIFY COLUMN Added0 String;

ALTER TABLE test.alter_test DROP COLUMN NestedColumn.A;
ALTER TABLE test.alter_test DROP COLUMN NestedColumn.S;

ALTER TABLE test.alter_test DROP COLUMN AddedNested1.B;

DESC TABLE test.alter_test;

SELECT * FROM test.alter_test;

DROP TABLE test.alter_test;
