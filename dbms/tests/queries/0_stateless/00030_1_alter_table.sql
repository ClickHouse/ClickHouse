DROP TABLE IF EXISTS alter_test;

CREATE TABLE alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

INSERT INTO alter_test VALUES (1, '2014-01-01', 2, 3, [1,2,3], ['a','b','c'], 4);

ALTER TABLE alter_test ADD COLUMN Added0 UInt32;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32;
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 AFTER Added0;

ALTER TABLE alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;
ALTER TABLE alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;
ALTER TABLE alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;

DESC TABLE alter_test;
