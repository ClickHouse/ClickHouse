DROP TABLE IF EXISTS alter_test;

CREATE TABLE alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;

ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;

ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;

DESC alter_test;
DETACH TABLE alter_test;
ATTACH TABLE alter_test;
DESC alter_test;

ALTER TABLE alter_test MODIFY COLUMN Added2 UInt32 FIRST;

ALTER TABLE alter_test MODIFY COLUMN Added3 UInt32 AFTER CounterID;

DESC alter_test;
DETACH TABLE alter_test;
ATTACH TABLE alter_test;
DESC alter_test;

DROP TABLE IF EXISTS alter_test;
