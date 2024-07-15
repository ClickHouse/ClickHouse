DROP TABLE IF EXISTS alter_01355;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE alter_01355 (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

ALTER TABLE alter_01355 ADD COLUMN Added1 UInt32 FIRST;

ALTER TABLE alter_01355 ADD COLUMN Added2 UInt32 AFTER NestedColumn;

ALTER TABLE alter_01355 ADD COLUMN Added3 UInt32 AFTER ToDrop;

DESC alter_01355;
DETACH TABLE alter_01355;
ATTACH TABLE alter_01355;
DESC alter_01355;

ALTER TABLE alter_01355 MODIFY COLUMN Added2 UInt32 FIRST;

ALTER TABLE alter_01355 MODIFY COLUMN Added3 UInt32 AFTER CounterID;

DESC alter_01355;
DETACH TABLE alter_01355;
ATTACH TABLE alter_01355;
DESC alter_01355;

DROP TABLE IF EXISTS alter_01355;
