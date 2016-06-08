ALTER TABLE alter_test DROP COLUMN ToDrop;

ALTER TABLE alter_test MODIFY COLUMN Added0 String;

ALTER TABLE alter_test DROP COLUMN NestedColumn.A;
ALTER TABLE alter_test DROP COLUMN NestedColumn.S;

ALTER TABLE alter_test DROP COLUMN AddedNested1.B;

DESC TABLE alter_test;
