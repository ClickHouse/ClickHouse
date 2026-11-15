DROP TABLE IF EXISTS column_modify_test;

CREATE TABLE column_modify_test (id UInt64, val String, other_col UInt64) engine=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0;
INSERT INTO column_modify_test VALUES (1,'one',0);
INSERT INTO column_modify_test VALUES (2,'two',0);

-- on 21.9 that was done via mutations mechanism
ALTER TABLE column_modify_test MODIFY COLUMN val Nullable(String);

INSERT INTO column_modify_test VALUES (3,Null,0);

-- till now everythings looks ok
SELECT * FROM column_modify_test order by id, val, other_col;

-- Now we do mutation. It will affect one of the parts, and will update columns.txt to the latest / correct state w/o updating the column file!
alter table column_modify_test update other_col=1 where id = 1 SETTINGS mutations_sync=1;

-- row 1 is damaged now the column file & columns.txt is out of sync!
SELECT *, throwIf(val <> 'one') as issue FROM column_modify_test WHERE id = 1;
