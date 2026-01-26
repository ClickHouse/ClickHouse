-- Tags: no-fasttest

DROP TABLE IF EXISTS column_modify_test;
set allow_statistics_optimize=1;
CREATE TABLE column_modify_test (id UInt64, val String, other_col UInt64) engine=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0, auto_statistics_types='uniq,countmin';
INSERT INTO column_modify_test VALUES (1,'one',0);

ALTER TABLE column_modify_test MODIFY COLUMN val Nullable(String);

alter table column_modify_test update other_col=1 where id = 1 SETTINGS mutations_sync=1;

SELECT *, throwIf(val <> 'one') as issue FROM column_modify_test WHERE id = 1 FORMAT NULL;
DROP TABLE column_modify_test;
CREATE TABLE column_modify_test (id UInt64, val Nullable(String), other_col UInt64) engine=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0, auto_statistics_types='uniq,countmin';
INSERT INTO column_modify_test VALUES (1,'one',0);

ALTER TABLE column_modify_test MODIFY COLUMN val String;

alter table column_modify_test update other_col=1 where id = 1 SETTINGS mutations_sync=1;

SELECT *, throwIf(val <> 'one') as issue FROM column_modify_test WHERE id = 1 FORMAT NULL;
