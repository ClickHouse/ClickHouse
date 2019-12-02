-- that test is failing on versions <= 19.11.12
DROP TABLE IF EXISTS lc_empty_part_bug;
create table lc_empty_part_bug (id  UInt64, s String) Engine=MergeTree ORDER BY id;
insert into lc_empty_part_bug select number as id, toString(rand()) from numbers(100);
alter table lc_empty_part_bug delete where id < 100;
SELECT 'wait for delete to finish', sleep(1);
alter table lc_empty_part_bug modify column s LowCardinality(String);
SELECT 'still alive';
insert into lc_empty_part_bug select number+100 as id, toString(rand()) from numbers(100);
SELECT count() FROM lc_empty_part_bug WHERE not ignore(*);
DROP TABLE IF EXISTS lc_empty_part_bug;