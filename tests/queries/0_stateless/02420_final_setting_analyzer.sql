-- { echoOn }
set enable_analyzer=1;
SYSTEM STOP MERGES tbl;

-- simple test case
create table if not exists replacing_mt (x String) engine=ReplacingMergeTree() ORDER BY x;

insert into replacing_mt values ('abc');
insert into replacing_mt values ('abc');

-- expected output is 2 because final is turned off
select count() from replacing_mt;

set final = 1;
-- expected output is 1 because final is turned on
select count() from replacing_mt;

-- JOIN test cases
create table if not exists lhs (x String) engine=ReplacingMergeTree() ORDER BY x;
create table if not exists rhs (x String) engine=ReplacingMergeTree() ORDER BY x;

insert into lhs values ('abc');
insert into lhs values ('abc');

insert into rhs values ('abc');
insert into rhs values ('abc');

set final = 0;
-- expected output is 4 because select_final == 0
select count() from lhs inner join rhs on lhs.x = rhs.x;

set final = 1;
-- expected output is 1 because final == 1
select count() from lhs inner join rhs on lhs.x = rhs.x;

-- regular non final table
set final = 1;
create table if not exists regular_mt_table (x String) engine=MergeTree() ORDER BY x;
insert into regular_mt_table values ('abc');
insert into regular_mt_table values ('abc');
-- expected output is 2, it should silently ignore final modifier
select count() from regular_mt_table;

-- view test
create materialized VIEW mv_regular_mt_table TO regular_mt_table AS SELECT * FROM regular_mt_table;
create view nv_regular_mt_table AS SELECT * FROM mv_regular_mt_table;

set final=1;
select count() from nv_regular_mt_table;

-- join on mix of tables that support / do not support select final with explain
create table if not exists left_table (id UInt64, val_left String) engine=ReplacingMergeTree() ORDER BY id;
create table if not exists middle_table (id UInt64, val_middle String) engine=MergeTree() ORDER BY id;
create table if not exists right_table (id UInt64, val_right String) engine=ReplacingMergeTree() ORDER BY id;
insert into left_table values (1,'a');
insert into left_table values (1,'b');
insert into left_table values (1,'c');
insert into middle_table values (1,'a');
insert into middle_table values (1,'b');
insert into right_table values (1,'a');
insert into right_table values (1,'b');
insert into right_table values (1,'c');
-- expected output
-- 1 c a c
-- 1 c b c
select left_table.id,val_left, val_middle, val_right from left_table
                                                              inner join middle_table on left_table.id = middle_table.id
                                                              inner join right_table on middle_table.id = right_table.id
ORDER BY left_table.id, val_left, val_middle, val_right;

explain syntax select left_table.id,val_left, val_middle, val_right from left_table
                                                                             inner join middle_table on left_table.id = middle_table.id
                                                                             inner join right_table on middle_table.id = right_table.id
               ORDER BY left_table.id, val_left, val_middle, val_right;


explain syntax select left_table.id,val_left, val_middle, val_right from left_table
                                                                             inner join middle_table on left_table.id = middle_table.id
                                                                             inner join right_table on middle_table.id = right_table.id
               ORDER BY left_table.id, val_left, val_middle, val_right SETTINGS enable_analyzer=0;

-- extra: same with subquery
select left_table.id,val_left, val_middle, val_right from left_table
                                                              inner join middle_table on left_table.id = middle_table.id
                                                              inner join (SELECT * FROM right_table WHERE id = 1) r on middle_table.id = r.id
ORDER BY left_table.id, val_left, val_middle, val_right;

-- Quite exotic with Merge engine
DROP TABLE IF EXISTS table_to_merge_a;
DROP TABLE IF EXISTS table_to_merge_b;
DROP TABLE IF EXISTS table_to_merge_c;
DROP TABLE IF EXISTS merge_table;

create table if not exists table_to_merge_a (id UInt64, val String) engine=ReplacingMergeTree() ORDER BY id;
create table if not exists table_to_merge_b (id UInt64, val String) engine=MergeTree() ORDER BY id;
create table if not exists table_to_merge_c (id UInt64, val String) engine=ReplacingMergeTree() ORDER BY id;
CREATE TABLE merge_table Engine=Merge(currentDatabase(), '^(table_to_merge_[a-z])$') AS table_to_merge_a;

insert into table_to_merge_a values (1,'a');
insert into table_to_merge_a values (1,'b');
insert into table_to_merge_a values (1,'c');
insert into table_to_merge_b values (2,'a');
insert into table_to_merge_b values (2,'b');
insert into table_to_merge_c values (3,'a');
insert into table_to_merge_c values (3,'b');
insert into table_to_merge_c values (3,'c');

-- expected output:
-- 1 c, 2 a, 2 b, 3 c
SELECT * FROM merge_table ORDER BY id, val;

select sum(number) from numbers(10) settings final=1;
select sum(number) from remote('127.0.0.{1,2}', numbers(10)) settings final=1;
