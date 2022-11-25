-- { echoOn }
SYSTEM STOP MERGES tbl;

-- simple test case
create table if not exists replacing_mt (x String) engine=ReplacingMergeTree() ORDER BY x;

insert into replacing_mt values ('abc');
insert into replacing_mt values ('abc');

-- expected output is 2 because force_select_final is turned off
select count() from replacing_mt;

set force_select_final = 1;
-- expected output is 1 because force_select_final is turned on
select count() from replacing_mt;

-- JOIN test cases
create table if not exists lhs (x String) engine=ReplacingMergeTree() ORDER BY x;
create table if not exists rhs (x String) engine=ReplacingMergeTree() ORDER BY x;

insert into lhs values ('abc');
insert into lhs values ('abc');

insert into rhs values ('abc');
insert into rhs values ('abc');

set force_select_final = 0;
-- expected output is 4 because select_final == 0
select count() from lhs inner join rhs on lhs.x = rhs.x;

set force_select_final = 1;
-- expected output is 1 because force_select_final == 1
select count() from lhs inner join rhs on lhs.x = rhs.x;
