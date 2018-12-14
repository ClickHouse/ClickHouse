drop table IF EXISTS test.tab1;
drop table IF EXISTS test.tab1_copy;

create table test.tab1 (a1 Int32, b1 Int32) engine = MergeTree order by a1;
create table test.tab1_copy (a1 Int32, b1 Int32) engine = MergeTree order by a1;

insert into test.tab1 values (1, 2);
insert into test.tab1_copy values (2, 3);

select tab1.a1, tab1_copy.a1, tab1.b1 from test.tab1 any left join test.tab1_copy on tab1.b1 + 3 = b1 + 2;

drop table test.tab1;
drop table test.tab1_copy;
