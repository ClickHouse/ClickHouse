drop table if exists test.prewhere_alias;
create table test.prewhere_alias (a Int32, b Int32, c alias a + b) engine = MergeTree order by b;
insert into test.prewhere_alias values(1, 1);
select a, c + toInt32(1), (c + toInt32(1)) * 2 from test.prewhere_alias prewhere (c + toInt32(1)) * 2 = 6;
drop table test.prewhere_alias;
