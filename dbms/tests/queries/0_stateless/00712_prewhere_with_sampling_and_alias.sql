drop table if exists test.t;
create table test.t (date Date, counter UInt64, sampler UInt64, alias_col alias date + 1) engine = MergeTree(date, intHash32(sampler), (counter, date, intHash32(sampler)), 8192);
insert into test.t values ('2018-01-01', 1, 1);
select alias_col from test.t sample 1 / 2 where date = '2018-01-01' and counter = 1 and sampler = 1;
drop table if exists test.t;

