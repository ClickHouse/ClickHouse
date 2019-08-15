drop table if exists lc_lambda;
create table lc_lambda (arr Array(LowCardinality(UInt64))) engine = Memory;
insert into lc_lambda select range(number) from system.numbers limit 10;
select arrayFilter(x -> x % 2 == 0, arr) from lc_lambda;
drop table if exists lc_lambda;

