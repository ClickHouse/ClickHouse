set allow_experimental_low_cardinality_type = 1;
drop table if exists test.lc_lambda;
create table test.lc_lambda (arr Array(LowCardinality(UInt64))) engine = Memory;
insert into test.lc_lambda select range(number) from system.numbers limit 10;
select arrayFilter(x -> x % 2 == 0, arr) from test.lc_lambda;
drop table if exists test.lc_lambda;

