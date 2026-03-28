set enable_json_type=1;

drop table if exists test;
create table test (agg1 AggregateFunction(sum, UInt64), agg2 AggregateFunction(sum, UInt64)) engine=Memory;
insert into test select sumState(number), sumState(number + 1) from numbers(10);
select * from test order by agg1; -- {serverError ILLEGAL_COLUMN}
select agg1 < agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 <= agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 > agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 >= agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 = agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 != agg2 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

drop table test;
