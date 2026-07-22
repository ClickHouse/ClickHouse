set enable_json_type=1;

drop table if exists test;
create table test (agg1 AggregateFunction(sum, UInt64), agg2 AggregateFunction(sum, UInt64)) engine=Memory;
insert into test select sumState(number), sumState(number + 1) from numbers(10);
select * from test order by agg1 settings allow_not_comparable_types_in_order_by=0; -- {serverError ILLEGAL_COLUMN}
select * from test order by agg1 settings allow_not_comparable_types_in_order_by=1;
select agg1 < agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 < agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;
select agg1 <= agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 <= agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;
select agg1 > agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 > agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;
select agg1 >= agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 >= agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;
select agg1 = agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 = agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;
select agg1 != agg2 from test settings allow_not_comparable_types_in_comparison_functions=0; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select agg1 != agg2 from test settings allow_not_comparable_types_in_comparison_functions=1;

drop table test;
