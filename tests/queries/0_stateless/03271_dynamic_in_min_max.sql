set allow_experimental_dynamic_type=1;
select max(number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select min(number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select argMax(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select argMin(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select anyArgMax(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select anyArgMin(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
create table test (d Dynamic, index idx d type minmax); -- {serverError BAD_ARGUMENTS}

