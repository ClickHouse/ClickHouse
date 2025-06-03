set allow_experimental_variant_type=1;

create table test (v Variant(String, UInt64)) engine=Memory;
insert into test values (42), ('Hello'), (NULL);

select * from test where v = 'Hello';
select * from test where v = 42; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select * from test where v = 42::UInt64::Variant(String, UInt64);

drop table test;

