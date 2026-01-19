set allow_experimental_qbit_type=1;

drop table if exists test;
create table test (qbit QBit(Float64, 3)) engine=MergeTree order by tuple();
insert into test select [1., 2., 3.] from numbers(10);
select * from test order by qbit; -- {serverError ILLEGAL_COLUMN}
select qbit < qbit from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
drop table test;

